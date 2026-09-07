"""Publish a deterministic review plan; every pending Egypt job is mandatory.

This is a derived index, never an acknowledgement or a job-fit decision. The
queue and ledger remain authoritative. Packet boundaries only bound individual
reads; they never cap Egypt coverage in one scheduled execution.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from queue_integrity import atomic_write_json, parse_utc, read_json, validate_part_payload
from radar_pipeline_v13 import is_egypt_candidate

INTERNATIONAL_CANDIDATES = 40
INTERNATIONAL_CHARACTERS = 100_000
CORE_TITLE = re.compile(
    r'systems? admin|systems? engineer|network|infrastructure|storage|backup|'
    r'IT (?:support|specialist|operations)|technical support|(?:security|SD.?WAN).*(?:engineer|expert)', re.I)
UNRELATED_TITLE = re.compile(
    r'civil|\bBIM\b|elevator|escalator|penetration|\bSOC\b|sales|marketing|'
    r'data engineer|AI architect|software|developer|backend|full.stack', re.I)
UNRELATED_DUTIES = re.compile(r'civil engineering|\bBIM\b|elevator|escalator|penetration testing|PHP|Laravel', re.I)
CORE_DUTIES = re.compile(
    r'windows server|active directory|forti(?:gate|net|analyzer)|vmware|hyper.v|'
    r'veeam|backup|disaster recovery|linux|routing|switching|powershell|microsoft 365', re.I)


def digest(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def priority(candidate, run, now):
    """An ordering hint from duties and freshness, never a final fit score."""
    stamp = parse_utc(candidate.get('first_seen_utc') or run.get('generated_at_utc'))
    seconds = stamp.timestamp() if stamp else 0
    age = max(0, now.timestamp() - seconds)
    freshness_band = 0 if age <= 86400 else 1 if age <= 7 * 86400 else 2
    title = str(candidate.get('title') or '')
    duties = str(candidate.get('description_excerpt') or '')
    # Preferred-certification lists and advisory skill hits are not experience.
    duties = re.split(r'preferred (?:certifications?|skills)', duties, flags=re.I)[0]
    alignment = (3 if CORE_TITLE.search(title) else 0) + min(3, len(set(CORE_DUTIES.findall(duties.lower()))))
    if UNRELATED_TITLE.search(title) or UNRELATED_DUTIES.search(duties):
        alignment -= 4
    relevance_band = 0 if alignment >= 3 else 1 if alignment >= 0 else 2
    return relevance_band, freshness_band, -alignment, -seconds, str(candidate.get('linkedin_job_id') or '')


def international_order(parts):
    """Remote has priority; relocation and older backlog still make progress."""
    remaining = list(sorted(parts, key=lambda p: p['_priority']))
    step = 0
    while remaining:
        step += 1
        if step % 5 == 0:
            selected = min(remaining, key=lambda p: (p['_oldest'], p['part_id']))
        else:
            preferred = 'relocation' if step % 4 == 0 else 'remote'
            selected = next((p for p in remaining if p['lane'] == preferred), remaining[0])
        remaining.remove(selected)
        yield selected


def build_plan(root: Path, pending=None, ledger=None, now=None):
    pending = pending if pending is not None else read_json(root / 'output/pending_runs.json')
    ledger = ledger if ledger is not None else read_json(root / 'state/reported_runs.json', {})
    now = now or datetime.now(timezone.utc)
    acknowledged_parts = set(ledger.get('reported_parts') or {})
    acknowledged_jobs = set(ledger.get('reported_jobs') or {})
    egypt_parts, other_parts = [], []
    egypt_ids, all_ids = set(), set()
    part_ids = set()
    for run in pending.get('runs', []):
        # A legacy reported_runs entry must never hide a new part of that run.
        for meta in run.get('delivery_parts', []):
            if meta['part_id'] in acknowledged_parts:
                continue
            ids = [str(i) for i in meta.get('job_ids', [])]
            if ids and all(i in acknowledged_jobs for i in ids):
                continue
            if meta['part_id'] in part_ids:
                raise ValueError('duplicate part_id in active queue')
            part_ids.add(meta['part_id'])
            packet_path = (root / meta['path']).resolve()
            if not packet_path.is_relative_to((root / 'output/delivery').resolve()):
                raise ValueError('delivery reference outside output/delivery')
            packet = read_json(packet_path)
            validate_part_payload(packet, meta)
            if (packet.get('schema_version') != 4
                    or packet.get('integrity', {}).get('complete_candidate_list') is not True
                    or packet.get('part_id') != meta['part_id']
                    or packet.get('run_id') != run['run_id']
                    or packet.get('expected_job_ids') != ids):
                raise ValueError('incomplete or inconsistent delivery packet')
            candidates = [c for c in packet['review_candidates'] if str(c['linkedin_job_id']) not in acknowledged_jobs]
            unacknowledged = [str(c['linkedin_job_id']) for c in candidates]
            if not unacknowledged:
                continue
            local_ids = [str(c['linkedin_job_id']) for c in candidates if is_egypt_candidate(c)]
            # Conservatively include a local or mixed packet in Egypt review.
            is_local = bool(local_ids) or meta.get('delivery_tier') == 'local'
            if is_local and not local_ids:
                local_ids = unacknowledged
            egypt_ids.update(local_ids)
            all_ids.update(unacknowledged)
            dates = [parse_utc(c.get('first_seen_utc') or run.get('generated_at_utc')) for c in candidates]
            part = {
                'part_id': meta['part_id'], 'path': meta['path'], 'run_id': run['run_id'],
                'run_finished_at_utc': run.get('run_finished_at_utc') or run.get('generated_at_utc'),
                'run_time_basis': 'run_finished_at_utc' if run.get('run_finished_at_utc') else 'batch_generation_time',
                'candidate_count': packet['candidate_count'], 'job_ids': ids,
                'job_ids_sha256': packet['integrity']['job_ids_sha256'],
                'compact_chars': len(json.dumps(packet, ensure_ascii=False)),
                'unacknowledged_job_ids': unacknowledged, 'egypt_job_ids': local_ids,
                'lane': 'egypt' if is_local else 'relocation' if meta.get('delivery_tier') == 'relocation' else 'remote',
                '_priority': min(priority(c, run, now) for c in candidates),
                '_oldest': min((d.timestamp() for d in dates if d), default=0),
            }
            (egypt_parts if is_local else other_parts).append(part)

    egypt_parts.sort(key=lambda p: p['_priority'])
    selected, selected_ids, chars = [], set(), 0
    egypt_packet_ids = {i for p in egypt_parts for i in p['unacknowledged_job_ids']}
    for part in international_order(other_parts):
        new_ids = set(part['unacknowledged_job_ids']) - selected_ids - egypt_packet_ids
        if not new_ids:
            continue
        if len(selected_ids | new_ids) > INTERNATIONAL_CANDIDATES or chars + part['compact_chars'] > INTERNATIONAL_CHARACTERS:
            continue
        selected.append(part)
        selected_ids.update(new_ids)
        chars += part['compact_chars']

    def clean(parts):
        return [{k: v for k, v in p.items() if not k.startswith('_')} for p in parts]

    return {
        'schema_version': 1, 'reviewer_contract_version': 17,
        'generated_at_utc': now.isoformat().replace('+00:00', 'Z'),
        'source_queue_sha256': digest(pending), 'source_ledger_sha256': digest(ledger),
        'source_index': 'output/pending_runs.json', 'source_ledger': 'state/reported_runs.json',
        'counts': {'unique_pending_jobs': len(all_ids), 'pending_parts': len(egypt_parts) + len(other_parts),
                   'egypt_unique_jobs': len(egypt_ids), 'international_unique_jobs': len(all_ids - egypt_ids)},
        'egypt': {'candidate_limit': None, 'compact_character_limit': None, 'deep_check_limit': None,
                  'required_unique_job_count': len(egypt_ids), 'required_part_count': len(egypt_parts),
                  'compact_chars': sum(p['compact_chars'] for p in egypt_parts),
                  'parts': clean(egypt_parts)},
        'international': {'candidate_limit': INTERNATIONAL_CANDIDATES,
                          'compact_character_limit': INTERNATIONAL_CHARACTERS, 'deep_check_limit': 10,
                          'selected_unique_job_count': len(selected_ids), 'selected_part_count': len(selected),
                          'compact_chars': chars, 'parts': clean(selected),
                          'selection_stop_reason': 'all_international_candidates_selected' if len(selected_ids | egypt_packet_ids) == len(all_ids)
                          else 'no_additional_whole_part_fits_international_limits'},
        'rules': ['Review every Egypt part before the international selection; no Egypt review or output cap.',
                  'This plan covers the pinned queue snapshot. New arrivals enter the next execution.',
                  'Read whole packets incrementally, deduplicate Job IDs, and compute review counts from actual decisions.',
                  'Unresolved Egypt parts stay pending with a concrete blocker; they are never silently skipped.',
                  'Ordering hints are not fit scores. No receipt is written by this planner.'],
    }


def publish_plan(root: Path, pending=None, ledger=None):
    plan = build_plan(root, pending, ledger)
    atomic_write_json(root / 'output/reviewer_plan.json', plan)
    return plan


if __name__ == '__main__':
    plan = publish_plan(Path(__file__).resolve().parent)
    print(json.dumps(plan['counts']))
