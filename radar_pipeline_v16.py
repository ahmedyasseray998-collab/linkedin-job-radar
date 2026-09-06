"""Policy-16 runner. One-off Egypt recovery never edits saved search defaults."""
from __future__ import annotations
import argparse
import copy
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import radar_pipeline_v13 as search
import radar_pipeline_v14 as orchestrator
import radar_pipeline_v15 as pipeline
from radar_policy_v16 import installed
from radar_maintenance_v16 import synchronize
from queue_integrity import atomic_write_json, read_json, resolve_reference, validate_part_payload

ROOT = Path(__file__).resolve().parent


def catchup_config(config):
    result = copy.deepcopy(config)
    result['window_minutes'] = 48 * 60
    result['max_detail_fetches'] = 10000
    # Same query strings/weights/eligibility policy. Deeper pagination is needed
    # for two days instead of three hours. A saturated page ceiling is reported.
    for group in ('queries', 'remote_egypt_queries'):
        for spec in result.get(group, []):
            spec['pages'] = max(60, int(spec.get('pages', 1)))
    return result


def release_unreviewed_egypt(root=ROOT):
    seen_path = root / 'state/seen.json'
    seen = read_json(seen_path, {'jobs': {}})
    reported = read_json(root / 'state/reported_runs.json', {})
    pending = read_json(root / 'output/pending_runs.json', {'runs': []})
    protected = set((reported.get('reported_jobs') or {}).keys())
    protected.update(str(j) for run in pending.get('runs', []) for part in run.get('delivery_parts', []) for j in part.get('job_ids', []))
    removed = []
    for job_id, record in list(seen.get('jobs', {}).items()):
        if job_id not in protected and search.is_egypt_candidate(record):
            seen['jobs'].pop(job_id)
            removed.append(job_id)
    atomic_write_json(seen_path, seen)
    return len(removed)


@contextmanager
def run_scope(mode):
    old_definitions = search.lane_definitions
    old_config = orchestrator.prepare_runtime_config
    def definitions(config):
        return [d for d in old_definitions(config) if d['name'] in {'egypt', 'remote_egypt'}]
    def runtime():
        config = old_config()
        # Smaller serialization budget also accounts for escaped JSON in tool
        # responses. This is packaging only, never a candidate-count cap.
        config['delivery_max_compact_chars'] = min(8500, int(config.get('delivery_max_compact_chars', 14000)))
        if mode == 'egypt_48h':
            config = catchup_config(config)
        atomic_write_json(orchestrator.RUNTIME_CONFIG, config)
        return config
    try:
        orchestrator.prepare_runtime_config = runtime
        if mode == 'egypt_48h':
            search.lane_definitions = definitions
        with installed():
            yield
    finally:
        orchestrator.prepare_runtime_config = old_config
        search.lane_definitions = old_definitions


def export_egypt_catchup(started, latest):
    pending = read_json(ROOT / 'output/pending_runs.json')
    reported = read_json(ROOT / 'state/reported_runs.json', {})
    reported_parts = set((reported.get('reported_parts') or {}).keys())
    reported_jobs = set((reported.get('reported_jobs') or {}).keys())
    selected, seen_ids, runs = [], set(), {}
    cutoff = started - timedelta(hours=48)
    for run in pending.get('runs', []):
        stamp = datetime.fromisoformat(str(run.get('generated_at_utc')).replace('Z', '+00:00'))
        for meta in run.get('delivery_parts', []):
            if meta.get('delivery_tier') != 'local' or meta.get('part_id') in reported_parts:
                continue
            payload = read_json(resolve_reference(ROOT, meta['path'], 'output/delivery'))
            validate_part_payload(payload, meta)
            for c in payload['review_candidates']:
                job_id = str(c['linkedin_job_id'])
                if job_id in reported_jobs or job_id in seen_ids:
                    continue
                age = c.get('estimated_age_minutes')
                posted = stamp - timedelta(minutes=float(age)) if age is not None else None
                if posted and posted < cutoff:
                    continue
                seen_ids.add(job_id)
                selected.append({**c, 'source_part_id': meta['part_id'], 'source_part_path': meta['path'],
                                 'source_run_id': run['run_id'], 'batch_generated_at_utc': run.get('generated_at_utc'),
                                 'estimated_posted_at_utc': posted.isoformat() if posted else None})
                runs[run['run_id']] = run.get('run_finished_at_utc') or run.get('generated_at_utc')
    data = {'schema_version': 1, 'run_id': latest['run_id'], 'mode': 'egypt_48h',
            'started_at_utc': started.isoformat(), 'window_start_utc': cutoff.isoformat(),
            'finished_at_utc': latest.get('run_finished_at_utc') or latest.get('generated_at_utc'),
            'note': 'Unacknowledged Egypt candidates from the existing queue plus the fresh 48-hour Egypt scan. Not yet GPT-reviewed. Ages are scan evidence, not a current application-status guarantee.',
            'candidate_count': len(selected), 'runs': runs, 'candidates': selected}
    atomic_write_json(ROOT / 'output/egypt_catchup.json', data)
    return {'candidate_count': len(selected), 'source_run_count': len(runs), 'path': 'output/egypt_catchup.json'}


def run(cli, mode='normal'):
    started = datetime.now(timezone.utc)
    original_config = (ROOT / 'queries_v13.json').read_bytes()
    released = release_unreviewed_egypt() if mode == 'egypt_48h' else 0
    with run_scope(mode):
        result = pipeline.run_pipeline(cli)
    if (ROOT / 'queries_v13.json').read_bytes() != original_config:
        raise RuntimeError('Persistent Egypt search configuration changed unexpectedly')
    latest = read_json(ROOT / 'output/latest.json')
    latest['execution_mode'] = mode
    latest['egypt_query_defaults_unchanged'] = True
    latest['catchup_released_unreviewed_egypt_seen_records'] = released
    if mode == 'egypt_48h':
        names = {d['name'] for d in latest.get('search_lanes', [])}
        if not names or not names <= {'egypt', 'remote_egypt'}:
            raise RuntimeError('Catch-up unexpectedly executed an international discovery lane')
        saturated = [q.get('query') for q in latest.get('query_stats', []) if q.get('pages_requested', 0) >= q.get('pages_configured', 60) and (q.get('page_counts') or [0])[-1] == 10]
        latest['catchup_page_ceiling_queries'] = saturated
        if saturated:
            latest.setdefault('warnings', []).append('Catch-up pagination ceiling reached; coverage incomplete: ' + ', '.join(saturated))
        latest['egypt_catchup'] = export_egypt_catchup(started, latest)
    atomic_write_json(ROOT / 'output/latest.json', latest)
    return {**result, 'mode': mode, 'current_queue': synchronize(), 'egypt_catchup': latest.get('egypt_catchup')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cli', required=True, type=Path)
    parser.add_argument('--mode', choices=['normal', 'egypt_48h', 'trigger'], default='normal')
    args = parser.parse_args()
    mode = args.mode
    if mode == 'trigger':
        try:
            mode = json.loads((ROOT / '.radar_trigger').read_text()).get('mode', 'normal')
        except (ValueError, FileNotFoundError):
            mode = 'normal'
        if mode not in {'normal', 'egypt_48h'}:
            raise SystemExit('Invalid one-off radar trigger mode')
    print(json.dumps(run(args.cli, mode), ensure_ascii=False))
