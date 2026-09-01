#!/usr/bin/env python3
"""Run broad, lossless LinkedIn discovery and publish durable review batches.

Discovery intentionally stays broad because advertised titles and LinkedIn/HR
metadata are not reliable enough to decide job fit. Every new Job ID that can
be exact-detail fetched is archived. Compact delivery records reduce ChatGPT
input size without deleting the full descriptions used for uncertain matches.
"""
import argparse
import copy
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import radar_v5 as radar

ROOT = Path(__file__).resolve().parent
BASE_CONFIG = ROOT / "queries.json"
SHARED_SEEN = ROOT / "state" / "seen.json"
LANE_HEALTH = ROOT / "state" / "lane_health.json"
REPORTED_RUNS = ROOT / "state" / "reported_runs.json"
FINAL_LATEST = ROOT / "output" / "latest.json"
PENDING_RUNS = ROOT / "output" / "pending_runs.json"
RUNS_DIR = ROOT / "output" / "runs"
DELIVERY_DIR = ROOT / "output" / "delivery"
TMP_DIR = ROOT / "output" / ".multilane"


def read_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        if default is None:
            raise
        return copy.deepcopy(default)


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_utc(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def slug(value):
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")


def remote_search_page(cli_path, query, location, window_minutes, page):
    payload = radar.run_cli(cli_path, [
        "search", "--query", query, "--location", location,
        "--remote", "remote", "--jobage-minutes", str(window_minutes),
        "--page", str(page), "--limit", "10", "--format", "json",
    ])
    return payload.get("results", [])


def run_pass(cli_path, config_path, latest_path, remote_only=False):
    old_config, old_seen, old_latest = radar.CONFIG_PATH, radar.SEEN_PATH, radar.LATEST_PATH
    old_search, old_argv = radar.search_page, list(sys.argv)
    try:
        radar.CONFIG_PATH, radar.SEEN_PATH, radar.LATEST_PATH = config_path, SHARED_SEEN, latest_path
        if remote_only:
            radar.search_page = remote_search_page
        sys.argv = ["radar_v5.py", "--cli", str(cli_path)]
        radar.main()
        return read_json(latest_path)
    finally:
        radar.CONFIG_PATH, radar.SEEN_PATH, radar.LATEST_PATH = old_config, old_seen, old_latest
        radar.search_page, sys.argv = old_search, old_argv


def prepare_remote_config(base, location):
    remote = copy.deepcopy(base)
    remote["location"] = location
    pages = int(base.get("remote_pages_per_query", 1))
    retry_queries = {str(query).casefold().strip() for query in base.get("priority_retry_queries", [])}
    for spec in remote.get("queries", []):
        spec["pages"] = pages
        if retry_queries:
            spec["retry_if_empty"] = spec["query"].casefold().strip() in retry_queries
    return remote


def lane_definitions(base):
    definitions = [
        {
            "name": f"remote_{slug(location)}",
            "location": location,
            "remote_filter": "remote",
            "pages_per_query": int(base.get("remote_pages_per_query", 1)),
        }
        for location in base.get("remote_locations", [])
    ]
    definitions.append({"name": "egypt", "location": base.get("location", "Egypt"), "remote_filter": None})
    return definitions


def tag_lane(payload, definition):
    tagged = copy.deepcopy(payload)
    for stat in tagged.get("query_stats", []):
        stat.update({
            "lane": definition["name"],
            "search_location": definition["location"],
            "remote_filter": definition["remote_filter"],
        })
    for candidate in tagged.get("review_candidates", []):
        candidate["discovery_lane"] = definition["name"]
        candidate["discovery_remote_filter"] = definition["remote_filter"]
        candidate["remote_eligibility"] = "requires_description_review" if definition["remote_filter"] else None
        for matched in candidate.get("matched_queries", []):
            matched.update({
                "lane": definition["name"],
                "search_location": definition["location"],
                "remote_filter": definition["remote_filter"],
            })
    tagged["new_matches"] = tagged.get("review_candidates", [])
    return tagged


def label_hits(items):
    return [item.get("label") for item in items or [] if item.get("label")]


def matched_query_names(items):
    names = []
    for item in items or []:
        value = item if isinstance(item, str) else item.get("query")
        if value and value not in names:
            names.append(value)
    return names


def remote_eligibility_annotation(candidate):
    """Annotate remote restrictions without rejecting ambiguous jobs."""
    text = " ".join([
        str(candidate.get("location") or ""),
        str(candidate.get("description") or ""),
    ]).casefold()
    eligible_patterns = [
        "worldwide", "work from anywhere", "anywhere in the world", "global remote",
        "remote across emea", "remote within emea", "based in emea", "egypt",
    ]
    restriction_patterns = [
        "must reside in", "must be located in", "must be based in",
        "authorized to work in", "authorised to work in", "us citizenship required",
        "u.s. citizenship required", "security clearance required", "no visa sponsorship",
    ]
    eligible = [pattern for pattern in eligible_patterns if pattern in text]
    restrictions = [pattern for pattern in restriction_patterns if pattern in text]
    if eligible:
        status = "explicit_egypt_emea_or_global_signal"
    elif restrictions:
        status = "explicit_location_or_work_authorization_restriction"
    else:
        status = "requires_full_review"
    return {"status": status, "eligible_signals": eligible, "restriction_signals": restrictions}


def compact_description(value, max_chars=900):
    """Keep decision-bearing text while the lossless source remains archived."""
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    segments = re.split(r"(?<=[.!?])\s+|\s*[|•]\s*", text)
    markers = (
        "responsib", "require", "qualif", "experience", "you will", "must ",
        "remote", "location", "reside", "authori", "citizen", "clearance", "visa",
        "windows", "active directory", "network", "infrastructure", "server", "vmware",
        "hyper-v", "forti", "cisco", "linux", "veeam", "backup", "azure", "microsoft 365",
        "office 365", "powershell", "security", "support", "operations",
    )
    selected = []
    used = 0
    for segment in segments:
        segment = segment.strip()
        if not segment or not any(marker in segment.casefold() for marker in markers):
            continue
        addition = len(segment) + (1 if selected else 0)
        if used + addition > max_chars:
            remaining = max_chars - used
            if remaining > 80:
                selected.append(segment[:remaining].rstrip())
            break
        selected.append(segment)
        used += addition
    excerpt = " ".join(selected).strip()
    if len(excerpt) < min(300, max_chars // 2):
        excerpt = text[:max_chars].rstrip()
    return excerpt


def compact_candidate(candidate, source_part, excerpt_chars=900):
    """Create a small first-pass record; never replace the lossless source record."""
    role_title = label_hits(candidate.get("role_hits_title"))
    role_description = label_hits(candidate.get("role_hits_description"))
    skills = label_hits(candidate.get("skill_hits"))
    negatives = label_hits(candidate.get("negative_hits"))
    return {
        "linkedin_job_id": candidate.get("linkedin_job_id"),
        "title": candidate.get("title"),
        "company": candidate.get("company"),
        "location": candidate.get("location"),
        "linkedin_date": candidate.get("linkedin_date"),
        "linkedin_posted_text": candidate.get("linkedin_posted_text"),
        "estimated_age_minutes": candidate.get("estimated_age_minutes"),
        "freshness_within_requested_window": candidate.get("freshness_within_requested_window"),
        "first_seen_utc": candidate.get("first_seen_utc"),
        "url": candidate.get("url"),
        "application_status": candidate.get("application_status"),
        "seniority": candidate.get("seniority"),
        "employment_type": candidate.get("employment_type"),
        "job_function": candidate.get("job_function"),
        "discovery_lane": candidate.get("discovery_lane"),
        "matched_queries": matched_query_names(candidate.get("matched_queries")),
        "advisory_score": candidate.get("score"),
        "role_hits_title": role_title,
        "role_hits_description": role_description,
        "skill_hits": skills,
        "negative_hits": negatives,
        "advisory_title_noise_signals": candidate.get("advisory_title_noise_signals", []),
        "advisory_it_evidence": candidate.get("advisory_it_evidence"),
        "remote_eligibility": remote_eligibility_annotation(candidate),
        "description_excerpt": compact_description(candidate.get("description"), excerpt_chars),
        "full_description_chars": len(str(candidate.get("description") or "")),
        "full_record_part": source_part,
        "full_review_rule": "Open the full record for any possible IT fit or any ambiguity; title alone cannot reject.",
    }


def update_lane_health(definitions, payloads, generated_at, empty_threshold, path=LANE_HEALTH):
    previous = read_json(path, {"schema_version": 1, "lanes": {}})
    lanes = {}
    for definition, payload in zip(definitions, payloads):
        name = definition["name"]
        old = previous.get("lanes", {}).get(name, {})
        cards = int(payload.get("stats", {}).get("unique_live_cards", 0) or 0)
        errors = int(payload.get("stats", {}).get("search_errors", 0) or 0)
        consecutive_empty = int(old.get("consecutive_empty_runs", 0) or 0) + 1 if cards == 0 else 0
        lanes[name] = {
            "location": definition["location"],
            "remote_filter": definition["remote_filter"],
            "last_checked_at_utc": generated_at,
            "last_card_count": cards,
            "last_search_errors": errors,
            "consecutive_empty_runs": consecutive_empty,
            "last_nonempty_at_utc": generated_at if cards else old.get("last_nonempty_at_utc"),
            "status": "degraded_empty" if consecutive_empty >= empty_threshold else (
                "degraded_errors" if errors else ("empty_observed" if cards == 0 else "healthy")
            ),
        }
    state = {"schema_version": 1, "updated_at_utc": generated_at, "lanes": lanes}
    write_json(path, state)
    remote = [value for name, value in lanes.items() if name.startswith("remote_")]
    remote_degraded = bool(remote) and all(
        lane["consecutive_empty_runs"] >= empty_threshold or lane["last_search_errors"] > 0
        for lane in remote
    )
    return state, remote_degraded


def base_health(payloads):
    healths = [payload.get("health") for payload in payloads]
    if any(health == "degraded" for health in healths):
        return "degraded"
    if any(health == "healthy_with_retries" for health in healths):
        return "healthy_with_retries"
    if all(health == "healthy_empty" for health in healths):
        return "healthy_empty"
    return "healthy"


def merge_payloads(base, definitions, raw_payloads, orchestrator_started):
    payloads = [tag_lane(payload, definition) for definition, payload in zip(definitions, raw_payloads)]
    candidates, candidate_ids = [], set()
    for payload in payloads:
        for candidate in payload.get("review_candidates", []):
            job_id = str(candidate.get("linkedin_job_id") or "")
            if job_id and job_id not in candidate_ids:
                candidate_ids.add(job_id)
                candidates.append(candidate)
    candidates.sort(key=lambda item: (
        float(item.get("score") or 0), len(item.get("skill_hits") or []),
        len(item.get("role_hits_title") or []), item.get("title") or "",
    ), reverse=True)

    query_stats, errors, warnings = [], [], []
    for payload in payloads:
        query_stats.extend(payload.get("query_stats", []))
        errors.extend(payload.get("errors", []))
        warnings.extend(payload.get("warnings", []))
    warnings = list(dict.fromkeys(warnings))

    generated_at = radar.iso(radar.now_utc())
    lane_health, remote_degraded = update_lane_health(
        definitions, payloads, generated_at, int(base.get("lane_empty_warning_runs", 3)),
    )
    if remote_degraded:
        warnings.append("All international remote discovery lanes are degraded or repeatedly empty")

    sum_keys = [
        "unique_live_cards", "already_seen", "detail_attempts", "details_fetched",
        "detail_failures", "detail_budget_skipped", "closed_candidates_kept",
        "freshness_conflicts_kept", "low_it_evidence_candidates_kept",
        "title_noise_candidates_kept", "search_errors", "query_retry_recoveries",
    ]
    stats = {key: sum(int(payload.get("stats", {}).get(key, 0) or 0) for payload in payloads) for key in sum_keys}
    stats.update({
        "query_count": len(base.get("queries", [])),
        "search_bucket_count": len(query_stats),
        "review_candidates": len(candidates),
        "relevance_rejections": 0,
        "lane_stats": {definition["name"]: payload.get("stats", {}) for definition, payload in zip(definitions, payloads)},
    })

    health = base_health(payloads)
    if remote_degraded and health != "degraded":
        health = "degraded_remote_discovery"
    return {
        "schema_version": 8,
        "config_version": int(base.get("config_version", 1)),
        "run_id": orchestrator_started.strftime("%Y%m%dT%H%M%SZ"),
        "run_started_at_utc": radar.iso(orchestrator_started),
        "generated_at_utc": generated_at,
        "health": health,
        "warnings": warnings,
        "lane_health": lane_health["lanes"],
        "source": "LinkedIn jobs-guest via pinned MadsLorentzen/ai-job-search linkedin-search CLI",
        "search_lanes": definitions,
        "window_minutes": int(base.get("window_minutes", 180)),
        "design": "broad multi-region discovery -> shared Job-ID dedupe -> exact detail fetch -> durable lossless ChatGPT review queue",
        "policy": {
            "relevance_rejections": 0,
            "closed_application_rejections": 0,
            "freshness_conflict_rejections": 0,
            "weak_keyword_rejections": 0,
            "title_noise_rejections": 0,
            "principle": "HR titles and metadata are advisory only; ChatGPT reviews every exact-detail-fetched candidate",
        },
        "stats": stats,
        "query_stats": query_stats,
        "errors": errors[:100],
        "review_candidates": candidates,
        "new_matches": candidates,
    }


def queue_path(value, directory):
    path = Path(value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == "output":
        return ROOT / path
    return directory / path.name


def queue_reference(directory, filename, default_directory):
    if directory == default_directory:
        return str(Path("output") / directory.name / filename)
    return filename


def delivery_part_from_source(source_path, delivery_path, source_reference,
                              excerpt_chars, part_id=None):
    source = read_json(source_path)
    candidates = source.get("review_candidates", [])
    part_id = part_id or source_path.stem
    compact = [compact_candidate(candidate, source_reference, excerpt_chars) for candidate in candidates]
    delivery = {
        "schema_version": 2,
        "part_id": part_id,
        "run_id": source.get("run_id"),
        "generated_at_utc": source.get("generated_at_utc"),
        "health": source.get("health"),
        "warnings": source.get("warnings", []),
        "part": source.get("part"),
        "part_count": source.get("part_count"),
        "candidate_count": len(compact),
        "review_candidates": compact,
    }
    write_json(delivery_path, delivery)
    return delivery


def migrate_delivery_parts(entry, runs_dir, delivery_dir, excerpt_chars):
    migrated = []
    delivery_dir.mkdir(parents=True, exist_ok=True)
    for source_reference in entry.get("parts", []):
        source_path = queue_path(source_reference, runs_dir)
        if not source_path.is_file():
            continue
        part_id = source_path.stem
        filename = f"{part_id}.json"
        delivery_path = delivery_dir / filename
        delivery = delivery_part_from_source(
            source_path, delivery_path, source_reference, excerpt_chars, part_id=part_id,
        )
        migrated.append({
            "part_id": part_id,
            "path": queue_reference(delivery_dir, filename, DELIVERY_DIR),
            "source_part": source_reference,
            "candidate_count": delivery["candidate_count"],
            "compact_chars": len(json.dumps(delivery, ensure_ascii=False)),
        })
    return migrated


def archive_run(payload, retention_hours=168, chunk_size=25, pending_path=PENDING_RUNS,
                reported_path=REPORTED_RUNS, runs_dir=RUNS_DIR, delivery_dir=DELIVERY_DIR,
                excerpt_chars=900, backlog_warning_candidates=250):
    now = parse_utc(payload["generated_at_utc"]) or datetime.now(timezone.utc)
    reported_state = read_json(
        reported_path,
        {"schema_version": 2, "reported_runs": {}, "reported_parts": {}},
    )
    reported_runs = set(reported_state.get("reported_runs", {}))
    reported_parts = set(reported_state.get("reported_parts", {}))
    pending = read_json(pending_path, {"schema_version": 2, "runs": []})

    kept = []
    for old_entry in pending.get("runs", []):
        entry = copy.deepcopy(old_entry)
        if entry.get("run_id") in reported_runs:
            delivery_parts = entry.get("delivery_parts", [])
            for reference in entry.get("parts", []):
                path = queue_path(reference, runs_dir)
                if path.is_file():
                    path.unlink()
            for part in delivery_parts:
                path = queue_path(part.get("path", ""), delivery_dir)
                if path.is_file():
                    path.unlink()
            continue

        if not entry.get("delivery_parts"):
            entry["delivery_parts"] = migrate_delivery_parts(
                entry, runs_dir, delivery_dir, excerpt_chars,
            )

        unreported = []
        unreported_sources = []
        for part in entry.get("delivery_parts", []):
            if part.get("part_id") in reported_parts:
                for reference, directory in (
                    (part.get("path", ""), delivery_dir),
                    (part.get("source_part", ""), runs_dir),
                ):
                    path = queue_path(reference, directory)
                    if path.is_file():
                        path.unlink()
                continue
            unreported.append(part)
            if part.get("source_part"):
                unreported_sources.append(part["source_part"])

        if unreported:
            entry["delivery_parts"] = unreported
            entry["parts"] = unreported_sources
            entry["candidate_count"] = sum(int(part.get("candidate_count", 0)) for part in unreported)
            kept.append(entry)

    run_id = payload["run_id"]
    candidates = payload.get("review_candidates", [])
    chunks = [candidates[index:index + chunk_size] for index in range(0, len(candidates), chunk_size)] or [[]]
    full_parts, delivery_parts = [], []
    runs_dir.mkdir(parents=True, exist_ok=True)
    delivery_dir.mkdir(parents=True, exist_ok=True)
    for index, candidates_part in enumerate(chunks, start=1):
        part_id = f"{run_id}-part-{index:03d}"
        filename = f"{part_id}.json"
        source_reference = queue_reference(runs_dir, filename, RUNS_DIR)
        delivery_reference = queue_reference(delivery_dir, filename, DELIVERY_DIR)
        source_path = runs_dir / filename
        delivery_path = delivery_dir / filename
        write_json(source_path, {
            "schema_version": 2, "run_id": run_id, "generated_at_utc": payload["generated_at_utc"],
            "health": payload["health"], "warnings": payload.get("warnings", []),
            "part": index, "part_count": len(chunks), "review_candidates": candidates_part,
        })
        delivery = delivery_part_from_source(
            source_path, delivery_path, source_reference, excerpt_chars, part_id=part_id,
        )
        full_parts.append(source_reference)
        delivery_parts.append({
            "part_id": part_id,
            "path": delivery_reference,
            "source_part": source_reference,
            "candidate_count": delivery["candidate_count"],
            "compact_chars": len(json.dumps(delivery, ensure_ascii=False)),
        })

    kept = [entry for entry in kept if entry.get("run_id") != run_id]
    kept.append({
        "run_id": run_id, "generated_at_utc": payload["generated_at_utc"],
        "health": payload["health"], "warnings": payload.get("warnings", []),
        "candidate_count": len(candidates), "parts": full_parts,
        "delivery_parts": delivery_parts,
    })
    candidate_count = sum(int(entry.get("candidate_count", 0)) for entry in kept)
    part_count = sum(len(entry.get("delivery_parts", [])) for entry in kept)
    timestamps = [parse_utc(entry.get("generated_at_utc")) for entry in kept]
    oldest = min((timestamp for timestamp in timestamps if timestamp is not None), default=None)
    oldest_age_minutes = int((now - oldest).total_seconds() // 60) if oldest else 0
    index = {
        "schema_version": 2,
        "updated_at_utc": payload["generated_at_utc"],
        "retention_hours": int(retention_hours),
        "runs": kept,
        "backlog": {
            "pending_run_count": len(kept),
            "pending_part_count": part_count,
            "pending_candidate_count": candidate_count,
            "oldest_candidate_age_minutes": oldest_age_minutes,
            "warning": candidate_count > int(backlog_warning_candidates) or oldest_age_minutes > 120,
        },
    }
    write_json(pending_path, index)
    return index


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    args = parser.parse_args()
    cli_path = Path(args.cli)
    if not cli_path.exists():
        raise SystemExit(f"LinkedIn CLI not found: {cli_path}")

    base = read_json(BASE_CONFIG)
    definitions = lane_definitions(base)
    orchestrator_started = radar.now_utc()
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    payloads = []
    for definition in definitions:
        latest_path = TMP_DIR / f"{definition['name']}.json"
        if definition["remote_filter"]:
            config_path = TMP_DIR / f"{definition['name']}_queries.json"
            write_json(config_path, prepare_remote_config(base, definition["location"]))
            payloads.append(run_pass(cli_path, config_path, latest_path, remote_only=True))
        else:
            payloads.append(run_pass(cli_path, BASE_CONFIG, latest_path, remote_only=False))

    merged = merge_payloads(base, definitions, payloads, orchestrator_started)
    pending = archive_run(
        merged, retention_hours=int(base.get("run_archive_retention_hours", 168)),
        chunk_size=int(base.get("run_archive_chunk_size", 25)),
        excerpt_chars=int(base.get("delivery_excerpt_chars", 900)),
        backlog_warning_candidates=int(base.get("delivery_backlog_warning_candidates", 250)),
    )
    merged["delivery_queue"] = dict(pending["backlog"], **{
        "index": "output/pending_runs.json",
        "compact_parts": "output/delivery",
        "lossless_parts": "output/runs",
        "reported_state": "state/reported_runs.json",
        "acknowledgement_granularity": "part",
    })
    candidate_count = len(merged["review_candidates"])
    merged["candidate_payload"] = {
        "candidate_count": candidate_count,
        "compact_delivery_index": "output/pending_runs.json",
        "lossless_records": "output/runs",
        "note": "Candidates are intentionally stored outside latest.json to avoid duplicate large payloads.",
    }
    merged["review_candidates"] = []
    merged["new_matches"] = []
    write_json(FINAL_LATEST, merged)
    print(json.dumps({
        "run_id": merged["run_id"], "health": merged["health"],
        "review_candidates": candidate_count, "pending_runs": len(pending["runs"]),
        "lane_cards": {definition["name"]: payload.get("stats", {}).get("unique_live_cards", 0)
                       for definition, payload in zip(definitions, payloads)},
        "errors": len(merged["errors"]),
    }))


if __name__ == "__main__":
    main()
