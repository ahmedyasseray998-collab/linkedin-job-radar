#!/usr/bin/env python3
"""Run broad, lossless LinkedIn discovery and publish durable review batches.

Discovery intentionally stays broad because advertised titles and LinkedIn/HR
metadata are not reliable enough to decide job fit. Every new Job ID that can
be exact-detail fetched reaches ChatGPT review. Code only annotates candidates.
"""
import argparse
import copy
import json
import re
import sys
from datetime import datetime, timedelta, timezone
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
        "schema_version": 7,
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


def archive_run(payload, retention_hours=168, chunk_size=25, pending_path=PENDING_RUNS,
                reported_path=REPORTED_RUNS, runs_dir=RUNS_DIR):
    now = parse_utc(payload["generated_at_utc"]) or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=int(retention_hours))
    reported_state = read_json(reported_path, {"schema_version": 1, "reported_runs": {}})
    reported = set(reported_state.get("reported_runs", {}))
    pending = read_json(pending_path, {"schema_version": 1, "runs": []})

    kept = []
    for entry in pending.get("runs", []):
        timestamp = parse_utc(entry.get("generated_at_utc"))
        keep = entry.get("run_id") not in reported and timestamp is not None and timestamp >= cutoff
        if keep:
            kept.append(entry)
        else:
            for part in entry.get("parts", []):
                part_path = ROOT / part if pending_path == PENDING_RUNS else runs_dir / Path(part).name
                if part_path.is_file():
                    part_path.unlink()

    run_id = payload["run_id"]
    candidates = payload.get("review_candidates", [])
    chunks = [candidates[index:index + chunk_size] for index in range(0, len(candidates), chunk_size)] or [[]]
    parts = []
    runs_dir.mkdir(parents=True, exist_ok=True)
    for index, candidates_part in enumerate(chunks, start=1):
        filename = f"{run_id}-part-{index:03d}.json"
        part_path = runs_dir / filename
        write_json(part_path, {
            "schema_version": 1, "run_id": run_id, "generated_at_utc": payload["generated_at_utc"],
            "health": payload["health"], "warnings": payload.get("warnings", []),
            "part": index, "part_count": len(chunks), "review_candidates": candidates_part,
        })
        parts.append(str(Path("output") / "runs" / filename) if pending_path == PENDING_RUNS else filename)

    kept = [entry for entry in kept if entry.get("run_id") != run_id]
    kept.append({
        "run_id": run_id, "generated_at_utc": payload["generated_at_utc"],
        "health": payload["health"], "warnings": payload.get("warnings", []),
        "candidate_count": len(candidates), "parts": parts,
    })
    index = {
        "schema_version": 1, "updated_at_utc": payload["generated_at_utc"],
        "retention_hours": int(retention_hours), "runs": kept,
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
    )
    merged["delivery_queue"] = {
        "pending_run_count": len(pending["runs"]),
        "pending_candidate_count": sum(int(entry.get("candidate_count", 0)) for entry in pending["runs"]),
        "index": "output/pending_runs.json", "reported_state": "state/reported_runs.json",
    }
    write_json(FINAL_LATEST, merged)
    print(json.dumps({
        "run_id": merged["run_id"], "health": merged["health"],
        "review_candidates": len(merged["review_candidates"]), "pending_runs": len(pending["runs"]),
        "lane_cards": {definition["name"]: payload.get("stats", {}).get("unique_live_cards", 0)
                       for definition, payload in zip(definitions, payloads)},
        "errors": len(merged["errors"]),
    }))


if __name__ == "__main__":
    main()
