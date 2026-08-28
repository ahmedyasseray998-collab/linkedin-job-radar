#!/usr/bin/env python3
"""Run the lossless LinkedIn radar in two discovery lanes and merge the results.

Lane 1: Egypt, all workplace types.
Lane 2: EMEA, LinkedIn remote-work filter, one page per query.

Both lanes share state/seen.json, so exact LinkedIn Job IDs are deduplicated both
between lanes and across hourly runs. Each newly discovered ID is still exact-
detail fetched by radar_v5 and passed losslessly to ChatGPT review.
"""
import argparse
import copy
import json
import sys
from pathlib import Path

import radar_v5 as radar

ROOT = Path(__file__).resolve().parent
BASE_CONFIG = ROOT / "queries.json"
SHARED_SEEN = ROOT / "state" / "seen.json"
FINAL_LATEST = ROOT / "output" / "latest.json"
TMP_DIR = ROOT / "output" / ".multilane"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def remote_search_page(cli_path, query, location, window_minutes, page):
    payload = radar.run_cli(cli_path, [
        "search",
        "--query", query,
        "--location", location,
        "--remote", "remote",
        "--jobage-minutes", str(window_minutes),
        "--page", str(page),
        "--limit", "10",
        "--format", "json",
    ])
    return payload.get("results", [])


def run_pass(cli_path, config_path, latest_path, remote_only=False):
    old_config = radar.CONFIG_PATH
    old_seen = radar.SEEN_PATH
    old_latest = radar.LATEST_PATH
    old_search = radar.search_page
    old_argv = list(sys.argv)
    try:
        radar.CONFIG_PATH = config_path
        radar.SEEN_PATH = SHARED_SEEN
        radar.LATEST_PATH = latest_path
        if remote_only:
            radar.search_page = remote_search_page
        sys.argv = ["radar_v5.py", "--cli", str(cli_path)]
        radar.main()
        return read_json(latest_path)
    finally:
        radar.CONFIG_PATH = old_config
        radar.SEEN_PATH = old_seen
        radar.LATEST_PATH = old_latest
        radar.search_page = old_search
        sys.argv = old_argv


def prepare_remote_config(base):
    remote = copy.deepcopy(base)
    remote["location"] = "EMEA"
    # One page per query gives broad remote coverage without doubling the full
    # Egypt crawl depth. Empty-result recovery remains enabled per query.
    for spec in remote.get("queries", []):
        spec["pages"] = 1
    return remote


def tag_lane(payload, lane_name, search_location, remote_filter):
    tagged = copy.deepcopy(payload)
    for stat in tagged.get("query_stats", []):
        stat["lane"] = lane_name
        stat["search_location"] = search_location
        stat["remote_filter"] = remote_filter
    for candidate in tagged.get("review_candidates", []):
        candidate["discovery_lane"] = lane_name
        for matched in candidate.get("matched_queries", []):
            matched["lane"] = lane_name
            matched["search_location"] = search_location
            matched["remote_filter"] = remote_filter
    tagged["new_matches"] = tagged.get("review_candidates", [])
    return tagged


def merge_health(payloads):
    healths = [p.get("health") for p in payloads]
    if any(h == "degraded" for h in healths):
        return "degraded"
    if any(h == "healthy_with_retries" for h in healths):
        return "healthy_with_retries"
    if all(h == "healthy_empty" for h in healths):
        return "healthy_empty"
    return "healthy"


def merge_payloads(base, egypt_raw, remote_raw):
    egypt = tag_lane(egypt_raw, "egypt", base.get("location", "Egypt"), None)
    remote = tag_lane(remote_raw, "emea_remote", "EMEA", "remote")
    payloads = [egypt, remote]

    candidates = []
    candidate_ids = set()
    for payload in payloads:
        for candidate in payload.get("review_candidates", []):
            job_id = str(candidate.get("linkedin_job_id") or "")
            if not job_id or job_id in candidate_ids:
                continue
            candidate_ids.add(job_id)
            candidates.append(candidate)

    candidates.sort(
        key=lambda item: (
            float(item.get("score") or 0),
            len(item.get("skill_hits") or []),
            len(item.get("role_hits_title") or []),
            item.get("title") or "",
        ),
        reverse=True,
    )

    query_stats = []
    errors = []
    warnings = []
    for payload in payloads:
        query_stats.extend(payload.get("query_stats", []))
        errors.extend(payload.get("errors", []))
        warnings.extend(payload.get("warnings", []))
    warnings = list(dict.fromkeys(warnings))

    sum_keys = [
        "unique_live_cards", "already_seen", "detail_attempts", "details_fetched",
        "detail_failures", "detail_budget_skipped", "closed_candidates_kept",
        "freshness_conflicts_kept", "low_it_evidence_candidates_kept",
        "title_noise_candidates_kept", "search_errors", "query_retry_recoveries",
    ]
    stats = {
        key: sum(int(p.get("stats", {}).get(key, 0) or 0) for p in payloads)
        for key in sum_keys
    }
    stats.update({
        "query_count": len(base.get("queries", [])),
        "search_bucket_count": len(query_stats),
        "review_candidates": len(candidates),
        "relevance_rejections": 0,
        "lane_stats": {
            "egypt": egypt.get("stats", {}),
            "emea_remote": remote.get("stats", {}),
        },
    })

    return {
        "schema_version": 6,
        "config_version": int(base.get("config_version", 1)),
        "run_id": egypt.get("run_id"),
        "run_started_at_utc": egypt.get("run_started_at_utc"),
        "generated_at_utc": radar.iso(radar.now_utc()),
        "health": merge_health(payloads),
        "warnings": warnings,
        "source": "LinkedIn jobs-guest via pinned MadsLorentzen/ai-job-search linkedin-search CLI",
        "search_lanes": [
            {"name": "egypt", "location": base.get("location", "Egypt"), "remote_filter": None},
            {"name": "emea_remote", "location": "EMEA", "remote_filter": "remote", "pages_per_query": 1},
        ],
        "window_minutes": int(base.get("window_minutes", 180)),
        "design": "Egypt live discovery + EMEA remote live discovery -> shared Job-ID dedupe -> exact detail fetch -> annotate only -> lossless ChatGPT review queue",
        "policy": {
            "relevance_rejections": 0,
            "closed_application_rejections": 0,
            "freshness_conflict_rejections": 0,
            "weak_keyword_rejections": 0,
            "title_noise_rejections": 0,
            "principle": "Do not let code decide job fit after exact-detail retrieval; ChatGPT reviews every verified new candidate",
        },
        "stats": stats,
        "query_stats": query_stats,
        "errors": errors[:50],
        "review_candidates": candidates,
        "new_matches": candidates,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    args = parser.parse_args()

    cli_path = Path(args.cli)
    if not cli_path.exists():
        raise SystemExit(f"LinkedIn CLI not found: {cli_path}")

    base = read_json(BASE_CONFIG)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    egypt_latest = TMP_DIR / "egypt.json"
    remote_latest = TMP_DIR / "emea_remote.json"
    remote_config_path = TMP_DIR / "remote_queries.json"
    write_json(remote_config_path, prepare_remote_config(base))

    egypt = run_pass(cli_path, BASE_CONFIG, egypt_latest, remote_only=False)
    remote = run_pass(cli_path, remote_config_path, remote_latest, remote_only=True)
    merged = merge_payloads(base, egypt, remote)
    write_json(FINAL_LATEST, merged)

    print(json.dumps({
        "run_id": merged["run_id"],
        "health": merged["health"],
        "review_candidates": len(merged["review_candidates"]),
        "egypt_cards": egypt.get("stats", {}).get("unique_live_cards", 0),
        "emea_remote_cards": remote.get("stats", {}).get("unique_live_cards", 0),
        "errors": len(merged["errors"]),
    }))


if __name__ == "__main__":
    main()
