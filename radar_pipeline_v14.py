#!/usr/bin/env python3
"""Run the v13 search engine with the targeted, auditable v14 review queue."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import radar_pipeline_v13 as search_pipeline
from queue_integrity import atomic_write_json, read_json
from targeted_queue_v14 import POLICY_VERSION, reprioritize_pending_queue

ROOT = Path(__file__).resolve().parent
SOURCE_CONFIG = ROOT / "queries_v13.json"
RUNTIME_DIR = ROOT / "output" / ".pipeline_v14"
RUNTIME_CONFIG = RUNTIME_DIR / "queries.json"
LATEST = ROOT / "output" / "latest.json"
REPORTED_STATE = ROOT / "state" / "reported_runs.json"
MANUAL_RECEIPTS = ROOT / "state" / "manual_backlog_receipts.json"


def merge_manual_backlog_receipts() -> int:
    """Merge reviewed content-addressed parts without replacing the main ledger.

    The ChatGPT reviewer can write a small one-shot receipt file after validating
    every candidate in a backlog. The next radar run folds those receipts into
    state/reported_runs.json, preserving all existing acknowledgements, then
    removes the one-shot file so the merge is idempotent and auditable.
    """
    if not MANUAL_RECEIPTS.is_file():
        return 0

    manual = read_json(MANUAL_RECEIPTS, {"reported_parts": {}})
    receipts = manual.get("reported_parts") or {}
    if not isinstance(receipts, dict) or not receipts:
        return 0

    reported = read_json(
        REPORTED_STATE,
        {
            "schema_version": 3,
            "reported_runs": {},
            "reported_parts": {},
            "reported_jobs": {},
        },
    )
    reported.setdefault("reported_runs", {})
    reported.setdefault("reported_jobs", {})
    reported_parts = reported.setdefault("reported_parts", {})

    for part_id, reviewed_at in receipts.items():
        key = str(part_id or "").strip()
        if not key:
            continue
        reported_parts[key] = str(reviewed_at or manual.get("reviewed_at_utc") or "")

    reported["schema_version"] = max(3, int(reported.get("schema_version", 0) or 0))
    if manual.get("reviewed_at_utc"):
        reported["updated_at_utc"] = str(manual["reviewed_at_utc"])
    atomic_write_json(REPORTED_STATE, reported)
    MANUAL_RECEIPTS.unlink(missing_ok=True)
    return len(receipts)


def prepare_runtime_config() -> dict:
    config = read_json(SOURCE_CONFIG)
    config["config_version"] = POLICY_VERSION
    config["delivery_mode"] = "targeted_high_recall_egypt_regional"
    config["delivery_default_chunk_size"] = max(
        8,
        int(config.get("delivery_default_chunk_size", 4)),
    )
    config["delivery_excerpt_chars"] = min(
        500,
        int(config.get("delivery_excerpt_chars", 500)),
    )
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    atomic_write_json(RUNTIME_CONFIG, config)
    return config


def run_pipeline(cli_path: Path) -> dict:
    merged_manual_receipts = merge_manual_backlog_receipts()
    config = prepare_runtime_config()
    search_pipeline.CONFIG_PATH = RUNTIME_CONFIG
    search_result = search_pipeline.run_pipeline(cli_path)
    targeted = reprioritize_pending_queue(config)

    current_run_id = str(search_result.get("run_id") or "")
    current_audit = next(
        (
            item
            for item in targeted["summary"].get("runs", [])
            if str(item.get("run_id") or "") == current_run_id
        ),
        {},
    )

    latest = read_json(LATEST)
    latest["config_version"] = POLICY_VERSION
    latest["search_strategy_version"] = POLICY_VERSION
    latest["targeting_policy_version"] = POLICY_VERSION
    latest["targeting_policy"] = targeted["pending"].get("targeting_policy", {})
    latest["targeting_audit"] = current_audit
    latest["targeting_backlog_totals"] = targeted["summary"].get("totals", {})
    latest["delivery_queue"] = dict(
        targeted["pending"].get("backlog", {}),
        **{
            "index": "output/pending_runs.json",
            "compact_parts": "output/delivery",
            "lossless_parts": "output/runs",
            "reported_state": "state/reported_runs.json",
            "deferred_summary": "output/deferred_summary.json",
            "acknowledgement_granularity": (
                "single-job Egypt parts; compact content-addressed parts elsewhere"
            ),
            "integrity": targeted["pending"].get("integrity", {}),
        },
    )
    latest["candidate_payload"]["candidate_count"] = int(
        current_audit.get("selected_candidates", 0)
    )
    latest["candidate_payload"]["note"] = (
        "All Egypt IT candidates remain protected. International candidates "
        "enter GPT's active queue only with job-level regional, eligibility, "
        "or credible relocation evidence; deferred counts remain auditable."
    )
    atomic_write_json(LATEST, latest)

    return {
        **search_result,
        "targeting_policy_version": POLICY_VERSION,
        "targeting_current_run": current_audit,
        "targeting_backlog": targeted["summary"].get("totals", {}),
        "backlog": targeted["pending"].get("backlog", {}),
        "merged_manual_receipts": merged_manual_receipts,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    args = parser.parse_args()
    cli_path = Path(args.cli)
    if not cli_path.exists():
        raise SystemExit(f"LinkedIn CLI not found: {cli_path}")
    print(json.dumps(run_pipeline(cli_path), ensure_ascii=False))


if __name__ == "__main__":
    main()
