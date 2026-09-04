#!/usr/bin/env python3
"""Run broad v13 discovery with policy-15 targeting and seen-state lifecycle."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import targeted_queue_v15 as targeting
from receipt_promotion_v15 import promote_manual_receipts

# Install patches before importing v14 so its direct imports receive v15 policy.
targeting.install_v15_patches()
import radar_pipeline_v14 as base_pipeline  # noqa: E402
from queue_integrity import atomic_write_json, read_json  # noqa: E402

ROOT = Path(__file__).resolve().parent
LATEST = ROOT / "output" / "latest.json"

base_pipeline.POLICY_VERSION = targeting.POLICY_VERSION
base_pipeline.reprioritize_pending_queue = targeting.reprioritize_pending_queue


def _queue_health(latest: dict) -> str:
    integrity = ((latest.get("delivery_queue") or {}).get("integrity") or {}).get("status")
    warning = bool((latest.get("delivery_queue") or {}).get("warning"))
    if integrity != "validated":
        return "degraded_integrity"
    if warning:
        return "healthy_with_backlog_warning"
    return "healthy"


def run_pipeline(cli_path: Path) -> dict:
    receipt_promotion = promote_manual_receipts(ROOT)
    lifecycle_before = targeting.prepare_seen_for_run(ROOT)
    result = base_pipeline.run_pipeline(cli_path)
    lifecycle_after = targeting.annotate_seen_from_pending(ROOT)

    latest = read_json(LATEST)
    discovery_health = str(result.get("health") or latest.get("health") or "unknown")
    queue_health = _queue_health(latest)
    latest["config_version"] = targeting.POLICY_VERSION
    latest["search_strategy_version"] = targeting.POLICY_VERSION
    latest["targeting_policy_version"] = targeting.POLICY_VERSION
    latest["health_components"] = {
        "discovery": discovery_health,
        "targeting": "healthy" if not lifecycle_after.get("missing_seen_record") else "healthy_with_state_warning",
        "queue": queue_health,
        "publish": "pending_workflow_commit",
        "publish_source_of_truth": "GitHub Actions workflow conclusion",
    }
    latest["overall_pre_publish_health"] = (
        "healthy"
        if discovery_health.startswith("healthy") and queue_health.startswith("healthy")
        else "degraded"
    )
    latest["receipt_promotion"] = receipt_promotion
    latest["seen_lifecycle"] = {
        "before_scan": lifecycle_before,
        "after_targeting": lifecycle_after,
    }
    latest.setdefault("candidate_payload", {})["note"] = (
        "Egypt IT candidates are protected. Remote candidates require job-level "
        "Egypt/MENA/EMEA/Africa/global scope; on-site and hybrid roles are kept "
        "separate as relocation leads. Deferred candidates receive versioned, "
        "retryable seen-state decisions, and reviewed packet receipts are "
        "promoted to durable per-job receipts before queue cleanup."
    )
    atomic_write_json(LATEST, latest)

    return {
        **result,
        "targeting_policy_version": targeting.POLICY_VERSION,
        "receipt_promotion": receipt_promotion,
        "seen_lifecycle_before_scan": lifecycle_before,
        "seen_lifecycle_after_targeting": lifecycle_after,
        "health_components": latest["health_components"],
        "overall_pre_publish_health": latest["overall_pre_publish_health"],
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
