#!/usr/bin/env python3
"""Reconcile GPT acknowledgements into the pending delivery queue.

Run after rebasing onto the latest main branch, immediately before push. This
closes the race where a GPT acknowledgement lands while the LinkedIn scan is
still running and the scan would otherwise publish a stale queue snapshot.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from queue_integrity import read_json, reconcile_pending_queue

ROOT = Path(__file__).resolve().parent
PENDING = ROOT / "output" / "pending_runs.json"
REPORTED = ROOT / "state" / "reported_runs.json"
CONFIG = ROOT / "queries_v13.json"
LATEST = ROOT / "output" / "latest.json"


def verify_policy_migration_guard(latest_path: Path = LATEST) -> None:
    """Refuse to publish policy-15 output that skipped legacy pre-retargeting.

    A scan that started before the ordering repair can finish after newer code is
    merged. Its local pipeline may already have reconciled old acknowledged
    packets before retargeting their lossless candidates. The final publish step
    always rebases and runs this script from current main, so this guard stops
    that stale in-flight state before it can replace the preserved repository
    queue. Fresh policy-15 runs carry an explicit pre_scan_retarget audit.
    """
    latest = read_json(latest_path, {})
    policy = int(latest.get("targeting_policy_version", 0) or 0)
    if policy >= 15 and "pre_scan_retarget" not in latest:
        raise SystemExit(
            "Refusing to publish policy-15 state without pre_scan_retarget audit; "
            "rerun with the current radar_pipeline_v15.py"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pending", type=Path, default=PENDING)
    parser.add_argument("--reported", type=Path, default=REPORTED)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--latest", type=Path, default=LATEST)
    args = parser.parse_args()

    verify_policy_migration_guard(args.latest)
    config = read_json(CONFIG, {})
    pending = reconcile_pending_queue(
        args.root,
        args.pending,
        args.reported,
        backlog_warning_candidates=int(config.get("delivery_backlog_warning_candidates", 500)),
    )
    print(json.dumps({
        "status": pending.get("integrity", {}).get("status"),
        **pending.get("backlog", {}),
        **pending.get("integrity", {}),
    }))


if __name__ == "__main__":
    main()
