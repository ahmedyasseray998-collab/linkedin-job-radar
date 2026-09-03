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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pending", type=Path, default=PENDING)
    parser.add_argument("--reported", type=Path, default=REPORTED)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()

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
