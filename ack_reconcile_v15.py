#!/usr/bin/env python3
"""Promote reviewer acknowledgements and immediately clean the durable queue."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from queue_integrity import (
    atomic_write_json,
    read_json,
    reconcile_pending_queue,
    resolve_reference,
    validate_part_payload,
)
from receipt_promotion_v15 import promote_manual_receipts

ROOT = Path(__file__).resolve().parent
POLICY_VERSION = 15


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def verify_legacy_retarget_complete(root: Path = ROOT) -> None:
    """Protect legacy lossless archives until policy 15 has retargeted them.

    Reviewer acknowledgements may arrive while an older packet is still indexed.
    Cleaning that packet before the policy-15 pre-pass would also delete its
    lossless archive and hide candidates that the new geography rules should
    reconsider. A current producer converts such runs to targeting policy 15
    first, after which normal acknowledgement cleanup is safe.
    """
    pending = read_json(root / "output" / "pending_runs.json", {"runs": []})
    reported = read_json(
        root / "state" / "reported_runs.json",
        {"reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_runs = set((reported.get("reported_runs") or {}).keys())
    reported_parts = set((reported.get("reported_parts") or {}).keys())
    blocked: list[str] = []

    for entry in pending.get("runs") or []:
        policy = int(entry.get("targeting_policy_version", 0) or 0)
        if policy >= POLICY_VERSION:
            continue
        run_id = str(entry.get("run_id") or "").strip()
        part_ids = {
            str(part.get("part_id") or "").strip()
            for part in entry.get("delivery_parts") or []
            if str(part.get("part_id") or "").strip()
        }
        if (run_id and run_id in reported_runs) or bool(part_ids & reported_parts):
            blocked.append(run_id or "<unknown-run>")

    if blocked:
        raise SystemExit(
            "Refusing acknowledgement cleanup before policy-15 legacy retargeting: "
            + ", ".join(sorted(set(blocked)))
        )


def _job_ids(root: Path, part: dict[str, Any]) -> list[str]:
    indexed = [str(value).strip() for value in part.get("job_ids") or [] if str(value).strip()]
    if indexed:
        return indexed
    path = resolve_reference(root, str(part.get("path") or ""), "output/delivery")
    if not path.is_file():
        return []
    return list(validate_part_payload(read_json(path), part)["job_ids"])


def promote_reported_part_receipts(root: Path = ROOT) -> dict[str, Any]:
    """Resolve exact reported part IDs into Job IDs before packets are deleted."""
    pending_path = root / "output" / "pending_runs.json"
    reported_path = root / "state" / "reported_runs.json"
    seen_path = root / "state" / "seen.json"

    pending = read_json(pending_path, {"runs": []})
    reported = read_json(
        reported_path,
        {"schema_version": 3, "reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_runs = reported.setdefault("reported_runs", {})
    reported_parts = reported.setdefault("reported_parts", {})
    reported_jobs = reported.setdefault("reported_jobs", {})
    seen = read_json(seen_path, {"jobs": {}})
    seen_jobs = seen.setdefault("jobs", {})

    promoted_jobs: list[str] = []
    marked_seen: list[str] = []
    inferred_runs: list[str] = []
    unresolved_parts: list[str] = []

    for entry in pending.get("runs") or []:
        run_id = str(entry.get("run_id") or "").strip()
        parts = list(entry.get("delivery_parts") or [])
        if not parts:
            continue

        all_parts_reported = True
        part_times: list[str] = []
        for part in parts:
            part_id = str(part.get("part_id") or "").strip()
            if not part_id or part_id not in reported_parts:
                all_parts_reported = False
                continue

            reviewed_at = str(reported_parts.get(part_id) or _iso_now())
            part_times.append(reviewed_at)
            job_ids = _job_ids(root, part)
            if not job_ids:
                unresolved_parts.append(part_id)
                all_parts_reported = False
                continue

            for job_id in job_ids:
                if job_id not in reported_jobs:
                    reported_jobs[job_id] = reviewed_at
                    promoted_jobs.append(job_id)
                record = seen_jobs.get(job_id)
                if isinstance(record, dict):
                    record["status"] = "reviewed_acknowledged"
                    record["reviewed_at_utc"] = reviewed_at
                    record.pop("retry_after_utc", None)
                    record.pop("deferred_reason", None)
                    marked_seen.append(job_id)

        if run_id and all_parts_reported and parts:
            reviewed_at = max(part_times) if part_times else _iso_now()
            if run_id not in reported_runs:
                reported_runs[run_id] = reviewed_at
                inferred_runs.append(run_id)

    changed = bool(promoted_jobs or inferred_runs or marked_seen)
    if changed:
        reported["schema_version"] = max(3, int(reported.get("schema_version", 0) or 0))
        reported["updated_at_utc"] = _iso_now()
        atomic_write_json(reported_path, reported)
        if marked_seen:
            atomic_write_json(seen_path, seen)

    return {
        "job_receipts_promoted": len(promoted_jobs),
        "promoted_job_ids": promoted_jobs,
        "seen_records_marked_reviewed": len(set(marked_seen)),
        "run_receipts_inferred": len(inferred_runs),
        "inferred_run_ids": inferred_runs,
        "unresolved_reported_parts": list(dict.fromkeys(unresolved_parts)),
    }


def reconcile_acknowledgements(root: Path = ROOT) -> dict[str, Any]:
    verify_legacy_retarget_complete(root)
    manual = promote_manual_receipts(root)
    promoted = promote_reported_part_receipts(root)

    # The manual file is one-shot. At this point its receipts are durably merged
    # into the main ledger, so deleting it is safe and keeps future runs idempotent.
    (root / "state" / "manual_backlog_receipts.json").unlink(missing_ok=True)

    config = read_json(root / "queries_v13.json", {})
    pending = reconcile_pending_queue(
        root,
        root / "output" / "pending_runs.json",
        root / "state" / "reported_runs.json",
        backlog_warning_candidates=int(config.get("delivery_backlog_warning_candidates", 500)),
    )
    return {
        "manual_receipt_promotion": manual,
        "reported_part_promotion": promoted,
        "backlog": pending.get("backlog", {}),
        "integrity": pending.get("integrity", {}),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    print(json.dumps(reconcile_acknowledgements(args.root), ensure_ascii=False))


if __name__ == "__main__":
    main()
