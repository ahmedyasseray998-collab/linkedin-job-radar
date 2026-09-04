#!/usr/bin/env python3
"""Promote content-addressed packet receipts into durable per-job receipts.

The ChatGPT reviewer normally writes a tiny one-shot file containing reviewed
part IDs. Before queue reconciliation deletes those packet files, this module
resolves each exact content-addressed part to its Job-ID manifest and records
candidate-level receipts. The packet receipt remains authoritative and is not
deleted here; radar_pipeline_v14 performs the normal idempotent merge/removal.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from queue_integrity import (
    atomic_write_json,
    read_json,
    resolve_reference,
    validate_part_payload,
)

ROOT = Path(__file__).resolve().parent


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _receipt_mapping(value: Any, default_timestamp: str) -> dict[str, str]:
    if isinstance(value, dict):
        return {
            str(key).strip(): str(timestamp or default_timestamp)
            for key, timestamp in value.items()
            if str(key).strip()
        }
    if isinstance(value, list):
        return {
            str(key).strip(): default_timestamp
            for key in value
            if str(key).strip()
        }
    return {}


def _part_job_ids(root: Path, part: dict[str, Any]) -> list[str]:
    indexed = [str(value).strip() for value in part.get("job_ids") or [] if str(value).strip()]
    if indexed:
        return indexed

    path = resolve_reference(root, str(part.get("path") or ""), "output/delivery")
    if not path.is_file():
        return []
    validation = validate_part_payload(read_json(path), part)
    return list(validation["job_ids"])


def promote_manual_receipts(root: Path = ROOT) -> dict[str, Any]:
    manual_path = root / "state" / "manual_backlog_receipts.json"
    pending_path = root / "output" / "pending_runs.json"
    reported_path = root / "state" / "reported_runs.json"
    seen_path = root / "state" / "seen.json"

    if not manual_path.is_file():
        return {
            "manual_receipt_present": False,
            "part_receipts_merged": 0,
            "job_receipts_promoted": 0,
            "unresolved_part_receipts": [],
        }

    manual = read_json(manual_path, {})
    default_timestamp = str(manual.get("reviewed_at_utc") or _iso_now())
    manual_parts = _receipt_mapping(manual.get("reported_parts"), default_timestamp)
    manual_jobs = _receipt_mapping(manual.get("reported_jobs"), default_timestamp)
    manual_runs = _receipt_mapping(manual.get("reported_runs"), default_timestamp)

    pending = read_json(pending_path, {"runs": []})
    part_index: dict[str, tuple[list[str], str]] = {}
    for entry in pending.get("runs") or []:
        run_id = str(entry.get("run_id") or "").strip()
        for part in entry.get("delivery_parts") or []:
            part_id = str(part.get("part_id") or "").strip()
            if not part_id:
                continue
            part_index[part_id] = (_part_job_ids(root, part), run_id)

    promoted_jobs = dict(manual_jobs)
    resolved_parts: list[str] = []
    unresolved_parts: list[str] = []
    inferred_runs = dict(manual_runs)
    for part_id, reviewed_at in manual_parts.items():
        indexed = part_index.get(part_id)
        if indexed is None:
            unresolved_parts.append(part_id)
            continue
        job_ids, run_id = indexed
        if not job_ids:
            unresolved_parts.append(part_id)
            continue
        resolved_parts.append(part_id)
        for job_id in job_ids:
            promoted_jobs.setdefault(job_id, reviewed_at)
        if run_id:
            run_parts = [
                str(part.get("part_id") or "")
                for entry in pending.get("runs") or []
                if str(entry.get("run_id") or "") == run_id
                for part in entry.get("delivery_parts") or []
            ]
            if run_parts and all(part in manual_parts for part in run_parts):
                inferred_runs.setdefault(run_id, reviewed_at)

    reported = read_json(
        reported_path,
        {"schema_version": 3, "reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_runs = reported.setdefault("reported_runs", {})
    reported_parts = reported.setdefault("reported_parts", {})
    reported_jobs = reported.setdefault("reported_jobs", {})

    for run_id, reviewed_at in inferred_runs.items():
        reported_runs[run_id] = reviewed_at
    for part_id, reviewed_at in manual_parts.items():
        reported_parts[part_id] = reviewed_at
    new_job_count = 0
    for job_id, reviewed_at in promoted_jobs.items():
        if job_id not in reported_jobs:
            new_job_count += 1
        reported_jobs[job_id] = reviewed_at

    reported["schema_version"] = max(3, int(reported.get("schema_version", 0) or 0))
    reported["updated_at_utc"] = default_timestamp
    atomic_write_json(reported_path, reported)

    seen = read_json(seen_path, {"jobs": {}})
    changed_seen = 0
    for job_id, reviewed_at in promoted_jobs.items():
        record = (seen.get("jobs") or {}).get(job_id)
        if not isinstance(record, dict):
            continue
        record["status"] = "reviewed_acknowledged"
        record["reviewed_at_utc"] = reviewed_at
        record.pop("retry_after_utc", None)
        record.pop("deferred_reason", None)
        changed_seen += 1
    if changed_seen:
        atomic_write_json(seen_path, seen)

    return {
        "manual_receipt_present": True,
        "part_receipts_merged": len(manual_parts),
        "resolved_part_receipts": resolved_parts,
        "unresolved_part_receipts": unresolved_parts,
        "job_receipts_promoted": len(promoted_jobs),
        "new_job_receipts": new_job_count,
        "run_receipts_merged": len(inferred_runs),
        "seen_records_marked_reviewed": changed_seen,
    }
