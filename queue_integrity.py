#!/usr/bin/env python3
"""Integrity and reconciliation helpers for the durable GPT review queue.

The queue is treated as a ledger, not a suggestion: a delivery part is only
removed after its part ID is acknowledged, or after every Job ID it contains
has an explicit candidate-level receipt. Every surviving part is validated
before the pending index is rewritten.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any | None = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        if default is None:
            raise
        return json.loads(json.dumps(default))


def atomic_write_json(path: Path, data: Any) -> None:
    """Durably replace a JSON file so a killed workflow cannot leave half JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def parse_utc(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_reference(root: Path, reference: str, default_directory: str) -> Path:
    path = Path(str(reference or ""))
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == "output":
        return root / path
    return root / default_directory / path.name


def normalize_job_ids(values: list[Any]) -> list[str]:
    return [str(value).strip() for value in values if str(value).strip()]


def job_ids_digest(job_ids: list[str]) -> str:
    payload = "\n".join(job_ids).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _part_job_ids(payload: dict[str, Any]) -> list[str]:
    candidates = payload.get("review_candidates")
    if not isinstance(candidates, list):
        raise ValueError("review_candidates must be a list")
    job_ids = normalize_job_ids([
        candidate.get("linkedin_job_id")
        for candidate in candidates
        if isinstance(candidate, dict)
    ])
    if len(job_ids) != len(candidates):
        raise ValueError("every review candidate must have a non-empty linkedin_job_id")
    if len(set(job_ids)) != len(job_ids):
        raise ValueError("duplicate linkedin_job_id inside one delivery part")
    return job_ids


def validate_part_payload(
    payload: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    job_ids = _part_job_ids(payload)
    declared = int(payload.get("candidate_count", -1))
    if declared != len(job_ids):
        raise ValueError(f"candidate_count={declared} but payload contains {len(job_ids)} jobs")

    expected = payload.get("expected_job_ids")
    if expected is not None and normalize_job_ids(list(expected)) != job_ids:
        raise ValueError("expected_job_ids does not match review_candidates order")

    digest = job_ids_digest(job_ids)
    integrity = payload.get("integrity") or {}
    if integrity.get("job_ids_sha256") and integrity["job_ids_sha256"] != digest:
        raise ValueError("delivery part Job-ID digest mismatch")
    if integrity.get("job_id_count") is not None and int(integrity["job_id_count"]) != len(job_ids):
        raise ValueError("integrity.job_id_count mismatch")

    if metadata:
        if int(metadata.get("candidate_count", -1)) != len(job_ids):
            raise ValueError("pending index candidate_count does not match delivery file")
        meta_ids = metadata.get("job_ids")
        if meta_ids is not None and normalize_job_ids(list(meta_ids)) != job_ids:
            raise ValueError("pending index job_ids does not match delivery file")
        if metadata.get("job_ids_sha256") and metadata["job_ids_sha256"] != digest:
            raise ValueError("pending index Job-ID digest mismatch")

    return {
        "candidate_count": len(job_ids),
        "job_ids": job_ids,
        "job_ids_sha256": digest,
    }


def refresh_part_payload(
    payload: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    refreshed = dict(payload)
    refreshed["review_candidates"] = candidates
    job_ids = normalize_job_ids([candidate.get("linkedin_job_id") for candidate in candidates])
    refreshed["candidate_count"] = len(job_ids)
    refreshed["expected_job_ids"] = job_ids
    refreshed["integrity"] = {
        "complete_candidate_list": True,
        "job_id_count": len(job_ids),
        "job_ids_sha256": job_ids_digest(job_ids),
    }
    return refreshed


def refresh_part_metadata(
    metadata: dict[str, Any],
    validation: dict[str, Any],
    compact_chars: int,
) -> dict[str, Any]:
    refreshed = dict(metadata)
    refreshed["candidate_count"] = validation["candidate_count"]
    refreshed["job_ids"] = validation["job_ids"]
    refreshed["job_ids_sha256"] = validation["job_ids_sha256"]
    refreshed["compact_chars"] = compact_chars
    return refreshed


_CONTENT_SUFFIX_RE = re.compile(r"-[0-9a-f]{12}$")


def content_addressed_part_id(part_id: str, digest: str) -> str:
    """Bind a part receipt to its exact Job-ID manifest, not only its position."""
    base = _CONTENT_SUFFIX_RE.sub("", str(part_id or "").strip())
    if not base:
        raise ValueError("delivery part must have a non-empty part_id")
    return f"{base}-{digest[:12]}"


def _replace_reference_filename(reference: str, filename: str) -> str:
    path = Path(str(reference or ""))
    return str(path.with_name(filename)) if path.name else filename


def canonicalize_part_file(
    part: dict[str, Any],
    payload: dict[str, Any],
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any], Path, dict[str, Any]]:
    """Rename schema-v4 packets to a Job-ID-manifest-addressed ID."""
    validation = validate_part_payload(payload)
    current_id = str(payload.get("part_id") or part.get("part_id") or "")
    canonical_id = content_addressed_part_id(current_id, validation["job_ids_sha256"])
    target = path.with_name(f"{canonical_id}.json")
    if canonical_id != current_id or target != path:
        payload = dict(payload)
        payload["part_id"] = canonical_id
        atomic_write_json(target, payload)
        if target != path:
            path.unlink(missing_ok=True)
        path = target
    part = dict(part)
    part["part_id"] = canonical_id
    part["path"] = _replace_reference_filename(part.get("path", ""), target.name)
    return part, payload, path, validation


def reconcile_pending_queue(
    root: Path,
    pending_path: Path,
    reported_path: Path,
    *,
    backlog_warning_candidates: int = 500,
    now: datetime | None = None,
    delete_acknowledged: bool = True,
) -> dict[str, Any]:
    """Apply receipts, validate survivors, and atomically rebuild pending_runs.json."""
    now = now or datetime.now(timezone.utc)
    pending = read_json(
        pending_path,
        {"schema_version": 2, "retention_hours": 168, "runs": []},
    )
    reported = read_json(
        reported_path,
        {"schema_version": 3, "reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_runs = set((reported.get("reported_runs") or {}).keys())
    reported_parts = set((reported.get("reported_parts") or {}).keys())
    reported_jobs = set((reported.get("reported_jobs") or {}).keys())

    kept_runs: list[dict[str, Any]] = []
    removed_parts = 0
    removed_jobs = 0
    validated_parts = 0

    for original_entry in pending.get("runs", []):
        entry = dict(original_entry)
        run_id = str(entry.get("run_id") or "")
        parts = list(entry.get("delivery_parts") or [])

        if run_id and run_id in reported_runs:
            for part in parts:
                path = resolve_reference(root, part.get("path", ""), "output/delivery")
                if delete_acknowledged:
                    path.unlink(missing_ok=True)
            for reference in entry.get("parts", []):
                path = resolve_reference(root, reference, "output/runs")
                if delete_acknowledged:
                    path.unlink(missing_ok=True)
            removed_parts += len(parts)
            continue

        remaining_parts: list[dict[str, Any]] = []
        for original_part in parts:
            part = dict(original_part)
            path = resolve_reference(root, part.get("path", ""), "output/delivery")
            if not path.is_file():
                raise FileNotFoundError(f"pending delivery part is missing: {path}")
            payload = read_json(path)

            # Schema-v4 packet IDs include the Job-ID manifest digest. A receipt
            # for old `run-part-001` content can therefore never acknowledge a
            # different packet that happens to reuse ordinal 001 after re-chunking.
            if int(payload.get("schema_version", 0) or 0) >= 4:
                part, payload, path, validation = canonicalize_part_file(part, payload, path)
            else:
                validation = validate_part_payload(payload)

            part_id = str(part.get("part_id") or "")
            if part_id in reported_parts:
                if delete_acknowledged:
                    path.unlink(missing_ok=True)
                removed_parts += 1
                continue

            candidates = list(payload.get("review_candidates") or [])
            if reported_jobs:
                survivors = [
                    candidate
                    for candidate in candidates
                    if str(candidate.get("linkedin_job_id") or "") not in reported_jobs
                ]
                removed_jobs += len(candidates) - len(survivors)
                if not survivors:
                    if delete_acknowledged:
                        path.unlink(missing_ok=True)
                    removed_parts += 1
                    continue
                if len(survivors) != len(candidates):
                    payload = refresh_part_payload(payload, survivors)
                    if int(payload.get("schema_version", 0) or 0) >= 4:
                        part, payload, path, validation = canonicalize_part_file(part, payload, path)
                    else:
                        atomic_write_json(path, payload)
                        validation = validate_part_payload(payload)

            compact_chars = len(json.dumps(payload, ensure_ascii=False))
            part = refresh_part_metadata(part, validation, compact_chars)
            validate_part_payload(payload, part)
            validated_parts += 1
            remaining_parts.append(part)

        if not remaining_parts:
            for reference in entry.get("parts", []):
                path = resolve_reference(root, reference, "output/runs")
                if delete_acknowledged:
                    path.unlink(missing_ok=True)
            continue

        entry["delivery_parts"] = remaining_parts
        entry["candidate_count"] = sum(int(part["candidate_count"]) for part in remaining_parts)
        entry["integrity"] = {
            "status": "validated",
            "delivery_part_count": len(remaining_parts),
            "candidate_count": entry["candidate_count"],
        }
        kept_runs.append(entry)

    candidate_count = sum(int(entry.get("candidate_count", 0)) for entry in kept_runs)
    part_count = sum(len(entry.get("delivery_parts", [])) for entry in kept_runs)
    timestamps = [parse_utc(entry.get("generated_at_utc")) for entry in kept_runs]
    oldest = min((stamp for stamp in timestamps if stamp is not None), default=None)
    oldest_age_minutes = max(0, int((now - oldest).total_seconds() // 60)) if oldest else 0

    pending["schema_version"] = max(3, int(pending.get("schema_version", 0) or 0))
    pending["updated_at_utc"] = iso_utc(now)
    pending["runs"] = kept_runs
    pending["backlog"] = {
        "pending_run_count": len(kept_runs),
        "pending_part_count": part_count,
        "pending_candidate_count": candidate_count,
        "oldest_candidate_age_minutes": oldest_age_minutes,
        "warning": candidate_count > int(backlog_warning_candidates) or oldest_age_minutes > 120,
    }
    pending["integrity"] = {
        "status": "validated",
        "validated_parts": validated_parts,
        "removed_acknowledged_parts": removed_parts,
        "removed_acknowledged_jobs": removed_jobs,
        "acknowledgement_granularity": "part_with_candidate_receipt_support",
    }
    atomic_write_json(pending_path, pending)
    return pending
