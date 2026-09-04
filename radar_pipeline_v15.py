#!/usr/bin/env python3
"""Run broad v13 discovery with policy-15 targeting and seen-state lifecycle."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import targeted_queue_v15 as targeting
from receipt_promotion_v15 import promote_manual_receipts

# Install patches before importing v14 so its direct imports receive v15 policy.
targeting.install_v15_patches()
import radar_pipeline_v14 as base_pipeline  # noqa: E402
from queue_integrity import atomic_write_json, read_json  # noqa: E402

ROOT = Path(__file__).resolve().parent
LATEST = ROOT / "output" / "latest.json"
_LAST_SEEN_DECISION_AUDIT: dict[str, int] = {}


def _decision_rank(kind: str) -> int:
    return {"deferred": 1, "selected": 2, "acknowledged": 3}.get(kind, 0)


def _capture_targeting_decisions(
    root: Path,
    pending_path: Path,
    reported_path: Path,
) -> dict[str, dict[str, Any]]:
    """Capture per-job outcomes before targeting can delete a zero-match archive."""
    pending = read_json(pending_path, {"runs": []})
    reported = read_json(
        reported_path,
        {"reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_parts = set((reported.get("reported_parts") or {}).keys())
    reported_jobs = set((reported.get("reported_jobs") or {}).keys())
    decisions: dict[str, dict[str, Any]] = {}

    for entry in pending.get("runs") or []:
        if int(entry.get("targeting_policy_version", 0) or 0) == targeting.POLICY_VERSION:
            continue
        candidates, _ = targeting.legacy._load_lossless_candidates(entry, root)
        acknowledged = targeting.legacy._acknowledged_job_ids(
            entry,
            root,
            reported_parts,
            reported_jobs,
        )
        for candidate in candidates:
            job_id = str(candidate.get("linkedin_job_id") or "").strip()
            if not job_id:
                continue
            if job_id in acknowledged:
                decision = {
                    "kind": "acknowledged",
                    "reason": "existing_candidate_or_part_receipt",
                    "tier": None,
                }
            else:
                tier, reason = targeting.classify_candidate(candidate)
                decision = {
                    "kind": "selected" if tier is not None else "deferred",
                    "reason": reason,
                    "tier": tier,
                }
            previous = decisions.get(job_id)
            if previous is None or _decision_rank(decision["kind"]) > _decision_rank(previous["kind"]):
                decisions[job_id] = decision

    return decisions


def _apply_targeting_decisions(
    root: Path,
    decisions: dict[str, dict[str, Any]],
    pending: dict[str, Any],
) -> dict[str, int]:
    """Persist decisions even when targeting removes a run with zero active jobs."""
    seen_path = root / "state" / "seen.json"
    reported_path = root / "state" / "reported_runs.json"
    seen = read_json(seen_path, {"config_version": targeting.POLICY_VERSION, "jobs": {}})
    jobs = seen.setdefault("jobs", {})
    reported = read_json(reported_path, {"reported_jobs": {}})
    reported_jobs = set((reported.get("reported_jobs") or {}).keys())
    active_ids = {
        str(job_id)
        for entry in pending.get("runs") or []
        for part in entry.get("delivery_parts") or []
        for job_id in part.get("job_ids") or []
        if str(job_id)
    }
    now = datetime.now(timezone.utc)
    counts = {
        "captured": len(decisions),
        "queued": 0,
        "acknowledged": 0,
        "deferred": 0,
        "anomaly": 0,
        "missing_seen_record": 0,
    }

    for job_id, decision in decisions.items():
        record = jobs.get(job_id)
        if not isinstance(record, dict):
            counts["missing_seen_record"] += 1
            continue

        record["targeting_policy_version"] = targeting.POLICY_VERSION
        record["targeting_updated_at_utc"] = targeting._iso_utc(now)
        record["targeting_reason"] = str(decision.get("reason") or "")
        record.pop("retry_after_utc", None)
        record.pop("deferred_reason", None)

        if decision["kind"] == "acknowledged" or job_id in reported_jobs:
            record["status"] = "reviewed_acknowledged"
            counts["acknowledged"] += 1
        elif job_id in active_ids:
            record["status"] = "queued_for_gpt"
            record["delivery_tier"] = decision.get("tier")
            counts["queued"] += 1
        elif decision["kind"] == "deferred":
            reason = str(decision.get("reason") or "outside_target_geography_or_unconfirmed_eligibility")
            record["status"] = "deferred_targeting_retryable"
            record["deferred_reason"] = reason
            record["retry_after_utc"] = targeting._iso_utc(
                now + targeting._retry_delay(reason)
            )
            counts["deferred"] += 1
        else:
            record["status"] = "targeting_queue_anomaly"
            record["deferred_reason"] = "selected_candidate_missing_from_staged_queue"
            counts["anomaly"] += 1

    seen["config_version"] = targeting.POLICY_VERSION
    seen["targeting_policy_version"] = targeting.POLICY_VERSION
    if seen_path.is_file() or decisions:
        atomic_write_json(seen_path, seen)
    return counts


def reprioritize_with_seen_decisions(
    config: dict[str, Any],
    *,
    pending_path: Path = targeting.legacy.PENDING,
    reported_path: Path = targeting.legacy.REPORTED,
    summary_path: Path = targeting.legacy.DEFERRED_SUMMARY,
) -> dict[str, Any]:
    """Run policy-15 targeting while preserving every per-job decision."""
    global _LAST_SEEN_DECISION_AUDIT
    pending_path = Path(pending_path)
    reported_path = Path(reported_path)
    summary_path = Path(summary_path)
    root = pending_path.parent.parent
    decisions = _capture_targeting_decisions(root, pending_path, reported_path)
    result = targeting.reprioritize_pending_queue(
        config,
        pending_path=pending_path,
        reported_path=reported_path,
        summary_path=summary_path,
    )
    audit = _apply_targeting_decisions(root, decisions, result.get("pending", {}))
    result["seen_decisions"] = audit
    _LAST_SEEN_DECISION_AUDIT = audit
    return result


base_pipeline.POLICY_VERSION = targeting.POLICY_VERSION
base_pipeline.reprioritize_pending_queue = reprioritize_with_seen_decisions


def _queue_health(latest: dict) -> str:
    integrity = ((latest.get("delivery_queue") or {}).get("integrity") or {}).get("status")
    warning = bool((latest.get("delivery_queue") or {}).get("warning"))
    if integrity != "validated":
        return "degraded_integrity"
    if warning:
        return "healthy_with_backlog_warning"
    return "healthy"


def _targeting_config(root: Path) -> dict:
    config = read_json(root / "queries_v13.json")
    config["config_version"] = targeting.POLICY_VERSION
    config["delivery_default_chunk_size"] = max(
        8,
        int(config.get("delivery_default_chunk_size", 4)),
    )
    config["delivery_excerpt_chars"] = min(
        500,
        int(config.get("delivery_excerpt_chars", 500)),
    )
    return config


def retarget_existing_queue_before_scan(root: Path = ROOT) -> dict:
    """Retarget legacy lossless runs before v13 applies old acknowledgements.

    The v13 search pipeline reconciles reported packets before v14 targeting. If
    a legacy run was already acknowledged, that ordering can delete its lossless
    archive before policy 15 has a chance to reconsider candidates that were
    previously deferred. This explicit pre-pass stages the v15 survivors first;
    their new content-addressed part IDs are not consumed by the old receipts.
    """
    pending_path = root / "output" / "pending_runs.json"
    if not pending_path.is_file():
        return {
            "targeting_policy_version": targeting.POLICY_VERSION,
            "runs_considered": 0,
            "selected_candidates": 0,
            "deferred_candidates": 0,
            "pending_candidate_count": 0,
        }

    result = reprioritize_with_seen_decisions(
        _targeting_config(root),
        pending_path=pending_path,
        reported_path=root / "state" / "reported_runs.json",
        summary_path=root / "output" / "deferred_summary.json",
    )
    totals = result.get("summary", {}).get("totals", {})
    backlog = result.get("pending", {}).get("backlog", {})
    return {
        "targeting_policy_version": targeting.POLICY_VERSION,
        "runs_considered": len(result.get("summary", {}).get("runs", [])),
        **totals,
        "pending_candidate_count": int(backlog.get("pending_candidate_count", 0)),
        "pending_part_count": int(backlog.get("pending_part_count", 0)),
        "integrity_status": (result.get("pending", {}).get("integrity") or {}).get("status"),
        "seen_decisions": result.get("seen_decisions", {}),
    }


def run_pipeline(cli_path: Path) -> dict:
    receipt_promotion = promote_manual_receipts(ROOT)
    lifecycle_before = targeting.prepare_seen_for_run(ROOT)
    pre_scan_retarget = retarget_existing_queue_before_scan(ROOT)
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
        "targeting": (
            "healthy"
            if not lifecycle_after.get("missing_seen_record")
            and not _LAST_SEEN_DECISION_AUDIT.get("anomaly")
            else "healthy_with_state_warning"
        ),
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
    latest["pre_scan_retarget"] = pre_scan_retarget
    latest["seen_lifecycle"] = {
        "before_scan": lifecycle_before,
        "decision_capture": _LAST_SEEN_DECISION_AUDIT,
        "after_targeting": lifecycle_after,
    }
    latest.setdefault("candidate_payload", {})["note"] = (
        "Egypt IT candidates are protected. Remote candidates require job-level "
        "Egypt/MENA/EMEA/Africa/global scope; on-site and hybrid roles are kept "
        "separate as relocation leads. Legacy lossless candidates are retargeted "
        "before old packet acknowledgements can remove their archive. Deferred "
        "decisions are persisted even when a run has zero active candidates, and "
        "reviewed packet receipts are promoted to durable per-job receipts."
    )
    atomic_write_json(LATEST, latest)

    return {
        **result,
        "targeting_policy_version": targeting.POLICY_VERSION,
        "receipt_promotion": receipt_promotion,
        "pre_scan_retarget": pre_scan_retarget,
        "seen_lifecycle_before_scan": lifecycle_before,
        "seen_decision_capture": _LAST_SEEN_DECISION_AUDIT,
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
