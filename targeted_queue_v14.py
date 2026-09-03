#!/usr/bin/env python3
"""Target the GPT review queue without narrowing raw LinkedIn discovery.

Policy 14 preserves every Egypt IT candidate, keeps credible regional remote
and relocation leads, and defers geographically blocked or weakly evidenced
international noise. Deferred candidates remain counted in an audit summary;
their Job IDs remain in the seen ledger so they cannot churn back every hour.
"""
from __future__ import annotations

import argparse
import copy
import json
import re
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import radar_pipeline_v13 as pipeline
from queue_integrity import (
    atomic_write_json,
    read_json,
    reconcile_pending_queue,
    resolve_reference,
    validate_part_payload,
)

ROOT = Path(__file__).resolve().parent
PENDING = ROOT / "output" / "pending_runs.json"
REPORTED = ROOT / "state" / "reported_runs.json"
DEFERRED_SUMMARY = ROOT / "output" / "deferred_summary.json"
POLICY_VERSION = 14

CORE_ROLE_LABELS = {
    "IT Infrastructure",
    "Systems Administration",
    "Systems Engineering",
    "Network Engineering",
    "Network Security",
    "IT Operations",
    "IT Administration",
    "Virtualization",
    "Technical Support",
    "Endpoint",
    "Cloud Infrastructure",
    "NOC",
    "IT/ICT General",
}
CORE_SKILL_LABELS = {
    "Windows Server",
    "Active Directory",
    "DNS/DHCP",
    "Group Policy",
    "VMware",
    "Hyper-V",
    "Fortinet",
    "Cisco",
    "Routing/Switching",
    "VPN",
    "Microsoft 365",
    "Azure",
    "Linux",
    "Veeam",
    "Backup/DR",
    "Infrastructure Security",
    "Google Workspace",
}
REGIONAL_LANES = {"remote_mena", "remote_middle_east", "remote_emea"}

MENA_LOCATION_MARKERS = (
    "egypt", "cairo", "giza", "new cairo", "alexandria, egypt",
    "saudi arabia", "riyadh", "jeddah", "dammam", "khobar", "thuwal", "makkah",
    "united arab emirates", "uae", "dubai", "abu dhabi", "sharjah", "ajman",
    "qatar", "doha", "kuwait", "bahrain", "oman", "muscat", "jordan", "amman",
    "lebanon", "beirut", "morocco", "casablanca", "rabat", "tunisia", "tunis",
    "algeria", "algiers",
)

PREFERRED_RELOCATION_MARKERS = (
    "portugal", "lisbon", "porto", "malta", "poland", "spain", "italy", "germany",
    "netherlands", "ireland", "united kingdom", "england", "scotland", "wales",
    "switzerland", "austria", "belgium", "luxembourg", "romania", "hungary",
    "czech", "slovakia", "sweden", "norway", "denmark", "finland",
)

GENERIC_REMOTE_LOCATIONS = {
    "remote",
    "worldwide",
    "global",
    "emea",
    "mena",
    "middle east",
    "africa",
}

RELOCATION_PATTERNS = (
    "visa sponsorship",
    "sponsorship available",
    "sponsor a work visa",
    "work visa support",
    "work permit support",
    "relocation assistance",
    "relocation support",
    "relocation package",
    "support with relocation",
    "global mobility",
    "international applicants",
    "overseas applicants",
    "open to applicants worldwide",
)


def _labels(candidate: dict[str, Any], key: str) -> set[str]:
    labels: set[str] = set()
    for value in candidate.get(key) or []:
        label = value.get("label") if isinstance(value, dict) else value
        if label:
            labels.add(str(label))
    return labels


def _core_evidence(candidate: dict[str, Any]) -> dict[str, Any]:
    title_roles = _labels(candidate, "role_hits_title") & CORE_ROLE_LABELS
    body_roles = _labels(candidate, "role_hits_description") & CORE_ROLE_LABELS
    skills = _labels(candidate, "skill_hits") & CORE_SKILL_LABELS
    moderate = bool(title_roles) or len(skills) >= 2 or (bool(body_roles) and bool(skills))
    strong = (
        (bool(title_roles) and len(skills) >= 2)
        or len(skills) >= 4
        or (bool(title_roles) and bool(body_roles))
    )
    return {
        "title_roles": sorted(title_roles),
        "body_roles": sorted(body_roles),
        "skills": sorted(skills),
        "moderate": moderate,
        "strong": strong,
    }


def _normalized_location(candidate: dict[str, Any]) -> str:
    return re.sub(r"\s+", " ", str(candidate.get("location") or "")).strip().casefold()


def _contains_marker(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def _explicit_relocation(candidate: dict[str, Any]) -> bool:
    text = " ".join([
        str(candidate.get("location") or ""),
        str(candidate.get("description") or "")[:10000],
    ]).casefold()
    return any(pattern in text for pattern in RELOCATION_PATTERNS)


def classify_candidate(candidate: dict[str, Any]) -> tuple[str | None, str]:
    """Return (delivery tier, reason). None means audited deferral, not deletion."""
    if not candidate.get("advisory_it_evidence"):
        return None, "no_it_evidence"

    if pipeline.is_egypt_candidate(candidate):
        return "local", "all_egypt_it_candidates_are_protected"

    evidence = _core_evidence(candidate)
    if not evidence["moderate"]:
        return None, "weak_core_infrastructure_evidence"

    lane = str(candidate.get("discovery_lane") or "").casefold()
    location = _normalized_location(candidate)
    annotation = pipeline.remote_eligibility_annotation(candidate)
    regional_signals = list(pipeline.regional_evidence(candidate).get("signals") or [])
    status = annotation.get("status")
    target_mena_location = _contains_marker(location, MENA_LOCATION_MARKERS)
    preferred_relocation_location = _contains_marker(location, PREFERRED_RELOCATION_MARKERS)
    explicit_relocation = _explicit_relocation(candidate)

    if status == "explicit_location_or_work_authorization_restriction":
        return None, "explicit_location_work_authorization_or_clearance_block"

    if target_mena_location:
        if status == "explicit_non_remote":
            return "relocation", "credible_mena_onsite_relocation_lead"
        return "remote", "credible_mena_remote_or_flexible_lead"

    if lane in REGIONAL_LANES:
        if not regional_signals:
            return None, "regional_query_without_job_level_regional_evidence"
        if status == "explicit_non_remote":
            if explicit_relocation or (preferred_relocation_location and evidence["strong"]):
                return "relocation", "strong_regional_onsite_relocation_lead"
            return None, "regional_result_is_nonremote_without_relocation_case"
        return "remote", "regional_remote_with_job_level_region_evidence"

    if status == "explicit_egypt_emea_or_global_signal":
        return "remote", "explicit_remote_eligibility_for_egypt_or_broader_region"

    if status == "explicit_non_remote":
        if explicit_relocation:
            return "relocation", "explicit_visa_or_relocation_support"
        if preferred_relocation_location and evidence["strong"]:
            return "relocation", "exceptionally_strong_emea_relocation_prospect"
        return None, "nonremote_without_credible_relocation_path"

    if explicit_relocation:
        return "relocation", "explicit_visa_or_relocation_support"

    if lane == "remote_worldwide":
        if location in GENERIC_REMOTE_LOCATIONS and evidence["strong"]:
            return "remote", "strong_global_remote_listing_eligibility_needs_gpt_check"
        return None, "country_specific_or_ambiguous_global_remote_eligibility"

    if preferred_relocation_location and evidence["strong"]:
        return "relocation", "exceptionally_strong_emea_relocation_prospect"

    return None, "outside_target_geography_or_unconfirmed_eligibility"


def _candidate_score(candidate: dict[str, Any]) -> float:
    try:
        return float(candidate.get("score") or 0)
    except (TypeError, ValueError):
        return 0.0


def _load_lossless_candidates(
    entry: dict[str, Any],
    root: Path,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    candidates: list[dict[str, Any]] = []
    source_by_job_id: dict[str, str] = {}
    seen: set[str] = set()
    for reference in entry.get("parts") or []:
        path = resolve_reference(root, reference, "output/runs")
        if not path.is_file():
            continue
        for candidate in read_json(path).get("review_candidates") or []:
            job_id = str(candidate.get("linkedin_job_id") or "").strip()
            if not job_id or job_id in seen:
                continue
            seen.add(job_id)
            candidates.append(candidate)
            source_by_job_id[job_id] = reference
    return candidates, source_by_job_id


def _acknowledged_job_ids(
    entry: dict[str, Any],
    root: Path,
    reported_parts: set[str],
    reported_jobs: set[str],
) -> set[str]:
    acknowledged = set(reported_jobs)
    for part in entry.get("delivery_parts") or []:
        if str(part.get("part_id") or "") not in reported_parts:
            continue
        job_ids = [str(value) for value in part.get("job_ids") or [] if str(value)]
        if not job_ids:
            path = resolve_reference(root, part.get("path", ""), "output/delivery")
            if path.is_file():
                payload = read_json(path)
                job_ids = [
                    str(item.get("linkedin_job_id") or "")
                    for item in payload.get("review_candidates") or []
                    if str(item.get("linkedin_job_id") or "")
                ]
        acknowledged.update(job_ids)
    return acknowledged


def _delete_references(
    references: list[str],
    root: Path,
    default_directory: str,
) -> None:
    for reference in references:
        resolve_reference(root, reference, default_directory).unlink(missing_ok=True)


def _stage_delivery_parts(
    entry: dict[str, Any],
    selected: list[dict[str, Any]],
    source_by_job_id: dict[str, str],
    config: dict[str, Any],
    root: Path,
) -> list[dict[str, Any]]:
    if not selected:
        return []

    pipeline.install_patches(config)
    fallback = (entry.get("parts") or [""])[0]
    delivery_dir = root / "output" / "delivery"
    with tempfile.TemporaryDirectory(prefix="radar-target-v14-") as tmp:
        temp_dir = Path(tmp)
        staged = pipeline.write_delivery_parts(
            str(entry.get("run_id") or ""),
            str(entry.get("generated_at_utc") or ""),
            str(entry.get("health") or "healthy"),
            list(entry.get("warnings") or []),
            selected,
            source_by_job_id,
            fallback,
            int(config.get("run_archive_chunk_size", 25)),
            temp_dir,
            int(config.get("delivery_excerpt_chars", 500)),
        )

        validated: list[tuple[dict[str, Any], Path]] = []
        for part in staged:
            source = temp_dir / Path(part["path"]).name
            payload = read_json(source)
            validation = validate_part_payload(payload)
            if validation["candidate_count"] != int(part["candidate_count"]):
                raise RuntimeError("staged targeted packet count mismatch")
            validated.append((part, source))

        delivery_dir.mkdir(parents=True, exist_ok=True)
        result: list[dict[str, Any]] = []
        for part, source in validated:
            target = delivery_dir / source.name
            source.replace(target)
            part = dict(part)
            part["path"] = str(Path("output") / "delivery" / target.name)
            result.append(part)
        return result


def reprioritize_pending_queue(
    config: dict[str, Any],
    *,
    pending_path: Path = PENDING,
    reported_path: Path = REPORTED,
    summary_path: Path = DEFERRED_SUMMARY,
) -> dict[str, Any]:
    root = pending_path.parent.parent
    pending = read_json(pending_path, {"schema_version": 3, "runs": []})
    reported = read_json(
        reported_path,
        {"schema_version": 3, "reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_runs = set((reported.get("reported_runs") or {}).keys())
    reported_parts = set((reported.get("reported_parts") or {}).keys())
    reported_jobs = set((reported.get("reported_jobs") or {}).keys())

    kept_runs: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    for original in pending.get("runs") or []:
        entry = copy.deepcopy(original)
        run_id = str(entry.get("run_id") or "")
        if run_id in reported_runs:
            _delete_references(
                [part.get("path", "") for part in entry.get("delivery_parts") or []],
                root,
                "output/delivery",
            )
            _delete_references(list(entry.get("parts") or []), root, "output/runs")
            continue

        if int(entry.get("targeting_policy_version", 0) or 0) == POLICY_VERSION:
            kept_runs.append(entry)
            existing = entry.get("targeting_audit") or {}
            summaries.append({"run_id": run_id, **existing})
            continue

        candidates, source_by_job_id = _load_lossless_candidates(entry, root)
        acknowledged = _acknowledged_job_ids(
            entry,
            root,
            reported_parts,
            reported_jobs,
        )
        selected: list[dict[str, Any]] = []
        deferred = Counter()
        excluded_acknowledged = 0

        for candidate in candidates:
            job_id = str(candidate.get("linkedin_job_id") or "")
            if job_id in acknowledged:
                excluded_acknowledged += 1
                continue
            tier, reason = classify_candidate(candidate)
            if tier is None:
                deferred[reason] += 1
                continue
            item = copy.deepcopy(candidate)
            item["delivery_tier"] = tier
            item["delivery_reason"] = reason
            selected.append(item)

        tier_order = {"local": 0, "remote": 1, "relocation": 2}
        selected.sort(key=lambda item: (
            tier_order.get(str(item.get("delivery_tier") or ""), 9),
            -_candidate_score(item),
            str(item.get("title") or ""),
        ))

        old_delivery_paths = [
            str(part.get("path") or "") for part in entry.get("delivery_parts") or []
        ]
        new_parts = _stage_delivery_parts(
            entry,
            selected,
            source_by_job_id,
            config,
            root,
        )
        new_paths = {str(part.get("path") or "") for part in new_parts}
        _delete_references(
            [path for path in old_delivery_paths if path not in new_paths],
            root,
            "output/delivery",
        )

        audit = {
            "input_lossless_candidates": len(candidates),
            "acknowledged_candidates_excluded": excluded_acknowledged,
            "selected_candidates": len(selected),
            "deferred_candidates": sum(deferred.values()),
            "deferred_reasons": dict(sorted(deferred.items())),
            "protected_egypt_candidates": sum(
                1 for item in selected if item.get("delivery_tier") == "local"
            ),
            "remote_candidates": sum(
                1 for item in selected if item.get("delivery_tier") == "remote"
            ),
            "relocation_candidates": sum(
                1 for item in selected if item.get("delivery_tier") == "relocation"
            ),
        }
        summaries.append({"run_id": run_id, **audit})

        if not new_parts:
            _delete_references(list(entry.get("parts") or []), root, "output/runs")
            continue

        entry["delivery_parts"] = new_parts
        entry["candidate_count"] = len(selected)
        entry["delivery_policy_version"] = POLICY_VERSION
        entry["targeting_policy_version"] = POLICY_VERSION
        entry["targeting_audit"] = audit
        entry["integrity"] = {
            "status": "staged_for_manifest_validation",
            "delivery_part_count": len(new_parts),
            "candidate_count": len(selected),
        }
        kept_runs.append(entry)

    pending["runs"] = kept_runs
    pending["targeting_policy"] = {
        "version": POLICY_VERSION,
        "principle": (
            "Protect every Egypt IT candidate; require job-level regional or "
            "eligibility evidence internationally; preserve deferral counts."
        ),
    }
    atomic_write_json(pending_path, pending)
    pending = reconcile_pending_queue(
        root,
        pending_path,
        reported_path,
        backlog_warning_candidates=int(config.get("delivery_backlog_warning_candidates", 500)),
    )

    summary = {
        "schema_version": 1,
        "targeting_policy_version": POLICY_VERSION,
        "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "runs": summaries,
        "totals": {
            "input_lossless_candidates": sum(item.get("input_lossless_candidates", 0) for item in summaries),
            "acknowledged_candidates_excluded": sum(item.get("acknowledged_candidates_excluded", 0) for item in summaries),
            "selected_candidates": sum(item.get("selected_candidates", 0) for item in summaries),
            "deferred_candidates": sum(item.get("deferred_candidates", 0) for item in summaries),
            "protected_egypt_candidates": sum(item.get("protected_egypt_candidates", 0) for item in summaries),
            "remote_candidates": sum(item.get("remote_candidates", 0) for item in summaries),
            "relocation_candidates": sum(item.get("relocation_candidates", 0) for item in summaries),
        },
        "pending_backlog_after_targeting": pending.get("backlog", {}),
        "note": (
            "Deferred jobs are not claimed to be irrelevant. They lacked the "
            "geographic eligibility or core evidence needed for Ahmed's active queue."
        ),
    }
    atomic_write_json(summary_path, summary)
    return {"pending": pending, "summary": summary}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=ROOT / "output" / ".pipeline_v14" / "queries.json",
    )
    args = parser.parse_args()
    config = read_json(args.config)
    result = reprioritize_pending_queue(config)
    print(json.dumps({
        "targeting_policy_version": POLICY_VERSION,
        **result["summary"]["totals"],
        **result["pending"].get("backlog", {}),
    }))


if __name__ == "__main__":
    main()
