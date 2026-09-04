#!/usr/bin/env python3
"""Targeting policy 15: precise remote scope and retryable deferrals.

This layer deliberately leaves broad LinkedIn discovery unchanged. It replaces
only the final targeting decision so an on-site role cannot masquerade as
remote merely because it was discovered by a regional remote query. It also
keeps uncertain deferrals retryable and allows already-reported v14 runs to be
retargeted without repeating candidates whose content-addressed parts were
already acknowledged.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import radar_pipeline_v13 as pipeline_v13
import targeted_queue_v14 as legacy
from queue_integrity import atomic_write_json, read_json

ROOT = Path(__file__).resolve().parent
POLICY_VERSION = 15

_LEGACY_REMOTE_ANNOTATION = pipeline_v13.remote_eligibility_annotation

ELIGIBLE_SCOPES = {"global", "emea", "mena", "africa", "egypt"}
GENERIC_REMOTE_LOCATIONS = {
    "remote",
    "worldwide",
    "global",
    "globally remote",
    "remote worldwide",
    "remote - worldwide",
    "remote, worldwide",
    "emea",
    "remote emea",
    "remote - emea",
    "mena",
    "remote mena",
    "middle east",
    "africa",
}

MENA_PHYSICAL_LOCATION_MARKERS = tuple(
    marker
    for marker in legacy.MENA_LOCATION_MARKERS
    if marker not in {"mena", "middle east", "gcc", "africa"}
)

PREFERRED_RELOCATION_MARKERS = tuple(dict.fromkeys((
    *legacy.PREFERRED_RELOCATION_MARKERS,
    "france",
    "paris",
    "estonia",
    "latvia",
    "lithuania",
    "croatia",
    "slovenia",
    "cyprus",
)))

GLOBAL_SCOPE_PATTERNS = (
    r"\b(?:fully\s+|100%\s+)?remote\b.{0,100}\b(?:worldwide|globally|any country|anywhere in the world)\b",
    r"\b(?:worldwide|global(?:ly)?)\b.{0,100}\bremote\b",
    r"\bwork from anywhere in the world\b",
    r"\b(?:open|available) to (?:candidates|applicants|people).{0,80}\b(?:worldwide|globally|in any country)\b",
    r"\b(?:hire|hiring) (?:from|across) (?:any country|the world|worldwide|globally)\b",
    r"\blocation\s*[:\-]\s*remote\s*(?:\(|-|,)?\s*(?:worldwide|global)\b",
)

REGIONAL_SCOPE_PATTERNS = {
    "emea": (
        r"\bremote\b.{0,100}\bemea\b",
        r"\bemea\b.{0,100}\bremote\b",
        r"\b(?:based|located|reside|live|work) anywhere in (?:the )?emea\b",
        r"\b(?:candidates|applicants).{0,80}\b(?:based|located|residing|living) in (?:the )?emea\b",
    ),
    "mena": (
        r"\bremote\b.{0,100}\b(?:mena|middle east)\b",
        r"\b(?:mena|middle east)\b.{0,100}\bremote\b",
        r"\b(?:based|located|reside|live|work) anywhere in (?:the )?(?:mena|middle east)\b",
        r"\b(?:candidates|applicants).{0,80}\b(?:based|located|residing|living) in (?:the )?(?:mena|middle east)\b",
    ),
    "africa": (
        r"\bremote\b.{0,100}\bafrica\b",
        r"\bafrica\b.{0,100}\bremote\b",
        r"\b(?:based|located|reside|live|work) anywhere in africa\b",
        r"\b(?:candidates|applicants).{0,80}\b(?:based|located|residing|living) in africa\b",
    ),
    "egypt": (
        r"\bremote\b.{0,100}\begypt\b",
        r"\begypt\b.{0,100}\bremote\b",
        r"\b(?:based|located|reside|live|work) anywhere in egypt\b",
        r"\b(?:candidates|applicants).{0,80}\b(?:based|located|residing|living) in egypt\b",
    ),
}

ONSITE_PATTERNS = (
    r"\b(?:fully|100%)\s+on[- ]?site\b",
    r"\b(?:this\s+)?(?:role|position|job)\s+(?:is\s+)?(?:fully\s+)?on[- ]?site\b",
    r"\bon[- ]?site\s+(?:role|position|job)\b",
    r"\b(?:work arrangement|work model|working pattern|workplace|location)\s*[:\-]\s*(?:fully\s+|100%\s+)?on[- ]?site\b",
    r"\boffice[- ]based\b",
    r"\bwork from (?:the )?office\b",
)

HYBRID_PATTERNS = (
    r"\b(?:work arrangement|work model|working pattern|workplace)\s*[:\-]\s*hybrid\b",
    r"\bhybrid\b.{0,100}\b(?:office|on[- ]?site|days? per week|times? per week)\b",
    r"\b(?:office|on[- ]?site)\b.{0,100}\bhybrid\b",
)

REMOTE_MODEL_PATTERNS = (
    r"\b(?:fully|100%)\s+remote\b",
    r"\b(?:this\s+)?(?:role|position|job)\s+(?:is\s+)?(?:fully\s+)?remote\b",
    r"\bremote\s+(?:role|position|job|opportunity)\b",
    r"\b(?:work arrangement|work model|working pattern|workplace|location)\s*[:\-]\s*(?:fully\s+|100%\s+)?remote\b",
    r"\bwork remotely\b",
)

QUALIFIED_WORK_FROM_ANYWHERE = re.compile(
    r"\bwork from anywhere\s+(?:in|within|across|from)\s+(?:the\s+)?([^.;\n]{2,80})",
    flags=re.IGNORECASE,
)
BARE_WORK_FROM_ANYWHERE = re.compile(r"\bwork from anywhere\b", flags=re.IGNORECASE)


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def _candidate_text(candidate: dict[str, Any], limit: int = 7000) -> str:
    return " ".join([
        _norm(candidate.get("title")),
        _norm(candidate.get("location")),
        _norm(str(candidate.get("description") or "")[:limit]),
    ])


def _pattern_hits(text: str, patterns: tuple[str, ...]) -> list[str]:
    return [pattern for pattern in patterns if re.search(pattern, text, re.IGNORECASE | re.DOTALL)]


def _generic_remote_location(location: str) -> bool:
    normalized = _norm(location).strip(" -(),")
    if normalized in GENERIC_REMOTE_LOCATIONS:
        return True
    if normalized.startswith("remote") and any(
        marker in normalized
        for marker in ("worldwide", "global", "emea", "mena", "middle east", "africa")
    ):
        return True
    return False


def work_model(candidate: dict[str, Any]) -> tuple[str, list[str]]:
    """Return remote, onsite, hybrid, or unknown using job-level wording."""
    title = _norm(candidate.get("title"))
    location = _norm(candidate.get("location"))
    description_head = _norm(str(candidate.get("description") or "")[:3000])
    head = f"{title}. {location}. {description_head}"

    onsite_hits = _pattern_hits(head, ONSITE_PATTERNS)
    if re.search(r"\bon[- ]?site\b", title):
        onsite_hits.insert(0, "onsite title")
    if onsite_hits:
        return "onsite", list(dict.fromkeys(onsite_hits))

    hybrid_hits = _pattern_hits(head, HYBRID_PATTERNS)
    if re.search(r"\bhybrid\b", title):
        hybrid_hits.insert(0, "hybrid title")
    if hybrid_hits:
        return "hybrid", list(dict.fromkeys(hybrid_hits))

    remote_hits = _pattern_hits(head, REMOTE_MODEL_PATTERNS)
    if re.search(r"\bremote\b", title):
        remote_hits.insert(0, "remote title")
    if _generic_remote_location(location):
        remote_hits.insert(0, "generic remote location")
    if remote_hits:
        return "remote", list(dict.fromkeys(remote_hits))

    return "unknown", []


def remote_scope(candidate: dict[str, Any]) -> tuple[str | None, list[str]]:
    """Return an Egypt-compatible remote scope, country_limited, or unknown.

    A bare 'work from anywhere' statement is not global proof when LinkedIn's
    location is a specific country. This is the key guard against India-only,
    Georgia-only, US-only, and similar false worldwide positives.
    """
    text = _candidate_text(candidate, 6000)
    location = _norm(candidate.get("location"))

    qualified = QUALIFIED_WORK_FROM_ANYWHERE.search(text)
    if qualified:
        qualifier = _norm(qualified.group(1))
        if any(marker in qualifier for marker in ("world", "global", "any country")):
            return "global", [f"work from anywhere in {qualifier}"]
        if "emea" in qualifier or "europe middle east and africa" in qualifier:
            return "emea", [f"work from anywhere in {qualifier}"]
        if "mena" in qualifier or "middle east" in qualifier:
            return "mena", [f"work from anywhere in {qualifier}"]
        if "africa" in qualifier:
            return "africa", [f"work from anywhere in {qualifier}"]
        if "egypt" in qualifier:
            return "egypt", [f"work from anywhere in {qualifier}"]
        return "country_limited", [f"work from anywhere in {qualifier}"]

    global_hits = _pattern_hits(text, GLOBAL_SCOPE_PATTERNS)
    if global_hits:
        return "global", global_hits

    for scope in ("egypt", "mena", "africa", "emea"):
        hits = _pattern_hits(text, REGIONAL_SCOPE_PATTERNS[scope])
        if hits:
            return scope, hits

    if BARE_WORK_FROM_ANYWHERE.search(text):
        if _generic_remote_location(location):
            return "global", ["bare work from anywhere with generic remote location"]
        return None, ["bare work from anywhere with country-specific location"]

    return None, []


def remote_eligibility_annotation(candidate: dict[str, Any]) -> dict[str, Any]:
    base = _LEGACY_REMOTE_ANNOTATION(candidate)
    model, model_signals = work_model(candidate)
    scope, scope_signals = remote_scope(candidate)

    restrictions = list(base.get("restriction_signals") or [])
    if scope == "country_limited":
        restrictions = list(dict.fromkeys([*restrictions, *scope_signals]))

    if restrictions:
        status = "explicit_location_or_work_authorization_restriction"
        confidence = "high"
    elif model in {"onsite", "hybrid"}:
        status = "explicit_non_remote"
        confidence = "high"
    elif scope in ELIGIBLE_SCOPES and model == "remote":
        status = "explicit_egypt_emea_or_global_signal"
        confidence = "high"
    else:
        status = "requires_full_review"
        confidence = "low"

    return {
        "status": status,
        "eligible_signals": scope_signals if scope in ELIGIBLE_SCOPES else [],
        "restriction_signals": restrictions,
        "non_remote_signals": model_signals if model in {"onsite", "hybrid"} else [],
        "confidence": confidence,
        "scope": scope,
        "work_model": model,
        "note": (
            "Remote eligibility requires job-level geographic scope. Europe/CET "
            "mentions and country-specific LinkedIn locations are not proof that "
            "an applicant based in Egypt is eligible."
        ),
    }


def regional_evidence(candidate: dict[str, Any]) -> dict[str, Any]:
    lane = _norm(candidate.get("discovery_lane"))
    scope, signals = remote_scope(candidate)
    eligible = False
    if lane in {"remote_mena", "remote_middle_east"}:
        eligible = scope in {"global", "emea", "mena", "africa", "egypt"}
    elif lane == "remote_emea":
        eligible = scope in {"global", "emea", "africa", "egypt"}
    elif lane == "remote_egypt":
        eligible = scope in {"global", "emea", "mena", "africa", "egypt"}
    return {
        "lane": lane,
        "signals": signals if eligible else [],
        "confidence": "high" if eligible else "low",
        "scope": scope,
    }


def classify_candidate(candidate: dict[str, Any]) -> tuple[str | None, str]:
    """Return (delivery tier, reason); None is an audited, retryable deferral."""
    if not candidate.get("advisory_it_evidence"):
        return None, "no_it_evidence"

    if pipeline_v13.is_egypt_candidate(candidate):
        return "local", "all_egypt_it_candidates_are_protected"

    evidence = legacy._core_evidence(candidate)
    if not evidence["moderate"]:
        return None, "weak_core_infrastructure_evidence"

    lane = _norm(candidate.get("discovery_lane"))
    location = legacy._normalized_location(candidate)
    annotation = remote_eligibility_annotation(candidate)
    status = annotation["status"]
    scope = annotation.get("scope")
    model = annotation.get("work_model")
    target_mena_location = legacy._contains_marker(location, MENA_PHYSICAL_LOCATION_MARKERS)
    preferred_relocation_location = legacy._contains_marker(location, PREFERRED_RELOCATION_MARKERS)
    explicit_relocation = legacy._explicit_relocation(candidate)

    if status == "explicit_location_or_work_authorization_restriction":
        return None, "explicit_location_work_authorization_or_clearance_block"

    if target_mena_location:
        if model == "remote" and scope in ELIGIBLE_SCOPES:
            return "remote", "mena_location_with_explicit_egypt_compatible_remote_scope"
        if model in {"onsite", "hybrid"}:
            return "relocation", "mena_onsite_or_hybrid_relocation_lead"
        return "relocation", "mena_work_model_unconfirmed_review_as_relocation"

    if lane in legacy.REGIONAL_LANES:
        if model == "remote" and scope in ELIGIBLE_SCOPES:
            return "remote", "regional_remote_with_explicit_egypt_compatible_scope"
        if model in {"onsite", "hybrid"}:
            if explicit_relocation or (preferred_relocation_location and evidence["strong"]):
                return "relocation", "strong_regional_onsite_relocation_lead"
            return None, "regional_result_is_nonremote_without_relocation_case"
        if preferred_relocation_location and evidence["strong"]:
            return "relocation", "strong_regional_role_review_as_relocation"
        return None, "regional_query_without_job_level_remote_scope"

    if model == "remote" and scope in ELIGIBLE_SCOPES:
        return "remote", "explicit_remote_eligibility_for_egypt_or_broader_region"

    if model == "remote" and scope == "country_limited":
        return None, "country_limited_remote_scope"

    if model == "remote" and lane == "remote_worldwide":
        if _generic_remote_location(location) and evidence["strong"]:
            return "remote", "strong_generic_remote_listing_requires_gpt_confirmation"
        return None, "country_specific_or_ambiguous_global_remote_eligibility"

    if model in {"onsite", "hybrid"}:
        if explicit_relocation:
            return "relocation", "explicit_visa_or_relocation_support"
        if preferred_relocation_location and evidence["strong"]:
            return "relocation", "exceptionally_strong_emea_relocation_prospect"
        return None, "nonremote_without_credible_relocation_path"

    if explicit_relocation:
        return "relocation", "explicit_visa_or_relocation_support"

    if preferred_relocation_location and evidence["strong"]:
        return "relocation", "exceptionally_strong_emea_relocation_prospect"

    if lane == "remote_worldwide":
        return None, "country_specific_or_ambiguous_global_remote_eligibility"

    return None, "outside_target_geography_or_unconfirmed_eligibility"


def install_v15_patches() -> None:
    """Install v15 policy into the v13/v14 machinery without duplicating it."""
    legacy.POLICY_VERSION = POLICY_VERSION
    legacy.classify_candidate = classify_candidate
    pipeline_v13.remote_eligibility_annotation = remote_eligibility_annotation
    pipeline_v13.regional_evidence = regional_evidence


def reprioritize_pending_queue(*args: Any, **kwargs: Any) -> dict[str, Any]:
    install_v15_patches()
    return legacy.reprioritize_pending_queue(*args, **kwargs)


def _parse_utc(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _retry_delay(reason: str) -> timedelta:
    if reason in {
        "country_specific_or_ambiguous_global_remote_eligibility",
        "regional_query_without_job_level_remote_scope",
        "outside_target_geography_or_unconfirmed_eligibility",
    }:
        return timedelta(minutes=55)
    if reason in {"weak_core_infrastructure_evidence", "no_it_evidence"}:
        return timedelta(hours=12)
    if reason in {
        "explicit_location_work_authorization_or_clearance_block",
        "country_limited_remote_scope",
    }:
        return timedelta(days=7)
    return timedelta(hours=3)


def prepare_seen_for_run(root: Path = ROOT) -> dict[str, Any]:
    """Release expired deferrals and reopen legacy reported runs for retargeting."""
    seen_path = root / "state" / "seen.json"
    pending_path = root / "output" / "pending_runs.json"
    reported_path = root / "state" / "reported_runs.json"
    now = datetime.now(timezone.utc)

    seen = read_json(seen_path, {"config_version": POLICY_VERSION, "jobs": {}})
    jobs = seen.setdefault("jobs", {})
    released_jobs: list[str] = []
    for job_id, record in list(jobs.items()):
        status = str(record.get("status") or "")
        if not status.startswith("deferred_targeting"):
            continue
        policy = int(record.get("targeting_policy_version", 0) or 0)
        retry_after = _parse_utc(record.get("retry_after_utc"))
        if policy != POLICY_VERSION or (retry_after is not None and retry_after <= now):
            jobs.pop(job_id, None)
            released_jobs.append(str(job_id))

    seen["config_version"] = POLICY_VERSION
    if released_jobs or seen_path.is_file():
        atomic_write_json(seen_path, seen)

    pending = read_json(pending_path, {"runs": []})
    reported = read_json(
        reported_path,
        {"schema_version": 3, "reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_runs = reported.setdefault("reported_runs", {})
    retargeted_runs: list[str] = []
    for entry in pending.get("runs") or []:
        run_id = str(entry.get("run_id") or "")
        old_policy = int(entry.get("targeting_policy_version", 0) or 0)
        if run_id and old_policy < POLICY_VERSION and run_id in reported_runs and entry.get("parts"):
            reported_runs.pop(run_id, None)
            retargeted_runs.append(run_id)

    if retargeted_runs:
        reported["updated_at_utc"] = _iso_utc(now)
        atomic_write_json(reported_path, reported)

    return {
        "released_deferred_job_count": len(released_jobs),
        "released_deferred_job_ids": released_jobs,
        "retargeted_legacy_run_count": len(retargeted_runs),
        "retargeted_legacy_run_ids": retargeted_runs,
    }


def annotate_seen_from_pending(root: Path = ROOT) -> dict[str, int]:
    """Record queued, acknowledged, and retryable-deferred outcomes per Job ID."""
    seen_path = root / "state" / "seen.json"
    pending_path = root / "output" / "pending_runs.json"
    reported_path = root / "state" / "reported_runs.json"

    seen = read_json(seen_path, {"config_version": POLICY_VERSION, "jobs": {}})
    jobs = seen.setdefault("jobs", {})
    pending = read_json(pending_path, {"runs": []})
    reported = read_json(
        reported_path,
        {"schema_version": 3, "reported_runs": {}, "reported_parts": {}, "reported_jobs": {}},
    )
    reported_parts = set((reported.get("reported_parts") or {}).keys())
    reported_jobs = set((reported.get("reported_jobs") or {}).keys())
    now = datetime.now(timezone.utc)

    counts = {"queued": 0, "acknowledged": 0, "deferred": 0, "missing_seen_record": 0}
    for entry in pending.get("runs") or []:
        if int(entry.get("targeting_policy_version", 0) or 0) != POLICY_VERSION:
            continue
        candidates, _ = legacy._load_lossless_candidates(entry, root)
        active_ids = {
            str(job_id)
            for part in entry.get("delivery_parts") or []
            for job_id in part.get("job_ids") or []
            if str(job_id)
        }
        acknowledged_ids = legacy._acknowledged_job_ids(
            entry,
            root,
            reported_parts,
            reported_jobs,
        )

        for candidate in candidates:
            job_id = str(candidate.get("linkedin_job_id") or "").strip()
            if not job_id:
                continue
            record = jobs.get(job_id)
            if record is None:
                counts["missing_seen_record"] += 1
                continue

            record["targeting_policy_version"] = POLICY_VERSION
            record["targeting_updated_at_utc"] = _iso_utc(now)
            record.pop("retry_after_utc", None)
            record.pop("deferred_reason", None)

            if job_id in acknowledged_ids:
                record["status"] = "reviewed_acknowledged"
                counts["acknowledged"] += 1
            elif job_id in active_ids:
                record["status"] = "queued_for_gpt"
                counts["queued"] += 1
            else:
                tier, reason = classify_candidate(candidate)
                if tier is None:
                    record["status"] = "deferred_targeting_retryable"
                    record["deferred_reason"] = reason
                    record["retry_after_utc"] = _iso_utc(now + _retry_delay(reason))
                    counts["deferred"] += 1
                else:
                    record["status"] = "targeting_queue_anomaly"
                    record["deferred_reason"] = "classified_for_delivery_but_missing_from_queue"

    seen["config_version"] = POLICY_VERSION
    seen["targeting_policy_version"] = POLICY_VERSION
    atomic_write_json(seen_path, seen)
    return counts
