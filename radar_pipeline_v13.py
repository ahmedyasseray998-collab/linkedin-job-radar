#!/usr/bin/env python3
"""Egypt-first LinkedIn radar orchestration with safer GPT delivery packets.

Version 13 keeps the broad Egypt scan, repairs the conceptual MENA/EMEA lanes
by searching regional language as keywords instead of treating those acronyms
as LinkedIn locations, and makes every Egypt delivery part contain one Job ID.
All other parts are size-bounded and carry a verifiable Job-ID manifest.
"""
from __future__ import annotations

import argparse
import copy
import json
import re
from pathlib import Path
from typing import Any

import radar_multilane as legacy
import radar_v5 as radar
from queue_integrity import job_ids_digest, read_json, reconcile_pending_queue

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "queries_v13.json"
TMP_DIR = ROOT / "output" / ".pipeline_v13"
ACTIVE_CONFIG: dict[str, Any] = {}
_ORIGINAL_COMPACT_CANDIDATE = legacy.compact_candidate


REGIONAL_TERMS = {
    "remote_mena": (
        "mena", "middle east and africa", "middle east", "gcc", "gulf cooperation council",
        "egypt", "saudi arabia", "united arab emirates", "uae", "qatar", "kuwait",
        "bahrain", "oman", "jordan", "lebanon", "morocco", "tunisia", "algeria",
    ),
    "remote_middle_east": (
        "middle east", "gcc", "gulf cooperation council", "saudi arabia",
        "united arab emirates", "uae", "qatar", "kuwait", "bahrain", "oman", "jordan",
    ),
    "remote_emea": (
        "emea", "europe, middle east and africa", "europe middle east and africa",
        "europe", "european time zone", "cet", "cest",
    ),
    "remote_egypt": ("egypt", "cairo", "giza", "new cairo", "6th of october"),
}


ELIGIBLE_REMOTE_PATTERNS = (
    ("work from anywhere", r"\bwork from anywhere\b"),
    ("remote worldwide", r"\b(?:fully\s+)?remote(?:\s+(?:role|position|job|work))?\b.{0,100}\b(?:worldwide|globally|anywhere)\b"),
    ("worldwide remote", r"\b(?:worldwide|global)\b.{0,100}\bremote\b"),
    ("remote EMEA", r"\bremote\b.{0,100}\bemea\b|\bemea\b.{0,100}\bremote\b"),
    ("remote MENA", r"\bremote\b.{0,100}\bmena\b|\bmena\b.{0,100}\bremote\b"),
    ("remote Middle East", r"\bremote\b.{0,100}\bmiddle east\b|\bmiddle east\b.{0,100}\bremote\b"),
    ("remote Egypt", r"\bremote\b.{0,100}\begypt\b|\begypt\b.{0,100}\bremote\b"),
    ("remote Africa", r"\bremote\b.{0,100}\bafrica\b|\bafrica\b.{0,100}\bremote\b"),
    ("based anywhere in eligible region", r"\bbased anywhere in (?:the )?(?:emea|mena|middle east|egypt|africa)\b"),
    ("open to eligible regional candidates", r"\bopen to (?:candidates|applicants).{0,80}\b(?:emea|mena|middle east|egypt|africa)\b"),
    ("can be based in eligible region", r"\bcan be based.{0,80}\b(?:emea|mena|middle east|egypt|africa)\b"),
)

# These phrases are geographic requirements, but they are requirements Ahmed
# satisfies from Egypt. They must not be mistaken for country blocking rules.
ALLOWED_REGIONAL_REQUIREMENT_PATTERNS = (
    (
        "required base in eligible region",
        r"\b(?:candidates?|applicants?)?(?:\s+must)?\s*(?:reside|live|be located|be based) in (?:the )?(?:emea|mena|middle east|egypt|africa)\b",
    ),
    (
        "only eligible regional candidates",
        r"\b(?:only open to|only accepting) (?:candidates|applicants).{0,60}\b(?:emea|mena|middle east|egypt|africa)\b",
    ),
)

RESTRICTION_PATTERNS = (
    ("must reside in", r"\bmust (?:reside|live) in\b"),
    ("must be located in", r"\bmust be (?:located|based) in\b"),
    ("candidates must be based in", r"\bcandidates? must be (?:located|based) in\b"),
    ("only candidates in", r"\b(?:only open to|only accepting) (?:candidates|applicants).{0,50}\bin\b"),
    ("country-limited remote", r"\bremote (?:only )?(?:within|in|from) (?:the )?(?:united states|u\.?s\.?|canada|united kingdom|uk|india|australia)\b"),
    ("authorized to work", r"\bauthori[sz]ed to work in\b"),
    ("right to work", r"\bright to work in\b"),
    ("eligible to work", r"\bmust be eligible to work in\b"),
    ("no sponsorship", r"\b(?:no|does not|do not|cannot|can't|unable to) (?:offer|provide)?\s*(?:visa )?sponsorship\b"),
    ("without sponsorship", r"\bwithout (?:current or future )?(?:visa )?sponsorship\b"),
    ("citizenship required", r"\b(?:u\.?s\.? )?citizenship (?:is )?required\b"),
    ("security clearance required", r"\b(?:active |current )?(?:security )?clearance (?:is )?required\b|\bmust (?:hold|possess|obtain).{0,40}\bclearance\b"),
)

NON_REMOTE_PATTERNS = (
    ("fully on-site", r"\b(?:fully|100%)\s+on[- ]?site\b"),
    ("role is on-site", r"\b(?:this )?(?:role|position|job) is (?:fully )?on[- ]?site\b"),
    ("office-based", r"\boffice[- ]based\b"),
    ("hybrid office requirement", r"\bhybrid\b.{0,100}\b(?:days?|times?) per week\b.{0,80}\b(?:office|on[- ]?site)\b"),
    ("working pattern hybrid", r"\bworking pattern\s*:\s*hybrid\b"),
)


def lane_definitions(base: dict[str, Any]) -> list[dict[str, Any]]:
    """Priority order matters because all passes share the same seen-job ledger."""
    return [
        {
            "name": "egypt",
            "location": base.get("location", "Egypt"),
            "remote_filter": None,
            "query_source": "queries",
            "strategy": "broad_local",
        },
        {
            "name": "remote_egypt",
            "location": "Egypt",
            "remote_filter": "remote",
            "query_source": "remote_egypt_queries",
            "strategy": "focused_remote_location",
        },
        {
            "name": "remote_mena",
            "location": "Worldwide",
            "remote_filter": "remote",
            "query_source": "regional_remote_queries.remote_mena",
            "strategy": "regional_keyword_remote",
        },
        {
            "name": "remote_middle_east",
            "location": "Worldwide",
            "remote_filter": "remote",
            "query_source": "regional_remote_queries.remote_middle_east",
            "strategy": "regional_keyword_remote",
        },
        {
            "name": "remote_emea",
            "location": "Worldwide",
            "remote_filter": "remote",
            "query_source": "regional_remote_queries.remote_emea",
            "strategy": "regional_keyword_remote",
        },
        {
            "name": "remote_worldwide",
            "location": "Worldwide",
            "remote_filter": "remote",
            "query_source": "remote_worldwide_queries",
            "strategy": "focused_global_remote",
        },
    ]


def nested_get(mapping: dict[str, Any], dotted: str) -> Any:
    value: Any = mapping
    for segment in dotted.split("."):
        if not isinstance(value, dict) or segment not in value:
            raise KeyError(f"missing config key: {dotted}")
        value = value[segment]
    return value


def prepare_lane_config(base: dict[str, Any], definition: dict[str, Any]) -> dict[str, Any]:
    config = copy.deepcopy(base)
    config["location"] = definition["location"]
    config["queries"] = copy.deepcopy(nested_get(base, definition["query_source"]))
    config["priority_retry_queries"] = [
        spec["query"] for spec in config["queries"] if bool(spec.get("retry_if_empty"))
    ]
    return config


def _pattern_hits(text: str, patterns: tuple[tuple[str, str], ...]) -> list[str]:
    return [label for label, pattern in patterns if re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)]


def is_egypt_candidate(candidate: dict[str, Any]) -> bool:
    """Avoid the historical Alexandria, Virginia -> Egypt false positive."""
    lane = str(candidate.get("discovery_lane") or "").casefold()
    if lane in {"egypt", "remote_egypt"}:
        return True
    location = str(candidate.get("location") or "").casefold()
    if re.search(r"\begypt\b|\bcairo\b|\bgiza\b|\bnew cairo\b|\b6th of october\b", location):
        return True
    return bool(re.search(r"\balexandria\b.{0,30}\begypt\b|\begypt\b.{0,30}\balexandria\b", location))


def remote_eligibility_annotation(candidate: dict[str, Any]) -> dict[str, Any]:
    description = re.sub(r"\s+", " ", str(candidate.get("description") or "")).strip()
    location = re.sub(r"\s+", " ", str(candidate.get("location") or "")).strip()
    text = f"{location}. {description[:7000]}".casefold()
    head = f"{location}. {description[:2400]}".casefold()

    allowed_requirements = _pattern_hits(text, ALLOWED_REGIONAL_REQUIREMENT_PATTERNS)
    restrictions = _pattern_hits(text, RESTRICTION_PATTERNS)
    eligible = _pattern_hits(text, ELIGIBLE_REMOTE_PATTERNS)
    non_remote = _pattern_hits(head, NON_REMOTE_PATTERNS)

    if allowed_requirements:
        generic_geography_labels = {
            "must reside in",
            "must be located in",
            "candidates must be based in",
            "only candidates in",
        }
        restrictions = [label for label in restrictions if label not in generic_geography_labels]
        eligible = list(dict.fromkeys([*eligible, *allowed_requirements]))

    if restrictions:
        status = "explicit_location_or_work_authorization_restriction"
        confidence = "high"
    elif eligible:
        status = "explicit_egypt_emea_or_global_signal"
        confidence = "high"
    elif non_remote:
        status = "explicit_non_remote"
        confidence = "high"
    else:
        status = "requires_full_review"
        confidence = "low"

    return {
        "status": status,
        "eligible_signals": eligible,
        "restriction_signals": restrictions,
        "non_remote_signals": non_remote,
        "confidence": confidence,
        "note": "Generic company words such as 'worldwide' do not count. Europe-only remote wording is not treated as open to Egypt.",
    }


def regional_evidence(candidate: dict[str, Any]) -> dict[str, Any]:
    lane = str(candidate.get("discovery_lane") or "").casefold()
    terms = REGIONAL_TERMS.get(lane, ())
    text = " ".join([
        str(candidate.get("location") or ""),
        str(candidate.get("description") or "")[:7000],
    ]).casefold()
    signals = [term for term in terms if re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text)]
    return {
        "lane": lane,
        "signals": signals,
        "confidence": "high" if signals else ("not_applicable" if not terms else "low"),
    }


def priority_lane(candidate: dict[str, Any]) -> str:
    lane = str(candidate.get("discovery_lane") or "").casefold()
    if is_egypt_candidate(candidate):
        return "egypt"
    if lane in {"remote_mena", "remote_middle_east"}:
        return "remote_mena_middle_east"
    if lane == "remote_emea":
        return "remote_emea"
    if lane == "remote_worldwide":
        return "remote_worldwide"
    return "relocation"


def delivery_bucket(candidate: dict[str, Any], config: dict[str, Any]):
    if not candidate.get("advisory_it_evidence"):
        return None, "no_it_evidence"
    if is_egypt_candidate(candidate):
        return "local", "egypt_or_egypt_remote"

    annotation = remote_eligibility_annotation(candidate)
    lane = str(candidate.get("discovery_lane") or "").casefold()
    discovered_as_remote = candidate.get("discovery_remote_filter") == "remote" or lane.startswith("remote_")

    if annotation["status"] == "explicit_non_remote":
        return "relocation", "remote_search_returned_explicit_non_remote_role"
    if annotation["status"] == "explicit_location_or_work_authorization_restriction":
        return "remote", "remote_with_explicit_restriction_for_gpt_review"
    if annotation["status"] == "explicit_egypt_emea_or_global_signal":
        return "remote", "explicit_egypt_emea_or_global_remote"
    if discovered_as_remote:
        return "remote", "remote_discovery_requires_full_review"
    return "relocation", "foreign_fit_sponsorship_unknown"


def compact_candidate(candidate: dict[str, Any], source_part: str, excerpt_chars: int = 650) -> dict[str, Any]:
    compact = _ORIGINAL_COMPACT_CANDIDATE(candidate, source_part, excerpt_chars)
    compact["priority_lane"] = priority_lane(candidate)
    compact["regional_evidence"] = regional_evidence(candidate)
    compact["remote_eligibility"] = remote_eligibility_annotation(candidate)
    compact["review_integrity"] = {
        "must_decide_job_id": str(candidate.get("linkedin_job_id") or ""),
        "acknowledge_only_after_explicit_decision": True,
    }
    return compact


def _compact_size(value: dict[str, Any]) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _chunk_compact_candidates(prepared: list[tuple[str, dict[str, Any]]]) -> list[tuple[str, list[dict[str, Any]]]]:
    local_limit = max(1, int(ACTIVE_CONFIG.get("delivery_local_chunk_size", 1)))
    default_limit = max(1, int(ACTIVE_CONFIG.get("delivery_default_chunk_size", 4)))
    max_chars = max(4000, int(ACTIVE_CONFIG.get("delivery_max_compact_chars", 14000)))
    chunks: list[tuple[str, list[dict[str, Any]]]] = []

    for tier in ("local", "remote", "relocation"):
        items = [compact for item_tier, compact in prepared if item_tier == tier]
        limit = local_limit if tier == "local" else default_limit
        current: list[dict[str, Any]] = []
        current_chars = 900
        for compact in items:
            item_chars = _compact_size(compact) + 2
            if current and (len(current) >= limit or current_chars + item_chars > max_chars):
                chunks.append((tier, current))
                current = []
                current_chars = 900
            current.append(compact)
            current_chars += item_chars
        if current:
            chunks.append((tier, current))

    return chunks or [("empty", [])]


def write_delivery_parts(
    run_id: str,
    generated_at: str,
    health: str,
    warnings: list[str],
    candidates: list[dict[str, Any]],
    source_by_job_id: dict[str, str],
    fallback_source: str,
    chunk_size: int,
    delivery_dir: Path,
    excerpt_chars: int,
) -> list[dict[str, Any]]:
    del chunk_size
    prepared: list[tuple[str, dict[str, Any]]] = []
    for candidate in candidates:
        job_id = str(candidate.get("linkedin_job_id") or "")
        tier = str(candidate.get("delivery_tier") or ("local" if is_egypt_candidate(candidate) else "remote"))
        prepared.append((tier, compact_candidate(
            candidate,
            source_by_job_id.get(job_id, fallback_source),
            excerpt_chars,
        )))

    chunks = _chunk_compact_candidates(prepared)
    parts: list[dict[str, Any]] = []
    delivery_dir.mkdir(parents=True, exist_ok=True)

    for index, (tier, compact_items) in enumerate(chunks, start=1):
        part_id = f"{run_id}-part-{index:03d}"
        filename = f"{part_id}.json"
        reference = legacy.queue_reference(delivery_dir, filename, legacy.DELIVERY_DIR)
        job_ids = [str(item.get("linkedin_job_id") or "") for item in compact_items]
        digest = job_ids_digest(job_ids)
        payload = {
            "schema_version": 4,
            "part_id": part_id,
            "run_id": run_id,
            "generated_at_utc": generated_at,
            "health": health,
            "warnings": warnings,
            "part": index,
            "part_count": len(chunks),
            "delivery_tier": tier,
            "candidate_count": len(compact_items),
            "expected_job_ids": job_ids,
            "integrity": {
                "complete_candidate_list": True,
                "job_id_count": len(job_ids),
                "job_ids_sha256": digest,
            },
            "review_candidates": compact_items,
        }
        legacy.write_json(delivery_dir / filename, payload)
        parts.append({
            "part_id": part_id,
            "path": reference,
            "delivery_tier": tier,
            "candidate_count": len(compact_items),
            "job_ids": job_ids,
            "job_ids_sha256": digest,
            "compact_chars": len(json.dumps(payload, ensure_ascii=False)),
        })
    return parts


def install_patches(config: dict[str, Any]) -> None:
    ACTIVE_CONFIG.clear()
    ACTIVE_CONFIG.update(config)
    legacy.is_egypt_candidate = is_egypt_candidate
    legacy.remote_eligibility_annotation = remote_eligibility_annotation
    legacy.delivery_bucket = delivery_bucket
    legacy.compact_candidate = compact_candidate
    legacy.write_delivery_parts = write_delivery_parts


def apply_lane_quality(merged: dict[str, Any], config: dict[str, Any]) -> None:
    quality: dict[str, Any] = {}
    candidates = merged.get("review_candidates", [])
    for definition in merged.get("search_lanes", []):
        name = definition["name"]
        lane_candidates = [item for item in candidates if item.get("discovery_lane") == name]
        evidence_count = sum(bool(regional_evidence(item)["signals"]) for item in lane_candidates)
        ratio = round(evidence_count / len(lane_candidates), 3) if lane_candidates else None
        quality[name] = {
            "strategy": definition.get("strategy"),
            "new_candidates": len(lane_candidates),
            "regional_evidence_candidates": evidence_count if name in REGIONAL_TERMS else None,
            "regional_precision": ratio if name in REGIONAL_TERMS else None,
        }

    degraded = [
        name for name, state in (merged.get("lane_health") or {}).items()
        if str(state.get("status") or "").startswith("degraded")
    ]
    low_precision = [
        name for name, state in quality.items()
        if name in {"remote_mena", "remote_middle_east", "remote_emea"}
        and state["new_candidates"] >= 5
        and (state["regional_precision"] or 0) < 0.25
    ]

    warnings = list(merged.get("warnings") or [])
    if degraded:
        warnings.append("Degraded discovery lane(s): " + ", ".join(degraded))
    if low_precision:
        warnings.append("Low regional precision lane(s): " + ", ".join(low_precision))
    merged["warnings"] = list(dict.fromkeys(warnings))
    if (degraded or low_precision) and merged.get("health") == "healthy":
        merged["health"] = "healthy_with_lane_warnings"
    merged["lane_quality"] = quality
    merged["priority_lane_names"] = config.get("priority_lane_names", [])


def annotate_pending_entry(pending: dict[str, Any], merged: dict[str, Any]) -> None:
    for entry in pending.get("runs", []):
        if entry.get("run_id") != merged.get("run_id"):
            continue
        entry["run_started_at_utc"] = merged.get("run_started_at_utc")
        entry["run_finished_at_utc"] = merged.get("generated_at_utc")
        entry["lane_health"] = merged.get("lane_health", {})
        entry["lane_quality"] = merged.get("lane_quality", {})
        entry["search_strategy_version"] = 13


def run_pipeline(cli_path: Path) -> dict[str, Any]:
    base = read_json(CONFIG_PATH)
    install_patches(base)
    definitions = lane_definitions(base)
    orchestrator_started = radar.now_utc()
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    payloads = []
    for definition in definitions:
        config = prepare_lane_config(base, definition)
        config_path = TMP_DIR / f"{definition['name']}_queries.json"
        latest_path = TMP_DIR / f"{definition['name']}.json"
        legacy.write_json(config_path, config)
        payloads.append(legacy.run_pass(
            cli_path,
            config_path,
            latest_path,
            remote_only=bool(definition["remote_filter"]),
        ))

    merged = legacy.merge_payloads(base, definitions, payloads, orchestrator_started)
    merged["schema_version"] = max(9, int(merged.get("schema_version", 0)))
    merged["search_strategy_version"] = 13
    merged["run_finished_at_utc"] = merged.get("generated_at_utc")
    merged["design"] = (
        "Egypt-first broad discovery -> regional-keyword MENA/EMEA remote lanes -> "
        "focused worldwide remote -> exact detail fetch -> size-bounded integrity-checked GPT queue"
    )
    apply_lane_quality(merged, base)

    pending = legacy.archive_run(
        merged,
        retention_hours=int(base.get("run_archive_retention_hours", 168)),
        chunk_size=int(base.get("run_archive_chunk_size", 25)),
        excerpt_chars=int(base.get("delivery_excerpt_chars", 650)),
        backlog_warning_candidates=int(base.get("delivery_backlog_warning_candidates", 500)),
        delivery_config=base,
    )
    annotate_pending_entry(pending, merged)
    legacy.write_json(legacy.PENDING_RUNS, pending)
    pending = reconcile_pending_queue(
        ROOT,
        legacy.PENDING_RUNS,
        legacy.REPORTED_RUNS,
        backlog_warning_candidates=int(base.get("delivery_backlog_warning_candidates", 500)),
    )

    merged["delivery_queue"] = dict(pending["backlog"], **{
        "index": "output/pending_runs.json",
        "compact_parts": "output/delivery",
        "lossless_parts": "output/runs",
        "reported_state": "state/reported_runs.json",
        "acknowledgement_granularity": "single-job Egypt parts; small manifest-checked parts elsewhere",
        "integrity": pending.get("integrity", {}),
    })
    audited_candidate_count = len(merged["review_candidates"])
    candidate_count = len(merged["delivery_candidates"])
    merged["candidate_payload"] = {
        "candidate_count": candidate_count,
        "audited_candidate_count": audited_candidate_count,
        "compact_delivery_index": "output/pending_runs.json",
        "lossless_records": "output/runs",
        "note": "Egypt jobs are isolated one per delivery part; all parts carry exact Job-ID manifests and digests.",
    }
    merged["review_candidates"] = []
    merged["delivery_candidates"] = []
    merged["new_matches"] = []
    legacy.write_json(legacy.FINAL_LATEST, merged)
    return {
        "run_id": merged["run_id"],
        "health": merged["health"],
        "review_candidates": candidate_count,
        "pending_runs": len(pending["runs"]),
        "lane_cards": {
            definition["name"]: payload.get("stats", {}).get("unique_live_cards", 0)
            for definition, payload in zip(definitions, payloads)
        },
        "lane_quality": merged.get("lane_quality", {}),
        "errors": len(merged.get("errors", [])),
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
