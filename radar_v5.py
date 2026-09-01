#!/usr/bin/env python3
import argparse
import json
import random
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "queries.json"
SEEN_PATH = ROOT / "state" / "seen.json"
LATEST_PATH = ROOT / "output" / "latest.json"


def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def jitter_sleep(seconds):
    """Spread public-endpoint requests without narrowing discovery coverage."""
    delay = max(0.0, float(seconds))
    if delay:
        time.sleep(delay * random.uniform(0.8, 1.35))


def run_cli(cli_path, args, timeout=90):
    cmd = ["bun", "run", str(cli_path), *args]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"Command failed: {' '.join(cmd)}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"CLI returned invalid JSON: {proc.stdout[:500]}") from exc


def norm(text):
    return " ".join(re.sub(r"[^a-z0-9+.#/ -]+", " ", (text or "").lower().replace("&", " and ")).split())


def contains_phrase(haystack, phrase):
    p = norm(phrase)
    if not p:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(p)}(?![a-z0-9])", haystack) is not None


def matching_signals(text, mapping):
    haystack = norm(text)
    hits = []
    total = 0.0
    for label, spec in mapping.items():
        if any(contains_phrase(haystack, variant) for variant in spec.get("variants", [])):
            weight = float(spec.get("weight", 0))
            hits.append({"label": label, "weight": weight})
            total += weight
    return hits, total


def advisory_title_signals(title, config):
    haystack = norm(title)
    terms = config.get("advisory_title_noise", config.get("hard_exclude_title", []))
    return [term for term in terms if contains_phrase(haystack, term)]


def relative_age_minutes(text):
    if not text:
        return None
    value_text = text.lower().strip()
    if any(token in value_text for token in ("just now", "moments ago", "seconds ago", "few seconds ago")):
        return 0
    match = re.search(r"(\d+)\s*(minute|min|mins|hour|hr|hrs|day|week)s?\s+ago", value_text)
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2)
    if unit.startswith("min"):
        return value
    if unit.startswith("h"):
        return value * 60
    if unit == "day":
        return value * 1440
    if unit == "week":
        return value * 10080
    return None


def freshness_info(card, window_minutes, tolerance_minutes=15):
    posted_text = card.get("postedText") or card.get("posted_text")
    age = relative_age_minutes(posted_text)
    if age is None:
        return {
            "posted_text": posted_text,
            "estimated_age_minutes": None,
            "within_requested_window": None,
            "verification": "linkedin_f_TPR_window_only",
            "conflict": False,
        }
    within = age <= int(window_minutes) + int(tolerance_minutes)
    return {
        "posted_text": posted_text,
        "estimated_age_minutes": age,
        "within_requested_window": within,
        "verification": "linkedin_f_TPR_plus_relative_text" if within else "relative_text_conflicts_with_f_TPR_window",
        "conflict": not within,
    }


def score_detail(detail, query_weight, config):
    title = detail.get("title") or ""
    description = detail.get("description") or ""
    metadata = " ".join([detail.get("jobFunction") or "", detail.get("industries") or ""])
    title_roles, title_role_score = matching_signals(title, config.get("role_signals", {}))
    body_roles, body_role_score = matching_signals(description + " " + metadata, config.get("role_signals", {}))
    skills, skill_score = matching_signals(title + " " + description + " " + metadata, config.get("skills", {}))
    negatives, negative_score = matching_signals(title + " " + metadata, config.get("negative_signals", {}))
    score = float(query_weight) + title_role_score + min(body_role_score, 5.0) + skill_score + negative_score
    return {
        "score": round(score, 1),
        "role_hits_title": title_roles,
        "role_hits_description": body_roles,
        "skill_hits": skills,
        "negative_hits": negatives,
    }


def has_it_evidence(scoring):
    return bool(scoring["role_hits_title"] or scoring["role_hits_description"] or scoring["skill_hits"])


def validate_config(config):
    errors = []
    for key in ("location", "window_minutes", "queries", "role_signals", "skills"):
        if key not in config:
            errors.append(f"missing required key: {key}")
    if errors:
        return errors
    if not isinstance(config["location"], str) or not config["location"].strip():
        errors.append("location must be a non-empty string")
    try:
        if int(config["window_minutes"]) <= 0:
            errors.append("window_minutes must be positive")
    except (TypeError, ValueError):
        errors.append("window_minutes must be an integer")
    if not isinstance(config["queries"], list) or not config["queries"]:
        errors.append("queries must be a non-empty list")
    else:
        seen_queries = set()
        for index, spec in enumerate(config["queries"]):
            if not isinstance(spec, dict) or not isinstance(spec.get("query"), str) or not spec["query"].strip():
                errors.append(f"queries[{index}].query must be a non-empty string")
                continue
            normalized = spec["query"].casefold().strip()
            if normalized in seen_queries:
                errors.append(f"duplicate query: {spec['query']}")
            seen_queries.add(normalized)
            try:
                if int(spec.get("pages", 1)) < 1:
                    errors.append(f"query {spec['query']}: pages must be >= 1")
            except (TypeError, ValueError):
                errors.append(f"query {spec['query']}: pages must be an integer")
    return errors


def trim_seen(seen, retention_days, config_version):
    cutoff = now_utc() - timedelta(days=retention_days)
    previous_version = seen.get("config_version")
    kept = {}
    for job_id, record in seen.get("jobs", {}).items():
        raw = record.get("first_seen_utc")
        try:
            timestamp = datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else cutoff
        except (ValueError, AttributeError):
            timestamp = cutoff
        if timestamp < cutoff:
            continue
        # Old filtered decisions are never trusted after a policy/config change.
        if previous_version != config_version and str(record.get("status", "")).startswith("filtered"):
            continue
        kept[job_id] = record
    return {"config_version": config_version, "jobs": kept}


def search_page(cli_path, query, location, window_minutes, page):
    payload = run_cli(cli_path, [
        "search",
        "--query", query,
        "--location", location,
        "--jobage-minutes", str(window_minutes),
        "--page", str(page),
        "--limit", "10",
        "--format", "json",
    ])
    return payload.get("results", [])


def choose_title(detail, card):
    value = detail.get("title")
    if value and value != "(untitled)":
        return value
    return card.get("title")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    args = parser.parse_args()

    cli_path = Path(args.cli)
    if not cli_path.exists():
        raise SystemExit(f"LinkedIn CLI not found: {cli_path}")

    config = load_json(CONFIG_PATH, {})
    config_errors = validate_config(config)
    if config_errors:
        raise SystemExit("Invalid config: " + "; ".join(config_errors))

    config_version = int(config.get("config_version", 1))
    seen = trim_seen(
        load_json(SEEN_PATH, {"jobs": {}}),
        int(config.get("seen_retention_days", 60)),
        config_version,
    )
    run_started = now_utc()
    cards = {}
    matched_queries = defaultdict(list)
    query_stats = []
    errors = []
    priority_retry_queries = {
        str(query).casefold().strip()
        for query in config.get("priority_retry_queries", [])
    }

    for spec in config["queries"]:
        query = spec["query"]
        pages = int(spec.get("pages", 2))
        query_weight = float(spec.get("query_weight", 0))
        unique_for_query = set()
        page_counts = []
        retry_counts = {}
        query_errors = 0

        def ingest(results):
            for card in results:
                job_id = str(card.get("id") or "").strip()
                if not job_id:
                    continue
                unique_for_query.add(job_id)
                if not any(item["query"] == query for item in matched_queries[job_id]):
                    matched_queries[job_id].append({"query": query, "weight": query_weight})
                if job_id not in cards:
                    cards[job_id] = card
                elif not cards[job_id].get("postedText") and card.get("postedText"):
                    cards[job_id]["postedText"] = card.get("postedText")

        for page in range(1, pages + 1):
            results = []
            try:
                results = search_page(cli_path, query, config["location"], config["window_minutes"], page)
                page_counts.append(len(results))
            except Exception as exc:
                query_errors += 1
                page_counts.append(None)
                errors.append({"stage": "search", "query": query, "page": page, "error": str(exc)[:500]})

            retry_allowed = (
                bool(spec.get("retry_if_empty", False))
                and (not priority_retry_queries or query.casefold().strip() in priority_retry_queries)
            )
            if page == 1 and not results and retry_allowed:
                jitter_sleep(config.get("empty_retry_delay_seconds", 2.5))
                try:
                    retried = search_page(cli_path, query, config["location"], config["window_minutes"], 1)
                    retry_counts[str(page)] = len(retried)
                    if retried:
                        results = retried
                except Exception as exc:
                    query_errors += 1
                    retry_counts[str(page)] = None
                    errors.append({"stage": "search_retry", "query": query, "page": 1, "error": str(exc)[:500]})

            ingest(results)
            jitter_sleep(config.get("delay_seconds", 1.0))
            if len(results) < 10:
                break

        query_stats.append({
            "query": query,
            "pages_configured": pages,
            "pages_requested": len(page_counts),
            "page_counts": page_counts,
            "retry_page_counts": retry_counts,
            "unique_cards": len(unique_for_query),
            "errors": query_errors,
        })

    already_seen = 0
    detail_failures = 0
    detail_budget_skipped = 0
    detail_attempts = 0
    details_fetched = 0
    closed_candidates = 0
    freshness_conflicts = 0
    low_it_evidence_candidates = 0
    title_noise_candidates = 0
    review_candidates = []
    max_details = int(config.get("max_detail_fetches", 100))

    def priority(job_id):
        queries = matched_queries[job_id]
        return (max((q["weight"] for q in queries), default=0), len(queries))

    for job_id in sorted(cards, key=priority, reverse=True):
        card = cards[job_id]
        if job_id in seen["jobs"]:
            already_seen += 1
            continue

        if detail_attempts >= max_details:
            detail_budget_skipped += 1
            continue

        detail_attempts += 1
        try:
            detail = run_cli(cli_path, ["detail", job_id, "--format", "json"])
            details_fetched += 1
        except Exception as exc:
            detail_failures += 1
            errors.append({"stage": "detail", "job_id": job_id, "error": str(exc)[:500]})
            # Do not mark as seen. A failed verification must be retried next run.
            continue

        title = choose_title(detail, card)
        company = detail.get("company") or card.get("company")
        location = detail.get("location") or card.get("location")
        url = detail.get("url") or card.get("url") or f"https://www.linkedin.com/jobs/view/{job_id}"
        queries = matched_queries[job_id]
        query_weight = max((q["weight"] for q in queries), default=0)
        scoring = score_detail(detail, query_weight, config)
        freshness = freshness_info(card, config["window_minutes"], config.get("freshness_tolerance_minutes", 15))
        title_noise = advisory_title_signals(title, config)
        it_evidence = has_it_evidence(scoring)
        application_status = detail.get("applicationStatus") or "unknown"

        if application_status == "closed_explicit":
            closed_candidates += 1
        if freshness["conflict"]:
            freshness_conflicts += 1
        if not it_evidence:
            low_it_evidence_candidates += 1
        if title_noise:
            title_noise_candidates += 1

        candidate = {
            "linkedin_job_id": job_id,
            "title": title,
            "company": company,
            "location": location,
            "linkedin_date": card.get("date"),
            "linkedin_posted_text": freshness["posted_text"],
            "estimated_age_minutes": freshness["estimated_age_minutes"],
            "freshness_within_requested_window": freshness["within_requested_window"],
            "freshness_verification": freshness["verification"],
            "freshness_conflict": freshness["conflict"],
            "first_seen_utc": iso(run_started),
            "url": url,
            "matched_queries": queries,
            "application_status": application_status,
            "seniority": detail.get("seniority"),
            "employment_type": detail.get("employmentType"),
            "job_function": detail.get("jobFunction"),
            "advisory_title_noise_signals": title_noise,
            "advisory_it_evidence": it_evidence,
            **scoring,
            "description": (detail.get("description") or "")[:14000],
            "verification": "Exact LinkedIn jobs-guest jobPosting endpoint returned a job detail page during this run",
            "review_policy": "Always pass exact-detail candidate to ChatGPT; closed status, weak keyword evidence, noisy title, or freshness conflict are annotations only",
        }
        review_candidates.append(candidate)
        seen["jobs"][job_id] = {
            "first_seen_utc": iso(run_started),
            "title": title,
            "company": company,
            "url": url,
            "status": "review_candidate",
            "application_status": application_status,
            "score": scoring["score"],
        }
        jitter_sleep(config.get("delay_seconds", 1.0))

    # Every successfully exact-detail-fetched new card must reach the review queue.
    if len(review_candidates) != details_fetched:
        raise RuntimeError(
            f"Lossless-review invariant failed: details_fetched={details_fetched}, review_candidates={len(review_candidates)}"
        )

    review_candidates.sort(
        key=lambda item: (item["score"], len(item["skill_hits"]), len(item["role_hits_title"]), item["title"] or ""),
        reverse=True,
    )

    search_errors = sum(1 for error in errors if str(error.get("stage", "")).startswith("search"))
    retry_recoveries = sum(
        1 for stat in query_stats if any((count or 0) > 0 for count in stat["retry_page_counts"].values())
    )
    warnings = []
    if retry_recoveries:
        warnings.append(f"LinkedIn guest search needed recovery retries for {retry_recoveries} query bucket(s)")
    if search_errors:
        warnings.append(f"{search_errors} search request(s) failed")
    if detail_failures:
        warnings.append(f"{detail_failures} exact-detail request(s) failed and remain unseen so they can retry next run")
    if detail_budget_skipped:
        warnings.append(f"{detail_budget_skipped} candidate(s) exceeded the detail budget and remain unseen so they can retry next run")
    if freshness_conflicts:
        warnings.append(f"{freshness_conflicts} candidate(s) had relative-age text that conflicted with LinkedIn's f_TPR search window; they were kept for review")

    if search_errors or detail_failures or detail_budget_skipped:
        health = "degraded"
    elif retry_recoveries or freshness_conflicts:
        health = "healthy_with_retries"
    elif not cards:
        health = "healthy_empty"
    else:
        health = "healthy"

    latest = {
        "schema_version": 5,
        "config_version": config_version,
        "run_id": run_started.strftime("%Y%m%dT%H%M%SZ"),
        "run_started_at_utc": iso(run_started),
        "generated_at_utc": iso(now_utc()),
        "health": health,
        "warnings": warnings,
        "source": "LinkedIn jobs-guest via pinned MadsLorentzen/ai-job-search linkedin-search CLI",
        "location": config["location"],
        "window_minutes": config["window_minutes"],
        "design": "broad discovery -> overlap/retry -> Job-ID dedupe -> exact detail fetch -> annotate only -> lossless ChatGPT review queue",
        "policy": {
            "relevance_rejections": 0,
            "closed_application_rejections": 0,
            "freshness_conflict_rejections": 0,
            "weak_keyword_rejections": 0,
            "title_noise_rejections": 0,
            "principle": "Do not let code decide job fit after exact-detail retrieval; ChatGPT reviews every verified new candidate",
        },
        "stats": {
            "query_count": len(config["queries"]),
            "unique_live_cards": len(cards),
            "already_seen": already_seen,
            "detail_attempts": detail_attempts,
            "details_fetched": details_fetched,
            "detail_failures": detail_failures,
            "detail_budget_skipped": detail_budget_skipped,
            "review_candidates": len(review_candidates),
            "closed_candidates_kept": closed_candidates,
            "freshness_conflicts_kept": freshness_conflicts,
            "low_it_evidence_candidates_kept": low_it_evidence_candidates,
            "title_noise_candidates_kept": title_noise_candidates,
            "search_errors": search_errors,
            "query_retry_recoveries": retry_recoveries,
            "relevance_rejections": 0,
        },
        "query_stats": query_stats,
        "errors": errors[:30],
        "review_candidates": review_candidates,
        "new_matches": review_candidates,
    }

    save_json(SEEN_PATH, seen)
    save_json(LATEST_PATH, latest)
    print(json.dumps({
        "run_id": latest["run_id"],
        "health": health,
        "cards": len(cards),
        "review_candidates": len(review_candidates),
        "errors": len(errors),
    }))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"radar failed: {exc}", file=sys.stderr)
        raise
