#!/usr/bin/env python3
import argparse
import json
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


def run_cli(cli_path, args, timeout=90):
    cmd = ["bun", "run", str(cli_path), *args]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"Command failed: {' '.join(cmd)}")
    return json.loads(proc.stdout)


def norm(text):
    text = (text or "").lower().replace("&", " and ")
    return " ".join(re.sub(r"[^a-z0-9+.#/ -]+", " ", text).split())


def contains_phrase(haystack, phrase):
    p = norm(phrase)
    if not p:
        return False
    if len(p) <= 3 and p.isalnum():
        return re.search(rf"(?<![a-z0-9]){re.escape(p)}(?![a-z0-9])", haystack) is not None
    return p in haystack


def matching_signals(text, mapping):
    h = norm(text)
    hits = []
    total = 0.0
    for label, spec in mapping.items():
        if any(contains_phrase(h, v) for v in spec.get("variants", [])):
            weight = float(spec.get("weight", 0))
            hits.append({"label": label, "weight": weight})
            total += weight
    return hits, total


def hard_excluded(title, config):
    t = norm(title)
    return [term for term in config.get("hard_exclude_title", []) if contains_phrase(t, term)]


def score_detail(detail, query_weight, config):
    title = detail.get("title") or ""
    description = detail.get("description") or ""
    metadata = " ".join([detail.get("jobFunction") or "", detail.get("industries") or ""])

    title_roles, title_role_score = matching_signals(title, config.get("role_signals", {}))
    body_roles, body_role_score = matching_signals(description + " " + metadata, config.get("role_signals", {}))
    skills, skill_score = matching_signals(title + " " + description + " " + metadata, config.get("skills", {}))
    negatives, negative_score = matching_signals(title, config.get("negative_signals", {}))

    # Title relevance matters most. Body role mentions are useful but capped so a generic
    # job description cannot overpower an unrelated title merely by listing IT keywords.
    score = float(query_weight) + title_role_score + min(body_role_score, 5.0) + skill_score + negative_score

    return {
        "score": round(score, 1),
        "role_hits_title": title_roles,
        "role_hits_description": body_roles,
        "skill_hits": skills,
        "negative_hits": negatives,
    }


def trim_seen(seen, retention_days, config_version):
    cutoff = now_utc() - timedelta(days=retention_days)
    previous_version = seen.get("config_version")
    kept = {}
    for jid, record in seen.get("jobs", {}).items():
        raw = record.get("first_seen_utc")
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else cutoff
        except ValueError:
            ts = cutoff
        if ts < cutoff:
            continue
        # Jobs discarded by an older filtering model deserve another look after a config upgrade.
        if previous_version != config_version and record.get("status", "").startswith("filtered"):
            continue
        kept[jid] = record
    return {"config_version": config_version, "jobs": kept}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True, help="Path to Mads linkedin-search cli.ts")
    args = parser.parse_args()

    cli_path = Path(args.cli)
    if not cli_path.exists():
        raise SystemExit(f"LinkedIn CLI not found: {cli_path}")

    config = load_json(CONFIG_PATH, {})
    required = ["location", "window_minutes", "queries", "role_signals", "skills"]
    missing = [k for k in required if k not in config]
    if missing:
        raise SystemExit(f"Missing config keys: {', '.join(missing)}")

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

    # Broad discovery. Each query gets its own diagnostics so zero-result runs are auditable.
    for qspec in config["queries"]:
        query = qspec["query"]
        pages = int(qspec.get("pages", 2))
        query_weight = float(qspec.get("query_weight", 0))
        q_unique = set()
        page_counts = []
        q_errors = 0
        for page in range(1, pages + 1):
            try:
                payload = run_cli(cli_path, [
                    "search",
                    "--query", query,
                    "--location", config["location"],
                    "--jobage-minutes", str(config["window_minutes"]),
                    "--page", str(page),
                    "--limit", "10",
                    "--format", "json",
                ])
                results = payload.get("results", [])
                page_counts.append(len(results))
                for card in results:
                    jid = str(card.get("id") or "").strip()
                    if not jid:
                        continue
                    q_unique.add(jid)
                    matched_queries[jid].append({"query": query, "weight": query_weight})
                    if jid not in cards:
                        cards[jid] = card
            except Exception as exc:
                q_errors += 1
                page_counts.append(None)
                errors.append({"stage": "search", "query": query, "page": page, "error": str(exc)[:500]})
            time.sleep(float(config.get("delay_seconds", 1.0)))
        query_stats.append({
            "query": query,
            "pages_requested": pages,
            "page_counts": page_counts,
            "unique_cards": len(q_unique),
            "errors": q_errors,
        })

    already_seen = 0
    hard_filtered = 0
    detail_failures = 0
    detail_budget_skipped = 0
    verified_new = []
    filtered_samples = []
    max_details = int(config.get("max_detail_fetches", 80))
    details_used = 0

    # Prefer cards seen by multiple searches, then those whose query was more targeted.
    def discovery_priority(jid):
        qs = matched_queries[jid]
        return (len(qs), max((q["weight"] for q in qs), default=0))

    for jid in sorted(cards.keys(), key=discovery_priority, reverse=True):
        card = cards[jid]
        if jid in seen["jobs"]:
            already_seen += 1
            continue

        excluded_by = hard_excluded(card.get("title"), config)
        if excluded_by:
            seen["jobs"][jid] = {
                "first_seen_utc": iso(run_started),
                "title": card.get("title"),
                "company": card.get("company"),
                "url": card.get("url"),
                "status": "filtered_hard_title",
                "excluded_by": excluded_by,
            }
            hard_filtered += 1
            if len(filtered_samples) < int(config.get("filtered_sample_limit", 20)):
                filtered_samples.append({
                    "linkedin_job_id": jid,
                    "title": card.get("title"),
                    "company": card.get("company"),
                    "excluded_by": excluded_by,
                })
            continue

        if details_used >= max_details:
            detail_budget_skipped += 1
            continue

        try:
            detail = run_cli(cli_path, ["detail", jid, "--format", "json"])
            details_used += 1
        except Exception as exc:
            detail_failures += 1
            errors.append({"stage": "detail", "job_id": jid, "error": str(exc)[:500]})
            continue

        qs = matched_queries[jid]
        best_query_weight = max((q["weight"] for q in qs), default=0)
        scoring = score_detail(detail, best_query_weight, config)
        title = detail.get("title") or card.get("title")
        company = detail.get("company") or card.get("company")
        url = detail.get("url") or card.get("url") or f"https://www.linkedin.com/jobs/view/{jid}"

        seen["jobs"][jid] = {
            "first_seen_utc": iso(run_started),
            "title": title,
            "company": company,
            "url": url,
            "status": "verified",
            "score": scoring["score"],
        }

        verified_new.append({
            "linkedin_job_id": jid,
            "title": title,
            "company": company,
            "location": detail.get("location") or card.get("location"),
            "linkedin_date": card.get("date"),
            "first_seen_utc": iso(run_started),
            "url": url,
            "matched_queries": qs,
            "seniority": detail.get("seniority"),
            "employment_type": detail.get("employmentType"),
            "job_function": detail.get("jobFunction"),
            **scoring,
            "description": (detail.get("description") or "")[:14000],
            "verification": "Exact LinkedIn jobs-guest jobPosting endpoint returned a live job detail page during this run",
        })
        time.sleep(float(config.get("delay_seconds", 1.0)))

    verified_new.sort(key=lambda x: (x["score"], len(x["skill_hits"]), x["title"] or ""), reverse=True)

    run_id = run_started.strftime("%Y%m%dT%H%M%SZ")
    latest = {
        "schema_version": int(config.get("schema_version", 3)),
        "config_version": config_version,
        "run_id": run_id,
        "run_started_at_utc": iso(run_started),
        "generated_at_utc": iso(now_utc()),
        "source": "LinkedIn jobs-guest via pinned MadsLorentzen/ai-job-search linkedin-search CLI",
        "location": config["location"],
        "window_minutes": config["window_minutes"],
        "design": "broad discovery -> Job-ID dedupe -> exact detail verification -> explainable scoring -> ChatGPT final fit review",
        "stats": {
            "query_count": len(config["queries"]),
            "unique_live_cards": len(cards),
            "already_seen": already_seen,
            "hard_filtered": hard_filtered,
            "details_fetched": details_used,
            "detail_failures": detail_failures,
            "detail_budget_skipped": detail_budget_skipped,
            "verified_new_candidates": len(verified_new),
            "search_errors": sum(1 for e in errors if e.get("stage") == "search"),
        },
        "query_stats": query_stats,
        "errors": errors[:30],
        "filtered_samples": filtered_samples,
        # Compatibility with the ChatGPT notifier. These are verified candidates, not final recommendations.
        "new_matches": verified_new,
    }

    save_json(SEEN_PATH, seen)
    save_json(LATEST_PATH, latest)
    print(json.dumps({
        "run_id": run_id,
        "cards": len(cards),
        "verified_new_candidates": len(verified_new),
        "hard_filtered": hard_filtered,
        "errors": len(errors),
    }))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"radar failed: {exc}", file=sys.stderr)
        raise
