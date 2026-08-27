#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time
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


def run_cli(cli_path, args, timeout=75):
    cmd = ["bun", "run", str(cli_path), *args]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"Command failed: {' '.join(cmd)}")
    return json.loads(proc.stdout)


def norm(text):
    return " ".join((text or "").lower().replace("&", " and ").split())


def title_relevant(title, config):
    t = norm(title)
    if any(term in t for term in config["title_exclude"]):
        return False
    return any(term in t for term in config["title_include"])


def skill_hits(detail, config):
    haystack = norm(" ".join([
        detail.get("title") or "",
        detail.get("description") or "",
        detail.get("jobFunction") or "",
        detail.get("industries") or "",
    ]))
    hits = []
    for label, variants in config["skills"].items():
        if any(norm(v) in haystack for v in variants):
            hits.append(label)
    return hits


def trim_seen(seen, retention_days):
    cutoff = now_utc() - timedelta(days=retention_days)
    kept = {}
    for jid, record in seen.get("jobs", {}).items():
        raw = record.get("first_seen_utc")
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else cutoff
        except ValueError:
            ts = cutoff
        if ts >= cutoff:
            kept[jid] = record
    return {"jobs": kept}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True, help="Path to Mads linkedin-search cli.ts")
    args = parser.parse_args()

    cli_path = Path(args.cli)
    if not cli_path.exists():
        raise SystemExit(f"LinkedIn CLI not found: {cli_path}")

    config = load_json(CONFIG_PATH, {})
    required = ["location", "window_minutes", "pages_per_query", "queries", "title_include", "title_exclude", "skills"]
    missing = [k for k in required if k not in config]
    if missing:
        raise SystemExit(f"Missing config keys: {', '.join(missing)}")

    seen = trim_seen(load_json(SEEN_PATH, {"jobs": {}}), int(config.get("seen_retention_days", 90)))
    run_started = now_utc()
    cards = {}
    errors = []

    # Mads' CLI sends f_TPR=r<seconds> directly to LinkedIn's jobs-guest endpoint.
    # Multiple compact searches are intentionally used instead of one giant Boolean query.
    for query in config["queries"]:
        for page in range(1, int(config["pages_per_query"]) + 1):
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
                for card in payload.get("results", []):
                    jid = str(card.get("id") or "").strip()
                    if jid:
                        card["matched_query"] = query
                        cards.setdefault(jid, card)
            except Exception as exc:
                errors.append({"query": query, "page": page, "error": str(exc)[:500]})
            time.sleep(float(config.get("delay_seconds", 1.0)))

    new_matches = []
    new_irrelevant = 0
    already_seen = 0
    detail_failures = 0

    for jid, card in cards.items():
        if jid in seen["jobs"]:
            already_seen += 1
            continue

        if not title_relevant(card.get("title"), config):
            seen["jobs"][jid] = {
                "first_seen_utc": iso(run_started),
                "title": card.get("title"),
                "company": card.get("company"),
                "url": card.get("url"),
                "status": "filtered_title",
            }
            new_irrelevant += 1
            continue

        # Re-fetch the exact LinkedIn Job ID. A 404/non-zero response is excluded and retried next run.
        try:
            detail = run_cli(cli_path, ["detail", jid, "--format", "json"])
        except Exception as exc:
            detail_failures += 1
            errors.append({"job_id": jid, "error": str(exc)[:500]})
            continue

        hits = skill_hits(detail, config)
        seen["jobs"][jid] = {
            "first_seen_utc": iso(run_started),
            "title": detail.get("title") or card.get("title"),
            "company": detail.get("company") or card.get("company"),
            "url": detail.get("url") or card.get("url"),
            "status": "matched",
        }
        new_matches.append({
            "linkedin_job_id": jid,
            "title": detail.get("title") or card.get("title"),
            "company": detail.get("company") or card.get("company"),
            "location": detail.get("location") or card.get("location"),
            "linkedin_date": card.get("date"),
            "first_seen_utc": iso(run_started),
            "url": detail.get("url") or card.get("url") or f"https://www.linkedin.com/jobs/view/{jid}",
            "matched_query": card.get("matched_query"),
            "seniority": detail.get("seniority"),
            "employment_type": detail.get("employmentType"),
            "job_function": detail.get("jobFunction"),
            "skill_hits": hits,
            "skill_hit_count": len(hits),
            "description": (detail.get("description") or "")[:12000],
            "verification": "Exact LinkedIn jobs-guest jobPosting endpoint returned a live job detail page during this run",
        })
        time.sleep(float(config.get("delay_seconds", 1.0)))

    new_matches.sort(key=lambda x: (x["skill_hit_count"], x["title"] or ""), reverse=True)
    run_id = run_started.strftime("%Y%m%dT%H%M%SZ")
    latest = {
        "run_id": run_id,
        "generated_at_utc": iso(now_utc()),
        "source": "LinkedIn jobs-guest via MadsLorentzen/ai-job-search linkedin-search CLI",
        "location": config["location"],
        "window_minutes": config["window_minutes"],
        "stats": {
            "unique_live_cards": len(cards),
            "already_seen": already_seen,
            "new_filtered_by_title": new_irrelevant,
            "detail_failures": detail_failures,
            "new_matches": len(new_matches),
            "search_errors": len(errors),
        },
        "errors": errors[:20],
        "new_matches": new_matches,
    }

    save_json(SEEN_PATH, seen)
    save_json(LATEST_PATH, latest)
    print(json.dumps({"run_id": run_id, "new_matches": len(new_matches), "cards": len(cards), "errors": len(errors)}))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"radar failed: {exc}", file=sys.stderr)
        raise
