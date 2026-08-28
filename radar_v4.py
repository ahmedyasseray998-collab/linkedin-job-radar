#!/usr/bin/env python3
import argparse, json, re, subprocess, sys, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "queries.json"
SEEN_PATH = ROOT / "state" / "seen.json"
LATEST_PATH = ROOT / "output" / "latest.json"


def now_utc(): return datetime.now(timezone.utc)
def iso(dt): return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def load_json(path, default):
    try: return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError): return default

def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def run_cli(cli_path, args, timeout=90):
    cmd = ["bun", "run", str(cli_path), *args]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"Command failed: {' '.join(cmd)}")
    try: return json.loads(proc.stdout)
    except json.JSONDecodeError as exc: raise RuntimeError(f"CLI returned invalid JSON: {proc.stdout[:500]}") from exc

def norm(text):
    return " ".join(re.sub(r"[^a-z0-9+.#/ -]+", " ", (text or "").lower().replace("&", " and ")).split())

def contains_phrase(haystack, phrase):
    p = norm(phrase)
    if not p: return False
    if len(p) <= 3 and p.isalnum():
        return re.search(rf"(?<![a-z0-9]){re.escape(p)}(?![a-z0-9])", haystack) is not None
    return p in haystack

def matching_signals(text, mapping):
    h, hits, total = norm(text), [], 0.0
    for label, spec in mapping.items():
        if any(contains_phrase(h, v) for v in spec.get("variants", [])):
            weight = float(spec.get("weight", 0)); hits.append({"label": label, "weight": weight}); total += weight
    return hits, total

def hard_excluded(title, config):
    t = norm(title)
    return [term for term in config.get("hard_exclude_title", []) if contains_phrase(t, term)]

def relative_age_minutes(text):
    if not text: return None
    t = text.lower().strip()
    if any(x in t for x in ("just now", "moments ago", "seconds ago", "few seconds ago")): return 0
    m = re.search(r"(\d+)\s*(minute|min|mins|hour|hr|hrs|day|week)s?\s+ago", t)
    if not m: return None
    value, unit = int(m.group(1)), m.group(2)
    if unit.startswith("min"): return value
    if unit.startswith("h"): return value * 60
    if unit == "day": return value * 1440
    if unit == "week": return value * 10080
    return None

def freshness_info(card, window_minutes, tolerance_minutes=15):
    posted_text = card.get("postedText") or card.get("posted_text")
    age = relative_age_minutes(posted_text)
    if age is None:
        return {"posted_text": posted_text, "estimated_age_minutes": None, "within_window": None, "verification": "linkedin_f_TPR_window_only"}
    within = age <= int(window_minutes) + int(tolerance_minutes)
    return {"posted_text": posted_text, "estimated_age_minutes": age, "within_window": within, "verification": "linkedin_f_TPR_plus_relative_text" if within else "relative_text_outside_requested_window"}

def score_detail(detail, query_weight, config):
    title = detail.get("title") or ""; desc = detail.get("description") or ""
    metadata = " ".join([detail.get("jobFunction") or "", detail.get("industries") or ""])
    title_roles, trs = matching_signals(title, config.get("role_signals", {}))
    body_roles, brs = matching_signals(desc + " " + metadata, config.get("role_signals", {}))
    skills, ss = matching_signals(title + " " + desc + " " + metadata, config.get("skills", {}))
    negatives, ns = matching_signals(title + " " + metadata, config.get("negative_signals", {}))
    return {"score": round(float(query_weight) + trs + min(brs, 5.0) + ss + ns, 1), "role_hits_title": title_roles, "role_hits_description": body_roles, "skill_hits": skills, "negative_hits": negatives}

def has_it_evidence(scoring):
    return bool(scoring["role_hits_title"] or scoring["role_hits_description"] or scoring["skill_hits"])

def validate_config(config):
    errors = []
    for key in ("location", "window_minutes", "queries", "role_signals", "skills"):
        if key not in config: errors.append(f"missing required key: {key}")
    if errors: return errors
    if not isinstance(config["location"], str) or not config["location"].strip(): errors.append("location must be a non-empty string")
    try:
        if int(config["window_minutes"]) <= 0: errors.append("window_minutes must be positive")
    except (TypeError, ValueError): errors.append("window_minutes must be an integer")
    if not isinstance(config["queries"], list) or not config["queries"]: errors.append("queries must be a non-empty list")
    else:
        seen = set()
        for i, q in enumerate(config["queries"]):
            if not isinstance(q, dict) or not isinstance(q.get("query"), str) or not q["query"].strip(): errors.append(f"queries[{i}].query must be a non-empty string"); continue
            k = q["query"].casefold().strip()
            if k in seen: errors.append(f"duplicate query: {q['query']}")
            seen.add(k)
            try:
                if int(q.get("pages", 1)) < 1: errors.append(f"query {q['query']}: pages must be >= 1")
            except (TypeError, ValueError): errors.append(f"query {q['query']}: pages must be an integer")
    return errors

def trim_seen(seen, retention_days, config_version):
    cutoff = now_utc() - timedelta(days=retention_days); previous = seen.get("config_version"); kept = {}
    for jid, record in seen.get("jobs", {}).items():
        raw = record.get("first_seen_utc")
        try: ts = datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else cutoff
        except (ValueError, AttributeError): ts = cutoff
        if ts < cutoff: continue
        if previous != config_version and record.get("status", "").startswith("filtered"): continue
        kept[jid] = record
    return {"config_version": config_version, "jobs": kept}

def search_page(cli_path, query, location, window_minutes, page):
    return run_cli(cli_path, ["search", "--query", query, "--location", location, "--jobage-minutes", str(window_minutes), "--page", str(page), "--limit", "10", "--format", "json"]).get("results", [])

def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--cli", required=True); args = parser.parse_args()
    cli_path = Path(args.cli)
    if not cli_path.exists(): raise SystemExit(f"LinkedIn CLI not found: {cli_path}")
    config = load_json(CONFIG_PATH, {}); config_errors = validate_config(config)
    if config_errors: raise SystemExit("Invalid config: " + "; ".join(config_errors))

    config_version = int(config.get("config_version", 1)); seen = trim_seen(load_json(SEEN_PATH, {"jobs": {}}), int(config.get("seen_retention_days", 60)), config_version)
    run_started = now_utc(); cards = {}; matched_queries = defaultdict(list); query_stats = []; errors = []

    for qspec in config["queries"]:
        query, pages, qweight = qspec["query"], int(qspec.get("pages", 2)), float(qspec.get("query_weight", 0))
        q_unique, page_counts, retry_counts, q_errors = set(), [], {}, 0
        def ingest(results):
            for card in results:
                jid = str(card.get("id") or "").strip()
                if not jid: continue
                q_unique.add(jid)
                if not any(x["query"] == query for x in matched_queries[jid]): matched_queries[jid].append({"query": query, "weight": qweight})
                if jid not in cards: cards[jid] = card
                elif not cards[jid].get("postedText") and card.get("postedText"): cards[jid]["postedText"] = card.get("postedText")
        for page in range(1, pages + 1):
            results = []
            try: results = search_page(cli_path, query, config["location"], config["window_minutes"], page); page_counts.append(len(results))
            except Exception as exc: q_errors += 1; page_counts.append(None); errors.append({"stage": "search", "query": query, "page": page, "error": str(exc)[:500]})
            if page == 1 and not results and bool(qspec.get("retry_if_empty", False)):
                time.sleep(float(config.get("empty_retry_delay_seconds", 2.5)))
                try:
                    retried = search_page(cli_path, query, config["location"], config["window_minutes"], 1); retry_counts[str(page)] = len(retried)
                    if retried: results = retried
                except Exception as exc: q_errors += 1; retry_counts[str(page)] = None; errors.append({"stage": "search_retry", "query": query, "page": 1, "error": str(exc)[:500]})
            ingest(results); time.sleep(float(config.get("delay_seconds", 1.0)))
            if len(results) < 10: break
        query_stats.append({"query": query, "pages_configured": pages, "pages_requested": len(page_counts), "page_counts": page_counts, "retry_page_counts": retry_counts, "unique_cards": len(q_unique), "errors": q_errors})

    counters = {"already_seen":0,"hard_filtered":0,"stale_filtered":0,"closed_filtered":0,"no_it_evidence_filtered":0,"detail_failures":0,"detail_budget_skipped":0}
    verified_new, filtered_samples = [], []; max_details = int(config.get("max_detail_fetches", 80)); attempts = successes = 0
    def sample(item):
        if len(filtered_samples) < int(config.get("filtered_sample_limit", 20)): filtered_samples.append(item)
    def priority(jid):
        qs = matched_queries[jid]; return (max((q["weight"] for q in qs), default=0), len(qs))

    for jid in sorted(cards, key=priority, reverse=True):
        card = cards[jid]
        if jid in seen["jobs"]: counters["already_seen"] += 1; continue
        excluded = hard_excluded(card.get("title"), config)
        if excluded:
            counters["hard_filtered"] += 1; seen["jobs"][jid] = {"first_seen_utc":iso(run_started),"title":card.get("title"),"company":card.get("company"),"url":card.get("url"),"status":"filtered_hard_title","excluded_by":excluded}; sample({"linkedin_job_id":jid,"title":card.get("title"),"reason":"hard_title","excluded_by":excluded}); continue
        fresh = freshness_info(card, config["window_minutes"], config.get("freshness_tolerance_minutes", 15))
        if fresh["within_window"] is False:
            counters["stale_filtered"] += 1; seen["jobs"][jid] = {"first_seen_utc":iso(run_started),"title":card.get("title"),"company":card.get("company"),"url":card.get("url"),"status":"filtered_stale_relative_text",**fresh}; sample({"linkedin_job_id":jid,"title":card.get("title"),"reason":"stale_relative_text",**fresh}); continue
        if attempts >= max_details: counters["detail_budget_skipped"] += 1; continue
        attempts += 1
        try: detail = run_cli(cli_path, ["detail", jid, "--format", "json"]); successes += 1
        except Exception as exc: counters["detail_failures"] += 1; errors.append({"stage":"detail","job_id":jid,"error":str(exc)[:500]}); continue
        title = detail.get("title") or card.get("title"); company = detail.get("company") or card.get("company"); url = detail.get("url") or card.get("url") or f"https://www.linkedin.com/jobs/view/{jid}"
        if detail.get("applicationStatus") == "closed_explicit":
            counters["closed_filtered"] += 1; seen["jobs"][jid] = {"first_seen_utc":iso(run_started),"title":title,"company":company,"url":url,"status":"filtered_closed_explicit"}; sample({"linkedin_job_id":jid,"title":title,"reason":"closed_explicit"}); continue
        qs = matched_queries[jid]; score = score_detail(detail, max((q["weight"] for q in qs), default=0), config)
        if not has_it_evidence(score):
            counters["no_it_evidence_filtered"] += 1; seen["jobs"][jid] = {"first_seen_utc":iso(run_started),"title":title,"company":company,"url":url,"status":"filtered_no_it_evidence","score":score["score"]}; sample({"linkedin_job_id":jid,"title":title,"company":company,"reason":"no_it_evidence","score":score["score"]}); continue
        seen["jobs"][jid] = {"first_seen_utc":iso(run_started),"title":title,"company":company,"url":url,"status":"verified","score":score["score"]}
        verified_new.append({"linkedin_job_id":jid,"title":title,"company":company,"location":detail.get("location") or card.get("location"),"linkedin_date":card.get("date"),"linkedin_posted_text":fresh["posted_text"],"estimated_age_minutes":fresh["estimated_age_minutes"],"freshness_verification":fresh["verification"],"first_seen_utc":iso(run_started),"url":url,"matched_queries":qs,"application_status":detail.get("applicationStatus") or "unknown","seniority":detail.get("seniority"),"employment_type":detail.get("employmentType"),"job_function":detail.get("jobFunction"),**score,"description":(detail.get("description") or "")[:14000],"verification":"Exact LinkedIn jobs-guest jobPosting endpoint returned a live job detail page during this run"}); time.sleep(float(config.get("delay_seconds", 1.0)))

    verified_new.sort(key=lambda x:(x["score"],len(x["skill_hits"]),x["title"] or ""), reverse=True)
    search_errors = sum(1 for e in errors if e.get("stage", "").startswith("search")); retries = sum(1 for q in query_stats if any((n or 0)>0 for n in q["retry_page_counts"].values()))
    warnings=[]
    if retries: warnings.append(f"LinkedIn guest search needed recovery retries for {retries} query bucket(s)")
    if search_errors: warnings.append(f"{search_errors} search request(s) failed")
    if counters["detail_failures"]: warnings.append(f"{counters['detail_failures']} detail verification request(s) failed and will be retried next run")
    if counters["detail_budget_skipped"]: warnings.append(f"{counters['detail_budget_skipped']} candidate(s) exceeded the detail budget and will be retried next run")
    health = "degraded" if (search_errors or counters["detail_failures"] or counters["detail_budget_skipped"]) else ("healthy_with_retries" if retries else ("healthy_empty" if not cards else "healthy"))
    latest={"schema_version":4,"config_version":config_version,"run_id":run_started.strftime("%Y%m%dT%H%M%SZ"),"run_started_at_utc":iso(run_started),"generated_at_utc":iso(now_utc()),"health":health,"warnings":warnings,"source":"LinkedIn jobs-guest via pinned MadsLorentzen/ai-job-search linkedin-search CLI","location":config["location"],"window_minutes":config["window_minutes"],"design":"broad discovery -> adaptive overlap/retry -> Job-ID dedupe -> relative-age sanity check -> exact detail/closed-status verification -> IT-evidence gate -> explainable scoring -> ChatGPT final fit review","stats":{"query_count":len(config["queries"]),"unique_live_cards":len(cards),**counters,"detail_attempts":attempts,"details_fetched":successes,"verified_new_candidates":len(verified_new),"search_errors":search_errors,"query_retry_recoveries":retries},"query_stats":query_stats,"errors":errors[:30],"filtered_samples":filtered_samples,"new_matches":verified_new}
    save_json(SEEN_PATH, seen); save_json(LATEST_PATH, latest); print(json.dumps({"run_id":latest["run_id"],"health":health,"cards":len(cards),"verified_new_candidates":len(verified_new),"errors":len(errors)}))

if __name__ == "__main__":
    try: main()
    except Exception as exc: print(f"radar failed: {exc}", file=sys.stderr); raise
