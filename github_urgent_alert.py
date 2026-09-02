#!/usr/bin/env python3
"""Build a broad, zero-LLM GitHub alert for the latest radar run."""

import argparse
import json
import re
from pathlib import Path


EGYPT_LOCATIONS = (
    "egypt", "cairo", "giza", "new cairo", "nasr city", "maadi",
    "heliopolis", "6th of october", "october city", "sheikh zayed",
    "smart village", "badr city",
)
REMOTE_SIGNALS = (
    "worldwide", "work from anywhere", "anywhere in the world",
    "global remote", "remote across emea", "remote within emea",
    "based in emea", "remote - emea", "remote, emea", "remote emea",
    "remote from egypt", "egypt remote",
)
IT_TERMS = (
    "infrastructure", "system administrator", "systems administrator",
    "system engineer", "systems engineer", "network", "windows server",
    "active directory", "vmware", "hyper-v", "forti", "cisco", "linux",
    "veeam", "backup", "disaster recovery", "microsoft 365", "office 365",
    "powershell", "it support", "technical support", "desktop support",
    "endpoint", "cloud engineer", "azure", "security engineer", "soc ",
)


def clean(value):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def labels(candidate, key):
    values = candidate.get(key) or []
    return [item.get("label") if isinstance(item, dict) else str(item) for item in values]


def candidate_text(candidate):
    return " ".join([
        str(candidate.get("title") or ""),
        str(candidate.get("location") or ""),
        str(candidate.get("description_excerpt") or candidate.get("description") or ""),
    ]).casefold()


def group(candidate):
    text = candidate_text(candidate)
    location = str(candidate.get("location") or "").casefold()
    if any(term in location for term in EGYPT_LOCATIONS):
        return 0
    if any(term in text for term in REMOTE_SIGNALS):
        return 0
    evidence = (
        labels(candidate, "role_hits_title")
        + labels(candidate, "role_hits_description")
        + labels(candidate, "skill_hits")
    )
    if evidence or any(term in text for term in IT_TERMS):
        return 1
    return 2


def line(candidate):
    job_id = clean(candidate.get("linkedin_job_id") or "unknown")
    title = clean(candidate.get("title") or "Untitled role")
    company = clean(candidate.get("company") or "Unknown company")
    location = clean(candidate.get("location") or "Location not stated")
    posted = clean(candidate.get("linkedin_posted_text") or candidate.get("linkedin_date") or "Posting time unavailable")
    url = str(candidate.get("url") or "").strip()
    label = f"{title} — {company}"
    linked = f"[{label}]({url})" if url.startswith(("https://", "http://")) else label
    return f"- {linked} · {location} · {posted} · Job ID `{job_id}`"


def build(payload):
    candidates = payload.get("review_candidates") or payload.get("new_matches") or []
    if not candidates:
        return ""

    grouped = {0: [], 1: [], 2: []}
    for candidate in candidates:
        grouped[group(candidate)].append(candidate)
    for items in grouped.values():
        items.sort(key=lambda item: (
            -(float(item.get("score") or 0)),
            float(item.get("estimated_age_minutes") or 10**9),
            str(item.get("title") or "").casefold(),
        ))

    run_id = clean(payload.get("run_id") or "unknown")
    generated = clean(payload.get("generated_at_utc") or "unknown time")
    warnings = [clean(item) for item in payload.get("warnings") or []]
    parts = [
        f"## Radar finished: {len(candidates)} new Job IDs",
        "",
        f"Run `{run_id}` completed at **{generated}**. This is the immediate broad alert; GPT will still provide the detailed hourly fit report in ChatGPT.",
        "",
    ]
    headings = {
        0: "### Egypt or explicit Egypt/EMEA/worldwide-remote signals",
        1: "### Other IT/infrastructure candidates",
        2: "### Noisy or ambiguous titles retained to avoid misses",
    }
    for key in (0, 1, 2):
        if not grouped[key]:
            continue
        parts.extend([headings[key], ""])
        parts.extend(line(candidate) for candidate in grouped[key])
        parts.append("")
    if warnings:
        parts.append("Scan warning: " + "; ".join(warnings))
    parts.append("All links came from the live GitHub radar and exact LinkedIn Job IDs; no GPT tokens were used for this immediate alert.")
    return "\n".join(parts).rstrip() + "\n"


def load_latest_delivery(latest_path, pending_path):
    latest = json.loads(Path(latest_path).read_text(encoding="utf-8"))
    pending = json.loads(Path(pending_path).read_text(encoding="utf-8"))
    runs = pending.get("runs") or []
    if not runs:
        return dict(latest, review_candidates=[])
    run_id = str(latest.get("run_id") or "")
    entry = next((item for item in reversed(runs) if str(item.get("run_id")) == run_id), runs[-1])
    candidates = []
    root = Path(pending_path).resolve().parent.parent
    for part in entry.get("delivery_parts") or []:
        reference = Path(str(part.get("path") or ""))
        path = reference if reference.is_absolute() else root / reference
        if not path.is_file():
            continue
        delivery = json.loads(path.read_text(encoding="utf-8"))
        candidates.extend(delivery.get("review_candidates") or [])
    return dict(latest, review_candidates=candidates)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest", default="output/latest.json")
    parser.add_argument("--pending", default="output/pending_runs.json")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    payload = load_latest_delivery(args.latest, args.pending)
    Path(args.output).write_text(build(payload), encoding="utf-8")


if __name__ == "__main__":
    main()
