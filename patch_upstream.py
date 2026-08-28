#!/usr/bin/env python3
from pathlib import Path
import sys


def replace_once(text, old, new, label):
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def patch_search(path):
    text = path.read_text(encoding="utf-8")
    if 'params.set("sortBy", "DD")' not in text:
        needle = '  params.set("start", String((opts.page - 1) * 10))'
        text = replace_once(text, needle, '  params.set("sortBy", "DD")\n' + needle, "search newest-first patch")
    path.write_text(text, encoding="utf-8")


def patch_helpers(path):
    text = path.read_text(encoding="utf-8")

    if "postedText?: string | null" not in text:
        text = replace_once(text, "  date: string | null\n  url: string\n", "  date: string | null\n  postedText?: string | null\n  url: string\n", "JobCard postedText interface patch")

    if "applicationStatus?: string | null" not in text:
        text = replace_once(text, "  industries: string | null\n}\n", "  industries: string | null\n  applicationStatus?: string | null\n}\n", "JobDetail applicationStatus interface patch")

    if "const postedText" not in text:
        old = '    const dt = chunk.match(/class="job-search-card__listdate[^"]*"[^>]*datetime="([^"]+)"/i)\n    const date = dt ? dt[1] : null\n'
        new = old + r'''    const dtText = chunk.match(/class="job-search-card__listdate[^"]*"[^>]*>([\s\S]*?)<\/time>/i)\n    const postedText = dtText ? clean(dtText[1]) || null : null\n'''.replace('\\n', '\n')
        text = replace_once(text, old, new, "posted relative-time parser patch")
        text = replace_once(text, "      date,\n      url: url || `https://www.linkedin.com/jobs/view/${id}`,\n", "      date,\n      postedText,\n      url: url || `https://www.linkedin.com/jobs/view/${id}`,\n", "postedText return patch")

    if "closed_explicit" not in text:
        needle = "  return {\n    id,\n"
        status_code = '''  const pageText = clean(html).toLowerCase()\n  const rawLower = html.toLowerCase()\n  let applicationStatus = "unknown"\n  if (\n    pageText.includes("no longer accepting applications") ||\n    pageText.includes("applications are closed") ||\n    pageText.includes("job is no longer available") ||\n    pageText.includes("position is no longer available")\n  ) {\n    applicationStatus = "closed_explicit"\n  } else if (\n    rawLower.includes("public_jobs_apply-link") ||\n    rawLower.includes("apply-button") ||\n    pageText.includes("apply now") ||\n    pageText.includes("apply on company website")\n  ) {\n    applicationStatus = "open_signal"\n  }\n\n'''
        text = replace_once(text, needle, status_code + needle, "application status parser patch")
        text = replace_once(text, '    industries: criteria["industries"] ?? null,\n  }\n', '    industries: criteria["industries"] ?? null,\n    applicationStatus,\n  }\n', "applicationStatus return patch")

    path.write_text(text, encoding="utf-8")


def verify(search_path, helpers_path):
    search = search_path.read_text(encoding="utf-8")
    helpers = helpers_path.read_text(encoding="utf-8")
    required = {
        "sortBy newest-first": 'params.set("sortBy", "DD")' in search,
        "postedText interface": "postedText?: string | null" in helpers,
        "postedText parser": "const postedText" in helpers,
        "applicationStatus interface": "applicationStatus?: string | null" in helpers,
        "closed status parser": 'applicationStatus = "closed_explicit"' in helpers,
        "open status parser": 'applicationStatus = "open_signal"' in helpers,
    }
    failed = [name for name, ok in required.items() if not ok]
    if failed:
        raise RuntimeError("Upstream patch verification failed: " + ", ".join(failed))


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: patch_upstream.py <upstream-repo-root>")
    root = Path(sys.argv[1])
    search_path = root / ".agents/skills/linkedin-search/cli/src/commands/search.ts"
    helpers_path = root / ".agents/skills/linkedin-search/cli/src/helpers.ts"
    if not search_path.exists() or not helpers_path.exists():
        raise SystemExit("Pinned LinkedIn CLI files not found")
    patch_search(search_path)
    patch_helpers(helpers_path)
    verify(search_path, helpers_path)
    print("Pinned LinkedIn CLI patched and verified")


if __name__ == "__main__":
    main()
