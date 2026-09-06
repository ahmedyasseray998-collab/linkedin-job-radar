"""Detect a stalled review consumer without changing searches or task settings."""
from __future__ import annotations
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

MARKER = '<!-- linkedin-radar-reviewer-watchdog-v1 -->'


def parse_time(value):
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc) if dt.tzinfo else None
    except (ValueError, TypeError):
        return None


def evaluate(pending, reported, now=None, limit_hours=2):
    now = now or datetime.now(timezone.utc)
    part_receipts = reported.get('reported_parts') or {}
    run_receipts = reported.get('reported_runs') or {}
    job_receipts = reported.get('reported_jobs') or {}
    active_ids, unindexed_count, times = set(), 0, []
    for run in pending.get('runs', []):
        if run.get('run_id') in run_receipts:
            continue
        run_pending = False
        for part in run.get('delivery_parts', []):
            if part.get('part_id') in part_receipts:
                continue
            ids = [str(i) for i in part.get('job_ids', [])]
            if ids:
                unseen = set(ids) - set(job_receipts)
                active_ids.update(unseen)
                run_pending = run_pending or bool(unseen)
            else:
                count = int(part.get('candidate_count', 0))
                unindexed_count += count
                run_pending = run_pending or count > 0
        if run_pending:
            stamp = parse_time(run.get('generated_at_utc'))
            if stamp:
                times.append(stamp)
    count = len(active_ids) + unindexed_count
    # File-maintenance timestamps are not proof of an actual completed review.
    values = list(part_receipts.values()) + list(job_receipts.values())
    valid = [t for v in values if (t := parse_time(v)) is not None and t <= now + timedelta(minutes=5)]
    newest = max(valid, default=None)
    oldest = min(times, default=None)
    grace = timedelta(hours=limit_hours)
    stalled = bool(count and (oldest is None or now - oldest >= grace) and (newest is None or now - newest >= grace))
    recent = bool(newest and now - newest < grace)
    status = 'stalled_review_receipts' if stalled else ('idle' if not count else ('recent_review_receipts' if recent else 'awaiting_review_grace'))
    return {'status': status,
            'pending_unique_jobs': count, 'last_review_receipt_utc': newest.isoformat() if newest else None,
            'oldest_pending_batch_utc': oldest.isoformat() if oldest else None,
            'checked_at_utc': now.isoformat(), 'threshold_hours': limit_hours,
            'native_task_enabled_state': 'not_accessible_from_github'}


def github(path, data=None, method=None):
    token = os.environ['GH_TOKEN']
    repo = os.environ['GITHUB_REPOSITORY']
    url = 'https://api.github.com/repos/' + repo + '/' + path
    request = Request(url, data=None if data is None else json.dumps(data).encode(), method=method,
                      headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
                               'X-GitHub-Api-Version': '2022-11-28', 'Content-Type': 'application/json'})
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def main():
    pending = json.loads(Path('output/pending_runs.json').read_text())
    reported = json.loads(Path('state/reported_runs.json').read_text())
    result = evaluate(pending, reported)
    print(json.dumps(result))
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as out:
            out.write('## Reviewer receipt watchdog\n```json\n' + json.dumps(result, indent=2) + '\n```\n')
    issue = None
    for page in range(1, 21):
        items = github(f'issues?state=all&per_page=100&page={page}')
        issue = next((i for i in items if 'pull_request' not in i and MARKER in (i.get('body') or '')), None)
        if issue or len(items) < 100:
            break
    else:
        raise RuntimeError('Issue search exceeded safety limit; refusing to create a possible duplicate alert')
    if result['status'] == 'stalled_review_receipts':
        body = (MARKER + '\nPending jobs exist, but no recent per-job or per-part review receipt was found. '
                'This does not prove whether the native ChatGPT task is paused, blocked, or failing.\n\n'
                'Open ChatGPT Scheduled, select LinkedIn Radar Fit Scores, and inspect/resume the existing reviewer. '
                'Keep duplicate old tasks paused. A GitHub scan or this warning cannot resume a native ChatGPT task.\n\n'
                'Do not acknowledge unread jobs or narrow Egypt search to clear the backlog.\n\n'
                '```json\n' + json.dumps(result, indent=2) + '\n```')
        payload = {'title': 'Radar reviewer needs attention: no recent review receipts', 'body': body}
        if issue:
            github(f"issues/{issue['number']}", dict(payload, state='open'), 'PATCH')
        else:
            github('issues', payload, 'POST')
    elif issue and issue.get('state') == 'open' and result['status'] in {'idle', 'recent_review_receipts'}:
        # Closure means receipt flow recovered, not that task configuration was verified.
        body = MARKER + '\nReview receipts are recent or the effective queue is empty. Native ChatGPT task state remains unverified.\n\n```json\n' + json.dumps(result, indent=2) + '\n```'
        github(f"issues/{issue['number']}", {'state': 'closed', 'body': body}, 'PATCH')


if __name__ == '__main__':
    main()
