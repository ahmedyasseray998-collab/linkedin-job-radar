"""Refresh derived queue status without claiming an unverified publication."""
from __future__ import annotations
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from queue_integrity import atomic_write_json, read_json

ROOT = Path(__file__).resolve().parent
REVIEW_CONTRACT = {
    'version': 16,
    'preflight': ['Read output/pending_runs.json and current state/reported_runs.json before opening packets.',
                  'Skip acknowledged part IDs and Job IDs. Verify every manifest and read every remaining candidate.',
                  'Egypt first; keep broad Egypt interpretation. Do not use advisory scores as final fit scores.',
                  'Report each actionable role with GPT fit score /100, exact LinkedIn Job ID, gaps and restrictions.',
                  'Show every processed run completion date/time in Africa/Cairo.',
                  'Keep strong remote_eligibility_unconfirmed leads for actual eligibility review; do not call them Egypt-eligible without evidence.',
                  'Never acknowledge unread or partially reviewed parts. Merge receipts against current main.'],
    'decision_log': 'state/review_decisions.json',
    'decision_fields': ['linkedin_job_id', 'decision', 'fit_score_100', 'priority_lane', 'reason', 'reviewed_at_utc', 'source_part_id'],
    'note': 'The native ChatGPT task must honor this contract; repository metadata does not change its schedule or notification settings.'
}


def synchronize(root: Path = ROOT):
    pending = read_json(root / 'output/pending_runs.json', {'runs': [], 'backlog': {}, 'integrity': {}})
    counts = pending.get('backlog', {})
    now = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    pending['reviewer_contract'] = REVIEW_CONTRACT
    atomic_write_json(root / 'output/pending_runs.json', pending)
    latest_path = root / 'output/latest.json'
    latest = read_json(latest_path, {})
    latest.setdefault('scan_time_backlog', dict(latest.get('delivery_queue') or {}))
    latest.setdefault('delivery_queue', {}).update(counts)
    latest['delivery_queue']['integrity'] = pending.get('integrity', {})
    latest['delivery_queue']['as_of_utc'] = now
    components = latest.setdefault('health_components', {})
    components['queue'] = ('degraded_integrity' if pending.get('integrity', {}).get('status') != 'validated'
                           else 'healthy_with_backlog_warning' if counts.get('warning') else 'healthy')
    # A file cannot prove its own push succeeded. Use the actual workflow result,
    # not a fabricated green publish flag stored before git push.
    components['publish'] = 'verify_workflow_conclusion'
    repo = os.getenv('GITHUB_REPOSITORY', 'ahmedyasseray998-collab/linkedin-job-radar')
    run = os.getenv('GITHUB_RUN_ID')
    if run:
        components['publication_workflow_url'] = f'https://github.com/{repo}/actions/runs/{run}'
    components['publish_source_of_truth'] = 'GitHub Actions conclusion plus published main commit'
    latest['queue_status_updated_at_utc'] = now
    latest['overall_pre_publish_health'] = ('degraded' if any(str(v).startswith('degraded') for v in components.values())
                                             else 'healthy_with_warnings' if latest.get('warnings') or counts.get('warning')
                                             else 'healthy')
    atomic_write_json(latest_path, latest)
    summary_path = root / 'output/deferred_summary.json'
    summary = read_json(summary_path, {})
    summary.setdefault('scan_time_backlog', dict(summary.get('pending_backlog_after_targeting') or {}))
    summary['pending_backlog_after_targeting'] = dict(counts)
    summary['queue_status_updated_at_utc'] = now
    atomic_write_json(summary_path, summary)
    return {'backlog': counts, 'queue_health': components['queue']}


def reconcile(root: Path = ROOT):
    # Promote receipts/retarget before cleanup, including when the policy changes.
    import radar_pipeline_v15 as pipeline
    import targeted_queue_v15 as targeting
    from radar_policy_v16 import installed
    from ack_reconcile_v15 import reconcile_acknowledgements
    with installed():
        targeting.prepare_seen_for_run(root)
        pipeline.retarget_existing_queue_before_scan(root)
    result = reconcile_acknowledgements(root)
    return {**result, **synchronize(root)}


if __name__ == '__main__':
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument('--ack', action='store_true')
    args = parser.parse_args()
    print(json.dumps(reconcile() if args.ack else synchronize()))
