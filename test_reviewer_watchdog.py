import unittest
from datetime import datetime, timezone
from reviewer_watchdog import evaluate


class WatchdogTests(unittest.TestCase):
    now = datetime(2026, 9, 6, 18, tzinfo=timezone.utc)

    def pending(self):
        return {'runs': [{'run_id': 'r1', 'generated_at_utc': '2026-09-06T10:00:00Z', 'delivery_parts': [{'part_id': 'p1', 'candidate_count': 1, 'job_ids': ['j1']}]}]}

    def test_stale_review_alerts(self):
        self.assertEqual(evaluate(self.pending(), {'reported_jobs': {'other': '2026-09-04T10:00:00Z'}}, self.now)['status'], 'stalled_review_receipts')

    def test_recent_receipt(self):
        self.assertEqual(evaluate(self.pending(), {'reported_jobs': {'other': '2026-09-06T17:00:00Z'}}, self.now)['status'], 'recent_review_receipts')

    def test_acknowledged_job_is_not_pending(self):
        self.assertEqual(evaluate(self.pending(), {'reported_jobs': {'j1': '2026-09-04T10:00:00Z'}}, self.now)['status'], 'idle')

    def test_acknowledged_part_is_not_pending(self):
        self.assertEqual(evaluate(self.pending(), {'reported_parts': {'p1': '2026-09-04T10:00:00Z'}}, self.now)['status'], 'idle')

    def test_maintenance_timestamp_not_review(self):
        self.assertEqual(evaluate(self.pending(), {'updated_at_utc': '2026-09-06T18:00:00Z'}, self.now)['status'], 'stalled_review_receipts')

    def test_empty_queue(self):
        self.assertEqual(evaluate({'runs': []}, {}, self.now)['status'], 'idle')

    def test_fresh_queue_grace(self):
        pending = self.pending()
        pending['runs'][0]['generated_at_utc'] = '2026-09-06T17:00:00Z'
        self.assertEqual(evaluate(pending, {}, self.now)['status'], 'awaiting_review_grace')

    def test_future_timestamp_not_health(self):
        self.assertEqual(evaluate(self.pending(), {'reported_jobs': {'other': '2027-01-01T00:00:00Z'}}, self.now)['status'], 'stalled_review_receipts')


if __name__ == '__main__':
    unittest.main()
