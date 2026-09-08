import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from queue_integrity import atomic_write_json, job_ids_digest, refresh_part_payload
from reviewer_plan import build_plan, digest

NOW = datetime(2026, 9, 7, 19, tzinfo=timezone.utc)


class ReviewPlanTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.pending = {'schema_version': 3, 'runs': []}
        self.ledger = {'reported_parts': {}, 'reported_jobs': {}, 'reported_runs': {}}

    def add_part(self, ids, lane='local', age=1, title='System Administrator', padding=0):
        ordinal = len(self.pending['runs'])
        run_id = f'run-{ordinal}'
        part_id = f'{run_id}-part-001-{job_ids_digest(ids)[:12]}'
        date = (NOW - timedelta(hours=age)).isoformat()
        candidates = [{'linkedin_job_id': jid, 'title': title, 'company': 'Example',
                       'location': 'Cairo, Egypt' if lane == 'local' else 'EMEA',
                       'discovery_lane': 'egypt' if lane == 'local' else 'remote_emea',
                       'first_seen_utc': date,
                       'description_excerpt': 'Administer Windows Server and VMware. ' + 'x' * padding}
                      for jid in ids]
        payload = refresh_part_payload({'schema_version': 4, 'part_id': part_id, 'run_id': run_id}, candidates)
        path = f'output/delivery/{part_id}.json'
        atomic_write_json(self.root / path, payload)
        meta = {'part_id': part_id, 'path': path, 'delivery_tier': lane, 'candidate_count': len(ids),
                'job_ids': ids, 'job_ids_sha256': job_ids_digest(ids)}
        self.pending['runs'].append({'run_id': run_id, 'generated_at_utc': date,
                                     'run_finished_at_utc': date, 'delivery_parts': [meta]})
        return meta, payload

    def plan(self):
        return build_plan(self.root, self.pending, self.ledger, NOW)

    def test_all_egypt_survives_both_old_limits_and_leaves_separate_international_budget(self):
        for n in range(65):
            self.add_part([str(n)], padding=1800)
        for n in range(65, 115):
            self.add_part([str(n)], lane='remote')
        plan = self.plan()
        self.assertEqual(plan['egypt']['required_unique_job_count'], 65)
        self.assertEqual(plan['egypt']['required_part_count'], 65)
        self.assertGreater(plan['egypt']['compact_chars'], 100_000)
        self.assertIsNone(plan['egypt']['candidate_limit'])
        self.assertIsNone(plan['egypt']['compact_character_limit'])
        self.assertIsNone(plan['egypt']['deep_check_limit'])
        self.assertEqual(plan['international']['selected_unique_job_count'], 40)

    def test_old_legacy_run_receipt_does_not_hide_new_egypt_parts(self):
        meta, _ = self.add_part(['1'])
        self.ledger['reported_runs']['run-0'] = 'legacy'
        self.assertEqual(self.plan()['egypt']['parts'][0]['part_id'], meta['part_id'])

    def test_mislabeled_remote_part_containing_egypt_is_mandatory(self):
        meta, _ = self.add_part(['1'])
        meta['delivery_tier'] = 'remote'
        self.assertEqual(self.plan()['egypt']['required_unique_job_count'], 1)
        self.assertEqual(self.plan()['international']['selected_unique_job_count'], 0)

    def test_counts_use_unique_ids_and_exclude_acknowledged_jobs(self):
        self.add_part(['1', '2'])
        self.add_part(['1', '3'])
        self.ledger['reported_jobs']['2'] = 'reviewed'
        plan = self.plan()
        self.assertEqual(plan['counts']['unique_pending_jobs'], 2)
        self.assertEqual(plan['egypt']['required_unique_job_count'], 2)
        self.assertEqual(plan['egypt']['required_part_count'], 2)

    def test_acknowledged_parts_are_not_read(self):
        meta, _ = self.add_part(['1'])
        self.ledger['reported_parts'][meta['part_id']] = 'reviewed'
        (self.root / meta['path']).unlink()
        self.assertEqual(self.plan()['counts']['unique_pending_jobs'], 0)

    def test_priority_favors_fresh_relevant_work_without_excluding_noise(self):
        old, _ = self.add_part(['1'], age=72)
        noise, _ = self.add_part(['2'], title='Civil BIM Infrastructure Engineer')
        fresh, _ = self.add_part(['3'], age=2)
        ids = [p['part_id'] for p in self.plan()['egypt']['parts']]
        self.assertEqual(ids, [fresh['part_id'], old['part_id'], noise['part_id']])

    def test_international_selection_fills_capacity_instead_of_stopping_at_six_parts(self):
        for n in range(48):
            self.add_part([str(n)], lane='relocation' if n % 3 == 0 else 'remote', age=n + 1)
        selected = self.plan()['international']
        self.assertEqual(selected['selected_unique_job_count'], 40)
        self.assertGreater(selected['selected_part_count'], 6)
        self.assertIn('relocation', {p['lane'] for p in selected['parts']})
        self.assertIn('47', {i for p in selected['parts'] for i in p['job_ids']})

    def test_whole_parts_are_kept_and_smaller_fitting_parts_are_considered(self):
        self.add_part(['oversized'], lane='remote', padding=110_000)
        self.add_part(['a', 'b'], lane='remote')
        selected = self.plan()['international']
        self.assertEqual(selected['selected_unique_job_count'], 2)
        self.assertEqual(selected['parts'][0]['job_ids'], ['a', 'b'])

    def test_integrity_failure_cannot_publish_a_partial_success_plan(self):
        meta, payload = self.add_part(['1'])
        payload['review_candidates'][0]['linkedin_job_id'] = 'unseen'
        atomic_write_json(self.root / meta['path'], payload)
        with self.assertRaises(ValueError):
            self.plan()

    def test_source_digests_bind_plan_to_queue_and_ledger(self):
        self.add_part(['1'])
        plan = self.plan()
        self.assertEqual(plan['source_queue_sha256'], digest(self.pending))
        self.assertEqual(plan['source_ledger_sha256'], digest(self.ledger))


if __name__ == '__main__':
    unittest.main()
