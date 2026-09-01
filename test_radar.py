import json
import tempfile
import unittest
from pathlib import Path

import radar_multilane as multilane
import radar_v5 as radar


class RadarTests(unittest.TestCase):
    def test_phrase_boundaries(self):
        self.assertTrue(radar.contains_phrase(radar.norm('LAN, VLAN and WAN'), 'lan'))
        self.assertFalse(radar.contains_phrase(radar.norm('freelance designer'), 'lan'))
        self.assertTrue(radar.contains_phrase(radar.norm('IT Intern'), 'intern'))
        self.assertFalse(radar.contains_phrase(radar.norm('International IT Engineer'), 'intern'))

    def test_relative_age(self):
        self.assertEqual(radar.relative_age_minutes('Reposted 2 hours ago'), 120)
        self.assertEqual(radar.relative_age_minutes('35 minutes ago'), 35)
        self.assertEqual(radar.relative_age_minutes('1 day ago'), 1440)
        self.assertIsNone(radar.relative_age_minutes('Today'))

    def test_freshness_is_annotation_not_rejection(self):
        fresh = radar.freshness_info({'postedText': '2 hours ago'}, 180)
        conflict = radar.freshness_info({'postedText': '5 hours ago'}, 180)
        self.assertTrue(fresh['within_requested_window'])
        self.assertFalse(fresh['conflict'])
        self.assertFalse(conflict['within_requested_window'])
        self.assertTrue(conflict['conflict'])

    def test_config_validation_and_duplicate_query(self):
        good = {'location': 'Egypt', 'window_minutes': 180, 'queries': [{'query': 'IT', 'pages': 1}], 'role_signals': {}, 'skills': {}}
        self.assertEqual(radar.validate_config(good), [])
        bad = dict(good)
        bad['queries'] = [{'query': 'IT'}, {'query': 'it'}]
        self.assertTrue(any('duplicate query' in e for e in radar.validate_config(bad)))

    def test_it_evidence_is_advisory_only(self):
        config = {
            'role_signals': {'IT Infrastructure': {'weight': 8, 'variants': ['it infrastructure', 'infrastructure engineer']}},
            'skills': {'Active Directory': {'weight': 3.5, 'variants': ['active directory']}},
            'negative_signals': {},
        }
        facility = {'title': 'Facility Manager', 'description': 'Oversee maintenance, cleaning, suppliers and workplace safety.', 'jobFunction': 'Management', 'industries': 'Facilities Services'}
        infra = {'title': 'Infrastructure Specialist', 'description': 'Manage Active Directory and Windows infrastructure.', 'jobFunction': 'Information Technology', 'industries': 'IT Services'}
        self.assertFalse(radar.has_it_evidence(radar.score_detail(facility, 2, config)))
        self.assertTrue(radar.has_it_evidence(radar.score_detail(infra, 2, config)))

    def test_title_noise_is_annotation_only(self):
        config = {'hard_exclude_title': ['software developer', 'security guard']}
        self.assertEqual(radar.advisory_title_signals('Senior Software Developer', config), ['software developer'])
        self.assertEqual(radar.advisory_title_signals('Network Security Engineer', config), [])

    def test_closed_status_is_representable(self):
        detail = {'applicationStatus': 'closed_explicit'}
        self.assertEqual(detail['applicationStatus'], 'closed_explicit')

    def test_remote_config_keeps_broad_queries(self):
        base = {
            'remote_pages_per_query': 1,
            'max_detail_fetches': 500,
            'priority_retry_queries': ['IT'],
            'queries': [
                {'query': 'IT', 'pages': 4, 'retry_if_empty': True},
                {'query': 'Unexpected HR Title', 'pages': 2, 'retry_if_empty': True},
            ],
        }
        remote = multilane.prepare_remote_config(base, 'Worldwide')
        self.assertEqual([item['query'] for item in remote['queries']], ['IT', 'Unexpected HR Title'])
        self.assertEqual([item['pages'] for item in remote['queries']], [1, 1])
        self.assertEqual(remote['max_detail_fetches'], 500)
        self.assertTrue(remote['queries'][0]['retry_if_empty'])
        self.assertFalse(remote['queries'][1]['retry_if_empty'])

    def test_lane_health_detects_repeated_silent_empty(self):
        definitions = [{'name': 'remote_worldwide', 'location': 'Worldwide', 'remote_filter': 'remote'}]
        payloads = [{'stats': {'unique_live_cards': 0, 'search_errors': 0}}]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'lane_health.json'
            degraded = False
            for hour in range(3):
                state, degraded = multilane.update_lane_health(
                    definitions, payloads, f'2026-09-01T0{hour}:00:00Z', 3, path=path,
                )
            self.assertTrue(degraded)
            self.assertEqual(state['lanes']['remote_worldwide']['status'], 'degraded_empty')

    def test_report_queue_survives_until_acknowledged(self):
        payload = {
            'run_id': '20260901T090000Z',
            'generated_at_utc': '2026-09-01T09:03:00Z',
            'health': 'healthy',
            'warnings': [],
            'review_candidates': [{'linkedin_job_id': '123', 'title': 'Mystery IT Officer'}],
        }
        second = dict(payload, run_id='20260901T100000Z', generated_at_utc='2026-09-01T10:03:00Z', review_candidates=[])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pending = root / 'pending_runs.json'
            reported = root / 'reported_runs.json'
            runs = root / 'runs'
            delivery = root / 'delivery'
            reported.write_text(json.dumps({
                'schema_version': 2, 'reported_runs': {}, 'reported_parts': {},
            }), encoding='utf-8')
            first_index = multilane.archive_run(
                payload, pending_path=pending, reported_path=reported,
                runs_dir=runs, delivery_dir=delivery,
            )
            self.assertEqual(first_index['runs'][0]['candidate_count'], 1)
            first_part = runs / Path(first_index['runs'][0]['parts'][0]).name
            first_delivery = delivery / Path(first_index['runs'][0]['delivery_parts'][0]['path']).name
            self.assertTrue(first_part.exists())
            self.assertTrue(first_delivery.exists())

            reported.write_text(json.dumps({
                'schema_version': 2,
                'reported_runs': {'20260901T090000Z': '2026-09-01T09:05:00Z'},
                'reported_parts': {},
            }), encoding='utf-8')
            second_index = multilane.archive_run(
                second, pending_path=pending, reported_path=reported,
                runs_dir=runs, delivery_dir=delivery,
            )
            self.assertEqual([entry['run_id'] for entry in second_index['runs']], ['20260901T100000Z'])
            self.assertFalse(first_part.exists())
            self.assertFalse(first_delivery.exists())

    def test_partial_part_ack_resumes_without_repeating(self):
        payload = {
            'run_id': '20260901T090000Z',
            'generated_at_utc': '2026-09-01T09:03:00Z',
            'health': 'healthy',
            'warnings': [],
            'review_candidates': [
                {'linkedin_job_id': str(index), 'title': f'Mystery Role {index}', 'description': 'Manage infrastructure.'}
                for index in range(3)
            ],
        }
        next_payload = dict(
            payload, run_id='20260901T100000Z', generated_at_utc='2026-09-01T10:03:00Z',
            review_candidates=[],
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pending, reported = root / 'pending.json', root / 'reported.json'
            runs, delivery = root / 'runs', root / 'delivery'
            reported.write_text(json.dumps({
                'schema_version': 2, 'reported_runs': {}, 'reported_parts': {},
            }), encoding='utf-8')
            first = multilane.archive_run(
                payload, chunk_size=2, pending_path=pending, reported_path=reported,
                runs_dir=runs, delivery_dir=delivery,
            )
            acknowledged = first['runs'][0]['delivery_parts'][0]['part_id']
            remaining = first['runs'][0]['delivery_parts'][1]['part_id']
            reported.write_text(json.dumps({
                'schema_version': 2, 'reported_runs': {},
                'reported_parts': {acknowledged: '2026-09-01T09:05:00Z'},
            }), encoding='utf-8')
            second = multilane.archive_run(
                next_payload, chunk_size=2, pending_path=pending, reported_path=reported,
                runs_dir=runs, delivery_dir=delivery,
            )
            old = next(entry for entry in second['runs'] if entry['run_id'] == payload['run_id'])
            self.assertEqual([part['part_id'] for part in old['delivery_parts']], [remaining])
            self.assertEqual(old['candidate_count'], 1)
            self.assertFalse((delivery / f'{acknowledged}.json').exists())
            self.assertTrue((delivery / f'{remaining}.json').exists())

    def test_legacy_pending_run_migrates_to_compact_delivery_without_loss(self):
        legacy_run = '20260901T090000Z'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pending, reported = root / 'pending.json', root / 'reported.json'
            runs, delivery = root / 'runs', root / 'delivery'
            runs.mkdir()
            source_name = f'{legacy_run}-part-001.json'
            (runs / source_name).write_text(json.dumps({
                'schema_version': 1,
                'run_id': legacy_run,
                'generated_at_utc': '2026-09-01T09:03:00Z',
                'health': 'healthy',
                'warnings': [],
                'part': 1,
                'part_count': 1,
                'review_candidates': [{
                    'linkedin_job_id': '123',
                    'title': 'Unusual Technology Custodian',
                    'description': 'Maintain Windows Server and Active Directory.',
                }],
            }), encoding='utf-8')
            pending.write_text(json.dumps({
                'schema_version': 1,
                'updated_at_utc': '2026-09-01T09:03:00Z',
                'runs': [{
                    'run_id': legacy_run,
                    'generated_at_utc': '2026-09-01T09:03:00Z',
                    'health': 'healthy',
                    'warnings': [],
                    'candidate_count': 1,
                    'parts': [source_name],
                }],
            }), encoding='utf-8')
            reported.write_text(json.dumps({
                'schema_version': 1, 'reported_runs': {},
            }), encoding='utf-8')
            new_payload = {
                'run_id': '20260901T100000Z',
                'generated_at_utc': '2026-09-01T10:03:00Z',
                'health': 'healthy',
                'warnings': [],
                'review_candidates': [],
            }
            index = multilane.archive_run(
                new_payload, pending_path=pending, reported_path=reported,
                runs_dir=runs, delivery_dir=delivery,
            )
            migrated = next(entry for entry in index['runs'] if entry['run_id'] == legacy_run)
            self.assertEqual(migrated['candidate_count'], 1)
            compact_path = delivery / Path(migrated['delivery_parts'][0]['path']).name
            compact = json.loads(compact_path.read_text(encoding='utf-8'))
            self.assertEqual(compact['review_candidates'][0]['linkedin_job_id'], '123')
            self.assertTrue((runs / source_name).exists())

    def test_compact_record_keeps_weird_title_evidence_and_full_source(self):
        candidate = {
            'linkedin_job_id': '123',
            'title': 'Technology Happiness Officer',
            'company': 'Example',
            'location': 'Cairo, Egypt',
            'description': 'Own Active Directory, VMware, routing, backups, and infrastructure operations.',
            'role_hits_title': [],
            'role_hits_description': [{'label': 'IT Infrastructure', 'weight': 8}],
            'skill_hits': [{'label': 'Active Directory', 'weight': 3.5}],
            'matched_queries': [{'query': 'IT'}],
        }
        compact = multilane.compact_candidate(candidate, 'output/runs/source.json', 900)
        self.assertEqual(compact['title'], 'Technology Happiness Officer')
        self.assertIn('Active Directory', compact['description_excerpt'])
        self.assertEqual(compact['full_record_part'], 'output/runs/source.json')
        self.assertEqual(compact['remote_eligibility']['status'], 'explicit_egypt_emea_or_global_signal')

    def test_large_compact_batch_preserves_every_id_and_reduces_payload(self):
        candidates = [{
            'linkedin_job_id': str(index),
            'title': f'Role {index}',
            'description': ('Company boilerplate. ' * 250) + 'Manage Windows Server and network infrastructure.',
        } for index in range(500)]
        compact = [
            multilane.compact_candidate(candidate, 'output/runs/source.json', 900)
            for candidate in candidates
        ]
        self.assertEqual(
            {item['linkedin_job_id'] for item in compact},
            {item['linkedin_job_id'] for item in candidates},
        )
        self.assertLess(
            len(json.dumps(compact)),
            len(json.dumps(candidates)) // 2,
        )


if __name__ == '__main__':
    unittest.main()
