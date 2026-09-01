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
            'priority_retry_queries': ['IT'],
            'queries': [
                {'query': 'IT', 'pages': 4, 'retry_if_empty': True},
                {'query': 'Unexpected HR Title', 'pages': 2, 'retry_if_empty': True},
            ],
        }
        remote = multilane.prepare_remote_config(base, 'Worldwide')
        self.assertEqual([item['query'] for item in remote['queries']], ['IT', 'Unexpected HR Title'])
        self.assertEqual([item['pages'] for item in remote['queries']], [1, 1])
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
            reported.write_text(json.dumps({'schema_version': 1, 'reported_runs': {}}), encoding='utf-8')
            first_index = multilane.archive_run(payload, pending_path=pending, reported_path=reported, runs_dir=runs)
            self.assertEqual(first_index['runs'][0]['candidate_count'], 1)
            first_part = runs / Path(first_index['runs'][0]['parts'][0]).name
            self.assertTrue(first_part.exists())

            reported.write_text(json.dumps({
                'schema_version': 1,
                'reported_runs': {'20260901T090000Z': '2026-09-01T09:05:00Z'},
            }), encoding='utf-8')
            second_index = multilane.archive_run(second, pending_path=pending, reported_path=reported, runs_dir=runs)
            self.assertEqual([entry['run_id'] for entry in second_index['runs']], ['20260901T100000Z'])
            self.assertFalse(first_part.exists())


if __name__ == '__main__':
    unittest.main()
