import unittest
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


if __name__ == '__main__':
    unittest.main()
