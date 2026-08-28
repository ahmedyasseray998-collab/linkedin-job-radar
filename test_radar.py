import unittest
import radar_v4 as radar


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

    def test_freshness_guard(self):
        self.assertTrue(radar.freshness_info({'postedText': '2 hours ago'}, 180)['within_window'])
        self.assertFalse(radar.freshness_info({'postedText': '5 hours ago'}, 180)['within_window'])

    def test_config_validation_and_duplicate_query(self):
        good = {'location': 'Egypt', 'window_minutes': 180, 'queries': [{'query': 'IT', 'pages': 1}], 'role_signals': {}, 'skills': {}}
        self.assertEqual(radar.validate_config(good), [])
        bad = dict(good)
        bad['queries'] = [{'query': 'IT'}, {'query': 'it'}]
        self.assertTrue(any('duplicate query' in e for e in radar.validate_config(bad)))

    def test_it_evidence_gate_rejects_facilities_false_positive(self):
        config = {
            'role_signals': {'IT Infrastructure': {'weight': 8, 'variants': ['it infrastructure', 'infrastructure engineer']}},
            'skills': {'Active Directory': {'weight': 3.5, 'variants': ['active directory']}},
            'negative_signals': {},
        }
        facility = {'title': 'Facility Manager', 'description': 'Oversee maintenance, cleaning, suppliers and workplace safety.', 'jobFunction': 'Management', 'industries': 'Facilities Services'}
        infra = {'title': 'Infrastructure Specialist', 'description': 'Manage Active Directory and Windows infrastructure.', 'jobFunction': 'Information Technology', 'industries': 'IT Services'}
        self.assertFalse(radar.has_it_evidence(radar.score_detail(facility, 2, config)))
        self.assertTrue(radar.has_it_evidence(radar.score_detail(infra, 2, config)))

    def test_hard_exclude_does_not_block_network_security(self):
        config = {'hard_exclude_title': ['software developer', 'security guard']}
        self.assertEqual(radar.hard_excluded('Senior Software Developer', config), ['software developer'])
        self.assertEqual(radar.hard_excluded('Network Security Engineer', config), [])


if __name__ == '__main__':
    unittest.main()
