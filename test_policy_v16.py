import copy
import tempfile
import unittest
from pathlib import Path
import radar_pipeline_v13 as search
import radar_pipeline_v14 as orchestrator
import targeted_queue_v15 as old
import radar_policy_v16 as policy
from radar_pipeline_v16 import catchup_config, run_scope
from radar_maintenance_v16 import synchronize
from queue_integrity import atomic_write_json, read_json


def candidate(description='', location='London, United Kingdom', title='System Administrator', skills=None):
    return {'linkedin_job_id': '123', 'title': title, 'company': 'Example', 'location': location,
            'description': description, 'discovery_lane': 'remote_emea', 'advisory_it_evidence': True,
            'role_hits_title': [{'label': 'Systems Administration'}],
            'role_hits_description': [{'label': 'Systems Administration'}],
            'skill_hits': [{'label': s} for s in (skills if skills is not None else ['Windows Server', 'Active Directory', 'VMware', 'Veeam', 'Cisco', 'Linux'])],
            'negative_hits': [], 'matched_queries': [], 'score': 30, 'application_status': 'unknown'}


class Policy16Tests(unittest.TestCase):
    def test_egypt_sparse_and_weird_titles_unchanged(self):
        for title, skills in [('IT Officer', []), ('Senior Executive Assistant', ['Google Workspace']), ('Electrical Tender Engineer', ['Backup/DR'])]:
            c = candidate('', 'Cairo, Egypt', title, skills)
            c['discovery_lane'] = 'egypt'
            c['role_hits_title'] = []
            c['role_hits_description'] = []
            expected = old.classify_candidate(copy.deepcopy(c))
            with policy.installed():
                self.assertEqual(policy.classify(c), expected)

    def test_genuine_emea_remote_not_dropped(self):
        with policy.installed():
            self.assertEqual(policy.classify(candidate('Fully remote EMEA role, open to applicants based in Egypt.'))[0], 'remote')

    def test_explicit_remote_location_is_valid_evidence(self):
        with policy.installed():
            c = candidate('Maintain Windows servers.', 'EMEA', 'System Administrator (Remote)')
            self.assertEqual(policy.annotation(c)['scope'], 'emea')
            self.assertEqual(policy.classify(c)[0], 'remote')

    def test_uncertain_remote_is_not_silently_lost_or_claimed_eligible(self):
        with policy.installed():
            c = candidate('This role is fully remote. Administer Windows and VMware.', 'Germany')
            self.assertEqual(policy.classify(c), ('remote', 'strong_remote_eligibility_unconfirmed_requires_gpt'))
            self.assertEqual(policy.annotation(c)['status'], 'requires_full_review')

    def test_country_limited_remote_blocked(self):
        for wording in ['Remote within the Republic of Ireland or the United Kingdom.', 'Work from anywhere in Georgia.', 'Work from anywhere in India.', 'Work from anywhere in South Africa.']:
            with self.subTest(wording=wording), policy.installed():
                c = candidate('This role is fully remote. ' + wording)
                self.assertIsNone(policy.classify(c)[0])

    def test_nationality_and_clearance_blockers(self):
        for wording in ['Only EU nationals are eligible.', 'British nationals only.', 'Security Clearance: British National', 'Active TS/SCI clearance required.']:
            with self.subTest(wording=wording), policy.installed():
                self.assertIsNone(policy.classify(candidate(wording))[0])

    def test_no_relocation_and_no_sponsorship(self):
        for wording in ['Relocation from other countries will not be available for this role!', 'We are currently unable to support visa sponsorship for H1B holders.', 'No visa sponsorship.']:
            with self.subTest(wording=wording), policy.installed():
                self.assertIsNone(policy.classify(candidate(wording))[0])

    def test_onsite_wording_detected(self):
        for wording in ['Location: London - Onsite', 'Monaghan 5 days onsite', 'Modalita: ON SITE per i primi 2 mesi, ibrido successivamente', 'Work regime: Full-time, Hybrid']:
            with self.subTest(wording=wording), policy.installed():
                self.assertIn(policy.work_model(candidate(wording))[0], {'onsite', 'hybrid'})

    def test_remote_support_is_not_remote_work(self):
        with policy.installed():
            self.assertEqual(policy.work_model(candidate('Provide remote diagnostic technical support for customers.'))[0], 'unknown')

    def test_negated_relocation_is_not_positive_support(self):
        self.assertEqual(policy.mobility(candidate('No relocation assistance.'))['status'], 'not_offered_or_restricted')

    def test_real_sponsorship_kept(self):
        with policy.installed():
            c = candidate('This position is on-site. We offer visa sponsorship and relocation assistance.')
            self.assertEqual(policy.classify(c), ('relocation', 'explicit_visa_or_relocation_support'))

    def test_ordinary_foreign_support_not_speculative_relocation(self):
        with policy.installed():
            c = candidate('Help users with computers.', skills=['Active Directory', 'Microsoft 365'])
            self.assertIsNone(policy.classify(c)[0])

    def test_strong_foreign_fit_remains_unconfirmed_relocation(self):
        with policy.installed():
            self.assertEqual(policy.classify(candidate('Maintain Windows and VMware.')), ('relocation', 'relocation_possible_sponsorship_unconfirmed'))

    def test_full_description_footer_is_checked(self):
        with policy.installed():
            c = candidate('Company information. ' * 650 + ' Only EU nationals are eligible.')
            self.assertIsNone(policy.classify(c)[0])

    def test_compact_priority_matches_final_tier(self):
        with policy.installed():
            c = candidate('Maintain Windows Server and VMware. Requirements: 4 years experience.')
            c['delivery_tier'] = 'relocation'
            compact = policy.compact(c, 'output/runs/example.json')
            self.assertEqual(compact['priority_lane'], 'relocation')
            self.assertIn('description_sha256', compact)

    def test_scopes_restore_legacy_globals(self):
        before = (old.POLICY_VERSION, old.classify_candidate, search.compact_candidate, search.lane_definitions, orchestrator.prepare_runtime_config)
        with run_scope('egypt_48h'):
            self.assertEqual(old.POLICY_VERSION, 16)
            self.assertEqual([d['name'] for d in search.lane_definitions({'location': 'Egypt'})], ['egypt', 'remote_egypt'])
        self.assertEqual(before, (old.POLICY_VERSION, old.classify_candidate, search.compact_candidate, search.lane_definitions, orchestrator.prepare_runtime_config))
        self.assertIn('remote_mena', [d['name'] for d in search.lane_definitions({'location': 'Egypt'})])
        self.assertIn('remote_emea', [d['name'] for d in search.lane_definitions({'location': 'Egypt'})])

    def test_oneoff_does_not_mutate_search_defaults(self):
        config = {'window_minutes': 180, 'queries': [{'query': 'IT', 'pages': 4, 'query_weight': 0}], 'remote_egypt_queries': [{'query': 'Systems Engineer', 'pages': 2}]}
        original = copy.deepcopy(config)
        catchup = catchup_config(config)
        self.assertEqual(config, original)
        self.assertEqual(catchup['window_minutes'], 2880)
        self.assertEqual(catchup['queries'][0]['query'], 'IT')
        self.assertEqual(catchup['queries'][0]['query_weight'], 0)
        self.assertEqual(catchup['queries'][0]['pages'], 60)

    def test_current_queue_summaries_sync_after_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            atomic_write_json(root / 'output/pending_runs.json', {'runs': [], 'backlog': {'pending_candidate_count': 0, 'pending_part_count': 0, 'pending_run_count': 0, 'warning': False}, 'integrity': {'status': 'validated'}})
            atomic_write_json(root / 'output/latest.json', {'delivery_queue': {'pending_candidate_count': 108}, 'health_components': {'queue': 'healthy_with_backlog_warning'}})
            atomic_write_json(root / 'output/deferred_summary.json', {'pending_backlog_after_targeting': {'pending_candidate_count': 108}})
            synchronize(root)
            latest = read_json(root / 'output/latest.json')
            self.assertEqual(latest['delivery_queue']['pending_candidate_count'], 0)
            self.assertEqual(latest['health_components']['queue'], 'healthy')
            self.assertEqual(read_json(root / 'output/deferred_summary.json')['pending_backlog_after_targeting']['pending_candidate_count'], 0)
            self.assertEqual(latest['scan_time_backlog']['pending_candidate_count'], 108)


if __name__ == '__main__':
    unittest.main()
