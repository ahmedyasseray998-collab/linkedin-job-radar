import unittest

import radar_pipeline_v13 as pipeline


class RemoteRegionEligibilityTests(unittest.TestCase):
    def test_required_emea_base_is_eligible_from_egypt(self):
        annotation = pipeline.remote_eligibility_annotation({
            "location": "EMEA",
            "description": "This is a remote role. Candidates must be based in EMEA.",
        })
        self.assertEqual(annotation["status"], "explicit_egypt_emea_or_global_signal")
        self.assertIn("required base in eligible region", annotation["eligible_signals"])
        self.assertEqual(annotation["restriction_signals"], [])

    def test_required_mena_base_is_eligible_from_egypt(self):
        annotation = pipeline.remote_eligibility_annotation({
            "location": "MENA",
            "description": "Applicants must reside in MENA and work remotely.",
        })
        self.assertEqual(annotation["status"], "explicit_egypt_emea_or_global_signal")
        self.assertEqual(annotation["restriction_signals"], [])

    def test_europe_only_requirement_is_not_treated_as_egypt_eligible(self):
        annotation = pipeline.remote_eligibility_annotation({
            "location": "Europe",
            "description": "This is remote, but candidates must be based in Europe.",
        })
        self.assertEqual(annotation["status"], "explicit_location_or_work_authorization_restriction")
        self.assertIn("candidates must be based in", annotation["restriction_signals"])


if __name__ == "__main__":
    unittest.main()
