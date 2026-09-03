import json
import tempfile
import unittest
from pathlib import Path

import radar_pipeline_v13 as pipeline


class CompactReviewPacketTests(unittest.TestCase):
    def setUp(self):
        pipeline.ACTIVE_CONFIG.clear()
        pipeline.ACTIVE_CONFIG.update({
            "delivery_local_chunk_size": 1,
            "delivery_default_chunk_size": 8,
            "delivery_max_compact_chars": 14000,
        })

    def candidate(self, job_id, *, lane="remote_worldwide", tier="remote", location="Worldwide", description=None):
        return {
            "linkedin_job_id": str(job_id),
            "title": "Infrastructure Engineer",
            "company": "Example",
            "location": location,
            "description": description or (
                "This is a remote role across EMEA. Manage Windows Server, "
                "Active Directory, VMware, networking, backups and security."
            ),
            "discovery_lane": lane,
            "discovery_remote_filter": "remote" if lane.startswith("remote_") else None,
            "delivery_tier": tier,
            "advisory_it_evidence": True,
            "score": 20,
            "role_hits_title": [{"label": "IT Infrastructure", "weight": 8}],
            "role_hits_description": [],
            "skill_hits": [{"label": "Windows Server", "weight": 3.5}],
            "negative_hits": [],
            "matched_queries": [
                {"query": f"Query {index}", "weight": index}
                for index in range(10)
            ],
        }

    def test_compact_candidate_drops_repeated_ballast_and_caps_queries(self):
        compact = pipeline.compact_candidate(self.candidate("1"), "source.json", 650)
        self.assertEqual(compact["matched_query_count"], 10)
        self.assertEqual(len(compact["matched_queries"]), 6)
        self.assertEqual(compact["decision_required_for_job_id"], "1")
        for key in pipeline.COMPACT_DROP_KEYS:
            self.assertNotIn(key, compact)
        self.assertNotIn("note", compact["remote_eligibility"])
        self.assertLessEqual(len(compact["description_excerpt"]), 500)

    def test_nonlocal_packets_fit_more_jobs_but_stay_under_byte_ceiling(self):
        candidates = [self.candidate(index) for index in range(17)]
        with tempfile.TemporaryDirectory() as tmp:
            delivery = Path(tmp)
            parts = pipeline.write_delivery_parts(
                "20260903T200000Z",
                "2026-09-03T20:05:00Z",
                "healthy",
                [],
                candidates,
                {},
                "source.json",
                25,
                delivery,
                500,
            )
            payloads = [
                json.loads((delivery / Path(part["path"]).name).read_text(encoding="utf-8"))
                for part in parts
            ]
            self.assertLess(len(parts), 5)
            self.assertTrue(all(payload["candidate_count"] <= 8 for payload in payloads))
            self.assertTrue(all(part["compact_chars"] <= 14000 for part in parts))
            self.assertEqual(sum(part["candidate_count"] for part in parts), 17)

    def test_fuzzy_regional_lane_does_not_get_priority_without_region_evidence(self):
        candidate = self.candidate(
            "2",
            lane="remote_mena",
            location="Remote",
            description="Remote technical role supporting a global company.",
        )
        self.assertEqual(pipeline.priority_lane(candidate), "remote_worldwide")

    def test_proven_mena_role_keeps_regional_priority(self):
        candidate = self.candidate(
            "3",
            lane="remote_mena",
            location="MENA",
            description="Remote infrastructure role open to candidates based in MENA.",
        )
        self.assertEqual(pipeline.priority_lane(candidate), "remote_mena_middle_east")

    def test_explicit_onsite_result_moves_to_relocation_lane(self):
        candidate = self.candidate(
            "4",
            lane="remote_emea",
            location="Berlin, Germany",
            description="This role is fully on-site in Berlin.",
        )
        self.assertEqual(pipeline.priority_lane(candidate), "relocation")


if __name__ == "__main__":
    unittest.main()
