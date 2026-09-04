import tempfile
import unittest
from pathlib import Path

from queue_integrity import atomic_write_json, job_ids_digest, read_json
from radar_pipeline_v15 import retarget_existing_queue_before_scan
from targeted_queue_v15 import POLICY_VERSION, prepare_seen_for_run


class PipelineV15OrderingTests(unittest.TestCase):
    def candidate(self, job_id, *, title, location, lane, description):
        return {
            "linkedin_job_id": job_id,
            "title": title,
            "company": "Example",
            "location": location,
            "description": description,
            "discovery_lane": lane,
            "discovery_remote_filter": "remote" if lane.startswith("remote_") else None,
            "advisory_it_evidence": True,
            "score": 30,
            "role_hits_title": [{"label": "IT Infrastructure", "weight": 8}],
            "role_hits_description": [{"label": "Systems Administration", "weight": 6}],
            "skill_hits": [
                {"label": "Windows Server", "weight": 3},
                {"label": "Active Directory", "weight": 3},
                {"label": "VMware", "weight": 3},
                {"label": "Backup/DR", "weight": 3},
            ],
            "negative_hits": [],
            "matched_queries": [{"query": "Infrastructure Engineer", "weight": 5}],
            "application_status": "unknown",
            "health": "healthy",
            "warnings": [],
        }

    def test_pre_scan_retarget_preserves_new_policy_survivor(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "output" / "runs"
            delivery = root / "output" / "delivery"
            state = root / "state"
            runs.mkdir(parents=True)
            delivery.mkdir(parents=True)
            state.mkdir(parents=True)

            done = self.candidate(
                "done",
                title="System Administrator",
                location="Remote - EMEA",
                lane="remote_emea",
                description="Fully remote EMEA systems role open anywhere in EMEA.",
            )
            newly_eligible = self.candidate(
                "new",
                title="Network Infrastructure Engineer (Remote)",
                location="Remote - EMEA",
                lane="remote_emea",
                description="Fully remote EMEA role open to candidates anywhere in EMEA.",
            )

            source_ref = "output/runs/legacy-audit-001.json"
            atomic_write_json(runs / "legacy-audit-001.json", {
                "run_id": "legacy",
                "generated_at_utc": "2026-09-04T05:00:00Z",
                "review_candidates": [done, newly_eligible],
            })

            old_digest = job_ids_digest(["done"])
            old_part_id = f"legacy-part-001-{old_digest[:12]}"
            atomic_write_json(delivery / f"{old_part_id}.json", {
                "schema_version": 4,
                "part_id": old_part_id,
                "run_id": "legacy",
                "generated_at_utc": "2026-09-04T05:00:00Z",
                "health": "healthy",
                "warnings": [],
                "candidate_count": 1,
                "expected_job_ids": ["done"],
                "integrity": {
                    "complete_candidate_list": True,
                    "job_id_count": 1,
                    "job_ids_sha256": old_digest,
                },
                "review_candidates": [{"linkedin_job_id": "done"}],
            })

            atomic_write_json(root / "output" / "pending_runs.json", {
                "schema_version": 3,
                "retention_hours": 168,
                "runs": [{
                    "run_id": "legacy",
                    "generated_at_utc": "2026-09-04T05:00:00Z",
                    "health": "healthy",
                    "warnings": [],
                    "candidate_count": 1,
                    "parts": [source_ref],
                    "delivery_parts": [{
                        "part_id": old_part_id,
                        "path": f"output/delivery/{old_part_id}.json",
                        "delivery_tier": "remote",
                        "candidate_count": 1,
                        "job_ids": ["done"],
                        "job_ids_sha256": old_digest,
                    }],
                    "targeting_policy_version": 14,
                }],
            })
            atomic_write_json(state / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {"legacy": "2026-09-04T06:00:00Z"},
                "reported_parts": {old_part_id: "2026-09-04T06:00:00Z"},
                "reported_jobs": {},
            })
            atomic_write_json(state / "seen.json", {
                "config_version": 14,
                "jobs": {
                    "done": {"status": "review_candidate"},
                    "new": {"status": "review_candidate"},
                },
            })
            atomic_write_json(root / "queries_v13.json", {
                "config_version": 14,
                "delivery_local_chunk_size": 1,
                "delivery_default_chunk_size": 8,
                "delivery_max_compact_chars": 14000,
                "delivery_excerpt_chars": 500,
                "run_archive_chunk_size": 25,
                "delivery_backlog_warning_candidates": 500,
            })

            migration = prepare_seen_for_run(root)
            result = retarget_existing_queue_before_scan(root)
            pending = read_json(root / "output" / "pending_runs.json")
            reported = read_json(state / "reported_runs.json")

            self.assertEqual(migration["retargeted_legacy_run_ids"], ["legacy"])
            self.assertEqual(result["targeting_policy_version"], POLICY_VERSION)
            self.assertEqual(result["acknowledged_candidates_excluded"], 1)
            self.assertEqual(result["selected_candidates"], 1)
            self.assertEqual(result["pending_candidate_count"], 1)
            self.assertNotIn("legacy", reported["reported_runs"])
            self.assertEqual(reported["reported_jobs"]["done"], "2026-09-04T06:00:00Z")

            self.assertEqual(len(pending["runs"]), 1)
            entry = pending["runs"][0]
            self.assertEqual(entry["targeting_policy_version"], POLICY_VERSION)
            queued_ids = [
                job_id
                for part in entry["delivery_parts"]
                for job_id in part["job_ids"]
            ]
            self.assertEqual(queued_ids, ["new"])
            self.assertTrue((runs / "legacy-audit-001.json").exists())
            self.assertFalse((delivery / f"{old_part_id}.json").exists())


if __name__ == "__main__":
    unittest.main()
