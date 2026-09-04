import tempfile
import unittest
from pathlib import Path

from queue_integrity import atomic_write_json, read_json
from radar_pipeline_v15 import reprioritize_with_seen_decisions


class SeenDecisionCaptureV15Tests(unittest.TestCase):
    def test_zero_selected_run_still_records_retryable_deferral(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "output" / "runs"
            delivery = root / "output" / "delivery"
            state = root / "state"
            runs.mkdir(parents=True)
            delivery.mkdir(parents=True)
            state.mkdir(parents=True)

            candidate = {
                "linkedin_job_id": "country-only",
                "title": "Infrastructure Engineer (Remote)",
                "company": "Example",
                "location": "India",
                "description": (
                    "Remote infrastructure role. Work from anywhere in India. "
                    "Administer Windows Server, Active Directory, VMware and backups."
                ),
                "discovery_lane": "remote_worldwide",
                "discovery_remote_filter": "remote",
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
            }
            atomic_write_json(runs / "zero-audit-001.json", {
                "run_id": "zero-run",
                "generated_at_utc": "2026-09-04T09:00:00Z",
                "review_candidates": [candidate],
            })
            pending_path = root / "output" / "pending_runs.json"
            reported_path = state / "reported_runs.json"
            summary_path = root / "output" / "deferred_summary.json"
            atomic_write_json(pending_path, {
                "schema_version": 3,
                "retention_hours": 168,
                "runs": [{
                    "run_id": "zero-run",
                    "generated_at_utc": "2026-09-04T09:00:00Z",
                    "health": "healthy",
                    "warnings": [],
                    "candidate_count": 1,
                    "parts": ["output/runs/zero-audit-001.json"],
                    "delivery_parts": [],
                    "targeting_policy_version": 14,
                }],
            })
            atomic_write_json(reported_path, {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {},
                "reported_jobs": {},
            })
            atomic_write_json(state / "seen.json", {
                "config_version": 15,
                "jobs": {
                    "country-only": {
                        "status": "review_candidate",
                        "title": "Infrastructure Engineer (Remote)",
                    },
                },
            })

            result = reprioritize_with_seen_decisions(
                {
                    "delivery_local_chunk_size": 1,
                    "delivery_default_chunk_size": 8,
                    "delivery_max_compact_chars": 14000,
                    "delivery_excerpt_chars": 500,
                    "run_archive_chunk_size": 25,
                    "delivery_backlog_warning_candidates": 500,
                },
                pending_path=pending_path,
                reported_path=reported_path,
                summary_path=summary_path,
            )
            seen = read_json(state / "seen.json")
            pending = read_json(pending_path)
            record = seen["jobs"]["country-only"]

            self.assertEqual(result["seen_decisions"]["captured"], 1)
            self.assertEqual(result["seen_decisions"]["deferred"], 1)
            self.assertEqual(result["seen_decisions"]["anomaly"], 0)
            self.assertEqual(record["status"], "deferred_targeting_retryable")
            self.assertEqual(
                record["deferred_reason"],
                "explicit_location_work_authorization_or_clearance_block",
            )
            self.assertIn("retry_after_utc", record)
            self.assertEqual(pending["backlog"]["pending_candidate_count"], 0)
            self.assertEqual(pending["runs"], [])


if __name__ == "__main__":
    unittest.main()
