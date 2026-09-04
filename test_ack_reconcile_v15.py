import tempfile
import unittest
from pathlib import Path

from ack_reconcile_v15 import reconcile_acknowledgements
from queue_integrity import atomic_write_json, job_ids_digest, read_json


class AckReconcileV15Tests(unittest.TestCase):
    def test_direct_part_receipt_promotes_jobs_and_removes_acknowledged_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery = root / "output" / "delivery"
            runs = root / "output" / "runs"
            state = root / "state"
            delivery.mkdir(parents=True)
            runs.mkdir(parents=True)
            state.mkdir(parents=True)

            job_ids = ["job-a", "job-b"]
            digest = job_ids_digest(job_ids)
            part_id = f"run-1-part-001-{digest[:12]}"
            part_path = f"output/delivery/{part_id}.json"
            atomic_write_json(delivery / f"{part_id}.json", {
                "schema_version": 4,
                "part_id": part_id,
                "run_id": "run-1",
                "candidate_count": 2,
                "expected_job_ids": job_ids,
                "integrity": {
                    "complete_candidate_list": True,
                    "job_id_count": 2,
                    "job_ids_sha256": digest,
                },
                "review_candidates": [
                    {"linkedin_job_id": "job-a"},
                    {"linkedin_job_id": "job-b"},
                ],
            })
            atomic_write_json(runs / "run-1-audit-001.json", {
                "run_id": "run-1",
                "review_candidates": [
                    {"linkedin_job_id": "job-a"},
                    {"linkedin_job_id": "job-b"},
                ],
            })
            atomic_write_json(root / "output" / "pending_runs.json", {
                "schema_version": 3,
                "runs": [{
                    "run_id": "run-1",
                    "generated_at_utc": "2026-09-04T09:00:00Z",
                    "candidate_count": 2,
                    "parts": ["output/runs/run-1-audit-001.json"],
                    "delivery_parts": [{
                        "part_id": part_id,
                        "path": part_path,
                        "candidate_count": 2,
                        "job_ids": job_ids,
                        "job_ids_sha256": digest,
                    }],
                }],
            })
            atomic_write_json(state / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {part_id: "2026-09-04T09:15:00Z"},
                "reported_jobs": {},
            })
            atomic_write_json(state / "seen.json", {
                "jobs": {
                    "job-a": {"status": "queued_for_gpt"},
                    "job-b": {"status": "queued_for_gpt"},
                },
            })
            atomic_write_json(root / "queries_v13.json", {
                "delivery_backlog_warning_candidates": 500,
            })

            result = reconcile_acknowledgements(root)
            reported = read_json(state / "reported_runs.json")
            pending = read_json(root / "output" / "pending_runs.json")
            seen = read_json(state / "seen.json")

            self.assertEqual(result["reported_part_promotion"]["job_receipts_promoted"], 2)
            self.assertEqual(result["reported_part_promotion"]["run_receipts_inferred"], 1)
            self.assertEqual(reported["reported_jobs"]["job-a"], "2026-09-04T09:15:00Z")
            self.assertEqual(reported["reported_jobs"]["job-b"], "2026-09-04T09:15:00Z")
            self.assertIn("run-1", reported["reported_runs"])
            self.assertEqual(seen["jobs"]["job-a"]["status"], "reviewed_acknowledged")
            self.assertEqual(seen["jobs"]["job-b"]["status"], "reviewed_acknowledged")
            self.assertEqual(pending["backlog"]["pending_candidate_count"], 0)
            self.assertFalse((delivery / f"{part_id}.json").exists())
            self.assertFalse((runs / "run-1-audit-001.json").exists())

    def test_partial_part_receipt_removes_only_that_part(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery = root / "output" / "delivery"
            runs = root / "output" / "runs"
            state = root / "state"
            delivery.mkdir(parents=True)
            runs.mkdir(parents=True)
            state.mkdir(parents=True)

            parts = []
            reported_part = None
            for index, job_id in enumerate(("job-a", "job-b"), start=1):
                digest = job_ids_digest([job_id])
                part_id = f"run-2-part-{index:03d}-{digest[:12]}"
                if index == 1:
                    reported_part = part_id
                atomic_write_json(delivery / f"{part_id}.json", {
                    "schema_version": 4,
                    "part_id": part_id,
                    "run_id": "run-2",
                    "candidate_count": 1,
                    "expected_job_ids": [job_id],
                    "integrity": {
                        "complete_candidate_list": True,
                        "job_id_count": 1,
                        "job_ids_sha256": digest,
                    },
                    "review_candidates": [{"linkedin_job_id": job_id}],
                })
                parts.append({
                    "part_id": part_id,
                    "path": f"output/delivery/{part_id}.json",
                    "candidate_count": 1,
                    "job_ids": [job_id],
                    "job_ids_sha256": digest,
                })

            atomic_write_json(runs / "run-2-audit-001.json", {
                "run_id": "run-2",
                "review_candidates": [
                    {"linkedin_job_id": "job-a"},
                    {"linkedin_job_id": "job-b"},
                ],
            })
            atomic_write_json(root / "output" / "pending_runs.json", {
                "schema_version": 3,
                "runs": [{
                    "run_id": "run-2",
                    "generated_at_utc": "2026-09-04T09:00:00Z",
                    "candidate_count": 2,
                    "parts": ["output/runs/run-2-audit-001.json"],
                    "delivery_parts": parts,
                }],
            })
            atomic_write_json(state / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {reported_part: "2026-09-04T09:15:00Z"},
                "reported_jobs": {},
            })
            atomic_write_json(state / "seen.json", {
                "jobs": {
                    "job-a": {"status": "queued_for_gpt"},
                    "job-b": {"status": "queued_for_gpt"},
                },
            })
            atomic_write_json(root / "queries_v13.json", {
                "delivery_backlog_warning_candidates": 500,
            })

            result = reconcile_acknowledgements(root)
            pending = read_json(root / "output" / "pending_runs.json")
            reported = read_json(state / "reported_runs.json")

            self.assertEqual(result["reported_part_promotion"]["run_receipts_inferred"], 0)
            self.assertNotIn("run-2", reported["reported_runs"])
            self.assertEqual(set(reported["reported_jobs"]), {"job-a"})
            self.assertEqual(pending["backlog"]["pending_candidate_count"], 1)
            self.assertEqual(pending["runs"][0]["delivery_parts"][0]["job_ids"], ["job-b"])
            self.assertTrue((runs / "run-2-audit-001.json").exists())


if __name__ == "__main__":
    unittest.main()
