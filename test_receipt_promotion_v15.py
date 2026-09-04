import tempfile
import unittest
from pathlib import Path

from queue_integrity import atomic_write_json, read_json
from receipt_promotion_v15 import promote_manual_receipts


class ReceiptPromotionV15Tests(unittest.TestCase):
    def test_part_receipt_promotes_job_receipts_and_seen_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "output" / "delivery").mkdir(parents=True)
            (root / "state").mkdir(parents=True)

            atomic_write_json(root / "output" / "pending_runs.json", {
                "runs": [{
                    "run_id": "run-1",
                    "delivery_parts": [{
                        "part_id": "run-1-part-001-abcdef123456",
                        "path": "output/delivery/run-1-part-001-abcdef123456.json",
                        "candidate_count": 2,
                        "job_ids": ["job-a", "job-b"],
                    }],
                }],
            })
            atomic_write_json(root / "output" / "delivery" / "run-1-part-001-abcdef123456.json", {
                "schema_version": 4,
                "part_id": "run-1-part-001-abcdef123456",
                "run_id": "run-1",
                "candidate_count": 2,
                "expected_job_ids": ["job-a", "job-b"],
                "integrity": {
                    "complete_candidate_list": True,
                    "job_id_count": 2,
                },
                "review_candidates": [
                    {"linkedin_job_id": "job-a"},
                    {"linkedin_job_id": "job-b"},
                ],
            })
            atomic_write_json(root / "state" / "manual_backlog_receipts.json", {
                "reviewed_at_utc": "2026-09-04T09:15:00Z",
                "reported_parts": {
                    "run-1-part-001-abcdef123456": "2026-09-04T09:15:00Z",
                },
            })
            atomic_write_json(root / "state" / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {},
                "reported_jobs": {"older-job": "2026-09-04T08:00:00Z"},
            })
            atomic_write_json(root / "state" / "seen.json", {
                "jobs": {
                    "job-a": {"status": "queued_for_gpt"},
                    "job-b": {"status": "queued_for_gpt"},
                },
            })

            result = promote_manual_receipts(root)
            reported = read_json(root / "state" / "reported_runs.json")
            seen = read_json(root / "state" / "seen.json")

            self.assertEqual(result["part_receipts_merged"], 1)
            self.assertEqual(result["job_receipts_promoted"], 2)
            self.assertEqual(result["new_job_receipts"], 2)
            self.assertEqual(result["run_receipts_merged"], 1)
            self.assertEqual(result["unresolved_part_receipts"], [])
            self.assertIn("older-job", reported["reported_jobs"])
            self.assertEqual(reported["reported_jobs"]["job-a"], "2026-09-04T09:15:00Z")
            self.assertEqual(reported["reported_jobs"]["job-b"], "2026-09-04T09:15:00Z")
            self.assertEqual(reported["reported_runs"]["run-1"], "2026-09-04T09:15:00Z")
            self.assertEqual(seen["jobs"]["job-a"]["status"], "reviewed_acknowledged")
            self.assertEqual(seen["jobs"]["job-b"]["status"], "reviewed_acknowledged")
            self.assertTrue((root / "state" / "manual_backlog_receipts.json").exists())

    def test_unresolved_part_receipt_never_invents_job_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "output").mkdir(parents=True)
            (root / "state").mkdir(parents=True)
            atomic_write_json(root / "output" / "pending_runs.json", {"runs": []})
            atomic_write_json(root / "state" / "manual_backlog_receipts.json", {
                "reviewed_at_utc": "2026-09-04T09:15:00Z",
                "reported_parts": {"unknown-part": "2026-09-04T09:15:00Z"},
            })
            atomic_write_json(root / "state" / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {},
                "reported_jobs": {},
            })

            result = promote_manual_receipts(root)
            reported = read_json(root / "state" / "reported_runs.json")
            self.assertEqual(result["unresolved_part_receipts"], ["unknown-part"])
            self.assertEqual(reported["reported_jobs"], {})
            self.assertIn("unknown-part", reported["reported_parts"])


if __name__ == "__main__":
    unittest.main()
