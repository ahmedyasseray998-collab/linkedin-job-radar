import tempfile
import unittest
from pathlib import Path

from ack_reconcile_v15 import verify_legacy_retarget_complete
from queue_integrity import atomic_write_json


class AckMigrationGuardV15Tests(unittest.TestCase):
    def test_acknowledged_legacy_run_is_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "output").mkdir(parents=True)
            (root / "state").mkdir(parents=True)
            atomic_write_json(root / "output" / "pending_runs.json", {
                "runs": [{
                    "run_id": "legacy-run",
                    "targeting_policy_version": 14,
                    "delivery_parts": [{"part_id": "legacy-part"}],
                }],
            })
            atomic_write_json(root / "state" / "reported_runs.json", {
                "reported_runs": {"legacy-run": "2026-09-04T09:00:00Z"},
                "reported_parts": {},
                "reported_jobs": {},
            })
            with self.assertRaises(SystemExit):
                verify_legacy_retarget_complete(root)

    def test_acknowledged_legacy_part_is_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "output").mkdir(parents=True)
            (root / "state").mkdir(parents=True)
            atomic_write_json(root / "output" / "pending_runs.json", {
                "runs": [{
                    "run_id": "legacy-run",
                    "targeting_policy_version": 14,
                    "delivery_parts": [{"part_id": "legacy-part"}],
                }],
            })
            atomic_write_json(root / "state" / "reported_runs.json", {
                "reported_runs": {},
                "reported_parts": {"legacy-part": "2026-09-04T09:00:00Z"},
                "reported_jobs": {},
            })
            with self.assertRaises(SystemExit):
                verify_legacy_retarget_complete(root)

    def test_policy_15_queue_is_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "output").mkdir(parents=True)
            (root / "state").mkdir(parents=True)
            atomic_write_json(root / "output" / "pending_runs.json", {
                "runs": [{
                    "run_id": "current-run",
                    "targeting_policy_version": 15,
                    "delivery_parts": [{"part_id": "current-part"}],
                }],
            })
            atomic_write_json(root / "state" / "reported_runs.json", {
                "reported_runs": {"current-run": "2026-09-04T09:00:00Z"},
                "reported_parts": {"current-part": "2026-09-04T09:00:00Z"},
                "reported_jobs": {},
            })
            verify_legacy_retarget_complete(root)


if __name__ == "__main__":
    unittest.main()
