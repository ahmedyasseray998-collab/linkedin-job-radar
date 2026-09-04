import tempfile
import unittest
from pathlib import Path

from queue_integrity import atomic_write_json
from reconcile_queue import verify_policy_migration_guard


class ReconcileGuardV15Tests(unittest.TestCase):
    def test_policy_15_without_pre_scan_retarget_is_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            latest = Path(tmp) / "latest.json"
            atomic_write_json(latest, {
                "targeting_policy_version": 15,
                "health": "healthy",
            })
            with self.assertRaises(SystemExit):
                verify_policy_migration_guard(latest)

    def test_policy_15_with_pre_scan_retarget_is_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            latest = Path(tmp) / "latest.json"
            atomic_write_json(latest, {
                "targeting_policy_version": 15,
                "pre_scan_retarget": {
                    "targeting_policy_version": 15,
                    "integrity_status": "validated",
                },
            })
            verify_policy_migration_guard(latest)

    def test_legacy_policy_is_not_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            latest = Path(tmp) / "latest.json"
            atomic_write_json(latest, {"targeting_policy_version": 14})
            verify_policy_migration_guard(latest)


if __name__ == "__main__":
    unittest.main()
