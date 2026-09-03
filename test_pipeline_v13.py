import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import radar_pipeline_v13 as pipeline
from queue_integrity import (
    atomic_write_json,
    job_ids_digest,
    reconcile_pending_queue,
    validate_part_payload,
)


class PipelineV13Tests(unittest.TestCase):
    def setUp(self):
        pipeline.ACTIVE_CONFIG.clear()
        pipeline.ACTIVE_CONFIG.update({
            "delivery_local_chunk_size": 1,
            "delivery_default_chunk_size": 4,
            "delivery_max_compact_chars": 14000,
        })

    def candidate(self, job_id, lane="remote_worldwide", tier="remote", location="Worldwide"):
        return {
            "linkedin_job_id": str(job_id),
            "title": "Infrastructure Engineer",
            "company": "Example",
            "location": location,
            "description": "This is a remote role across EMEA. Manage Windows Server, Active Directory and VMware infrastructure.",
            "discovery_lane": lane,
            "discovery_remote_filter": "remote" if lane.startswith("remote_") else None,
            "delivery_tier": tier,
            "advisory_it_evidence": True,
            "score": 20,
            "role_hits_title": [{"label": "IT Infrastructure", "weight": 8}],
            "role_hits_description": [],
            "skill_hits": [{"label": "Windows Server", "weight": 3.5}],
            "negative_hits": [],
            "matched_queries": [{"query": "Infrastructure Engineer"}],
        }

    def test_lane_order_is_egypt_first_and_keeps_mena_emea(self):
        definitions = pipeline.lane_definitions({"location": "Egypt"})
        self.assertEqual(
            [item["name"] for item in definitions],
            ["egypt", "remote_egypt", "remote_mena", "remote_middle_east", "remote_emea", "remote_worldwide"],
        )
        for name in ("remote_mena", "remote_middle_east", "remote_emea"):
            definition = next(item for item in definitions if item["name"] == name)
            self.assertEqual(definition["location"], "Worldwide")
            self.assertEqual(definition["strategy"], "regional_keyword_remote")

    def test_regional_lane_uses_keyword_queries_not_invalid_location(self):
        base = {
            "location": "Egypt",
            "queries": [{"query": "IT", "pages": 1}],
            "remote_egypt_queries": [{"query": "System Administrator", "pages": 1}],
            "remote_worldwide_queries": [{"query": "Infrastructure Engineer", "pages": 1}],
            "regional_remote_queries": {
                "remote_mena": [{"query": "MENA infrastructure", "pages": 1}],
                "remote_middle_east": [{"query": "Middle East network", "pages": 1}],
                "remote_emea": [{"query": "EMEA systems", "pages": 1}],
            },
        }
        definition = next(item for item in pipeline.lane_definitions(base) if item["name"] == "remote_emea")
        config = pipeline.prepare_lane_config(base, definition)
        self.assertEqual(config["location"], "Worldwide")
        self.assertEqual(config["queries"][0]["query"], "EMEA systems")

    def test_alexandria_virginia_is_not_egypt(self):
        self.assertFalse(pipeline.is_egypt_candidate({"location": "Alexandria, VA", "discovery_lane": "remote_middle_east"}))
        self.assertTrue(pipeline.is_egypt_candidate({"location": "Alexandria, Egypt", "discovery_lane": "remote_worldwide"}))
        self.assertTrue(pipeline.is_egypt_candidate({"location": "Cairo", "discovery_lane": "remote_worldwide"}))

    def test_generic_worldwide_company_text_is_not_remote_evidence(self):
        annotation = pipeline.remote_eligibility_annotation({
            "location": "Chestertown, MD",
            "description": "We serve customers worldwide. This role is fully on-site in Maryland.",
        })
        self.assertEqual(annotation["status"], "explicit_non_remote")
        self.assertEqual(annotation["eligible_signals"], [])

    def test_explicit_emea_remote_is_eligible(self):
        annotation = pipeline.remote_eligibility_annotation({
            "location": "EMEA",
            "description": "This is a fully remote role open to candidates based anywhere in EMEA.",
        })
        self.assertEqual(annotation["status"], "explicit_egypt_emea_or_global_signal")

    def test_location_restriction_wins_over_global_wording(self):
        annotation = pipeline.remote_eligibility_annotation({
            "location": "Worldwide",
            "description": "Global remote role. Candidates must be based in the United States.",
        })
        self.assertEqual(annotation["status"], "explicit_location_or_work_authorization_restriction")

    def test_egypt_jobs_are_one_per_part_and_other_parts_are_small(self):
        candidates = [
            self.candidate("eg1", lane="egypt", tier="local", location="Cairo, Egypt"),
            self.candidate("eg2", lane="remote_egypt", tier="local", location="Giza, Egypt"),
        ] + [self.candidate(f"r{i}") for i in range(9)]
        with tempfile.TemporaryDirectory() as tmp:
            delivery = Path(tmp) / "delivery"
            parts = pipeline.write_delivery_parts(
                "20260903T200000Z", "2026-09-03T20:05:00Z", "healthy", [],
                candidates, {}, "source.json", 25, delivery, 650,
            )
            local_parts = [part for part in parts if part["delivery_tier"] == "local"]
            remote_parts = [part for part in parts if part["delivery_tier"] == "remote"]
            self.assertEqual(len(local_parts), 2)
            self.assertTrue(all(part["candidate_count"] == 1 for part in local_parts))
            self.assertTrue(all(part["candidate_count"] <= 4 for part in remote_parts))
            self.assertEqual(sum(part["candidate_count"] for part in parts), len(candidates))
            for part in parts:
                payload = json.loads((delivery / Path(part["path"]).name).read_text(encoding="utf-8"))
                validation = validate_part_payload(payload, part)
                self.assertEqual(validation["job_ids"], part["job_ids"])

    def test_reconcile_removes_acknowledged_part_and_validates_survivor(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery = root / "output" / "delivery"
            delivery.mkdir(parents=True)
            first = self._write_part(delivery / "run-part-001.json", "run-part-001", ["1"])
            second = self._write_part(delivery / "run-part-002.json", "run-part-002", ["2", "3"])
            pending = {
                "schema_version": 3,
                "retention_hours": 168,
                "runs": [{
                    "run_id": "run",
                    "generated_at_utc": "2026-09-03T18:00:00Z",
                    "delivery_parts": [first, second],
                    "parts": [],
                    "candidate_count": 3,
                }],
            }
            atomic_write_json(root / "output" / "pending_runs.json", pending)
            atomic_write_json(root / "state" / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {"run-part-001": "2026-09-03T18:05:00Z"},
                "reported_jobs": {},
            })
            result = reconcile_pending_queue(
                root,
                root / "output" / "pending_runs.json",
                root / "state" / "reported_runs.json",
                now=datetime(2026, 9, 3, 18, 10, tzinfo=timezone.utc),
            )
            self.assertFalse((delivery / "run-part-001.json").exists())
            self.assertEqual(result["backlog"]["pending_part_count"], 1)
            self.assertEqual(result["backlog"]["pending_candidate_count"], 2)
            self.assertEqual(result["integrity"]["status"], "validated")

    def test_candidate_receipt_can_trim_part_without_losing_other_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery = root / "output" / "delivery"
            delivery.mkdir(parents=True)
            part = self._write_part(delivery / "run-part-001.json", "run-part-001", ["1", "2"])
            atomic_write_json(root / "output" / "pending_runs.json", {
                "schema_version": 3,
                "retention_hours": 168,
                "runs": [{
                    "run_id": "run",
                    "generated_at_utc": "2026-09-03T18:00:00Z",
                    "delivery_parts": [part],
                    "parts": [],
                    "candidate_count": 2,
                }],
            })
            atomic_write_json(root / "state" / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {},
                "reported_jobs": {"1": {"decision": "skip"}},
            })
            result = reconcile_pending_queue(
                root,
                root / "output" / "pending_runs.json",
                root / "state" / "reported_runs.json",
                now=datetime(2026, 9, 3, 18, 10, tzinfo=timezone.utc),
            )
            payload = json.loads((delivery / "run-part-001.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["expected_job_ids"], ["2"])
            self.assertEqual(result["backlog"]["pending_candidate_count"], 1)
            self.assertEqual(result["integrity"]["removed_acknowledged_jobs"], 1)

    def _write_part(self, path, part_id, ids):
        candidates = [{"linkedin_job_id": job_id, "title": "Role"} for job_id in ids]
        payload = {
            "schema_version": 4,
            "part_id": part_id,
            "run_id": "run",
            "generated_at_utc": "2026-09-03T18:00:00Z",
            "candidate_count": len(ids),
            "expected_job_ids": ids,
            "integrity": {
                "complete_candidate_list": True,
                "job_id_count": len(ids),
                "job_ids_sha256": job_ids_digest(ids),
            },
            "review_candidates": candidates,
        }
        atomic_write_json(path, payload)
        return {
            "part_id": part_id,
            "path": f"output/delivery/{path.name}",
            "candidate_count": len(ids),
            "job_ids": ids,
            "job_ids_sha256": job_ids_digest(ids),
            "compact_chars": len(json.dumps(payload)),
        }


if __name__ == "__main__":
    unittest.main()
