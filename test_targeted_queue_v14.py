import json
import tempfile
import unittest
from pathlib import Path

from queue_integrity import atomic_write_json
from targeted_queue_v14 import classify_candidate, reprioritize_pending_queue


class TargetedQueueV14Tests(unittest.TestCase):
    def candidate(
        self,
        job_id,
        *,
        lane="remote_worldwide",
        location="Remote",
        description="This is a remote worldwide infrastructure role.",
        title_roles=None,
        body_roles=None,
        skills=None,
        it_evidence=True,
    ):
        return {
            "linkedin_job_id": str(job_id),
            "title": "Infrastructure Engineer",
            "company": "Example",
            "location": location,
            "description": description,
            "discovery_lane": lane,
            "discovery_remote_filter": "remote" if lane.startswith("remote_") else None,
            "advisory_it_evidence": it_evidence,
            "score": 25,
            "role_hits_title": [
                {"label": label, "weight": 8} for label in (title_roles or [])
            ],
            "role_hits_description": [
                {"label": label, "weight": 6} for label in (body_roles or [])
            ],
            "skill_hits": [
                {"label": label, "weight": 3} for label in (skills or [])
            ],
            "negative_hits": [],
            "matched_queries": [{"query": "Infrastructure Engineer", "weight": 5}],
            "application_status": "unknown",
        }

    def strong_candidate(self, job_id, **kwargs):
        return self.candidate(
            job_id,
            title_roles=["IT Infrastructure"],
            body_roles=["Systems Administration"],
            skills=["Windows Server", "Active Directory", "VMware", "Backup/DR"],
            **kwargs,
        )

    def test_every_egypt_it_candidate_is_protected(self):
        tier, reason = classify_candidate(self.candidate(
            "eg",
            lane="egypt",
            location="Cairo, Egypt",
            description="Support internal business systems.",
            body_roles=["IT/ICT General"],
            skills=[],
        ))
        self.assertEqual(tier, "local")
        self.assertEqual(reason, "all_egypt_it_candidates_are_protected")

    def test_mena_remote_requires_job_level_region_evidence(self):
        good = self.strong_candidate(
            "mena-good",
            lane="remote_mena",
            location="MENA",
            description="Remote infrastructure role open to candidates based in MENA.",
        )
        bad = self.strong_candidate(
            "mena-noise",
            lane="remote_mena",
            location="United States",
            description="Remote role supporting a company with customers around the world.",
        )
        self.assertEqual(classify_candidate(good)[0], "remote")
        self.assertEqual(classify_candidate(bad), (
            None,
            "regional_query_without_job_level_regional_evidence",
        ))

    def test_us_only_and_clearance_roles_are_deferred(self):
        candidate = self.strong_candidate(
            "blocked",
            location="Washington, DC",
            description=(
                "Remote role. Candidates must be based in the United States and "
                "must hold an active security clearance."
            ),
        )
        self.assertEqual(classify_candidate(candidate), (
            None,
            "explicit_location_work_authorization_or_clearance_block",
        ))

    def test_strong_emea_onsite_role_can_be_relocation_lead(self):
        candidate = self.strong_candidate(
            "eu",
            lane="remote_emea",
            location="Lisbon, Portugal",
            description="This EMEA role is fully on-site in Lisbon.",
        )
        self.assertEqual(classify_candidate(candidate), (
            "relocation",
            "strong_regional_onsite_relocation_lead",
        ))

    def test_global_remote_needs_explicit_or_strong_generic_location(self):
        explicit = self.strong_candidate(
            "global",
            location="Worldwide",
            description="This is a fully remote role open worldwide.",
        )
        ambiguous = self.strong_candidate(
            "country",
            location="Toronto, Canada",
            description="LinkedIn labels this role remote.",
        )
        self.assertEqual(classify_candidate(explicit)[0], "remote")
        self.assertEqual(classify_candidate(ambiguous), (
            None,
            "country_specific_or_ambiguous_global_remote_eligibility",
        ))

    def test_reprioritization_reduces_noise_and_keeps_auditable_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "output" / "runs"
            delivery = root / "output" / "delivery"
            runs.mkdir(parents=True)
            delivery.mkdir(parents=True)
            (root / "state").mkdir()

            candidates = [
                self.candidate(
                    "eg",
                    lane="egypt",
                    location="Cairo, Egypt",
                    body_roles=["IT/ICT General"],
                ),
                self.strong_candidate(
                    "mena",
                    lane="remote_mena",
                    location="MENA",
                    description="Remote infrastructure role open to candidates based in MENA.",
                ),
                self.strong_candidate(
                    "eu",
                    lane="remote_emea",
                    location="Lisbon, Portugal",
                    description="This EMEA role is fully on-site in Lisbon.",
                ),
                self.strong_candidate(
                    "blocked",
                    location="Washington, DC",
                    description="Candidates must be based in the United States.",
                ),
            ]
            source_ref = "output/runs/run-audit-001.json"
            atomic_write_json(runs / "run-audit-001.json", {
                "run_id": "run",
                "generated_at_utc": "2026-09-03T18:00:00Z",
                "review_candidates": candidates,
            })
            old_delivery = delivery / "run-part-001.json"
            atomic_write_json(old_delivery, {
                "schema_version": 3,
                "part_id": "run-part-001",
                "run_id": "run",
                "generated_at_utc": "2026-09-03T18:00:00Z",
                "candidate_count": 4,
                "review_candidates": [
                    {"linkedin_job_id": item["linkedin_job_id"]}
                    for item in candidates
                ],
            })
            pending_path = root / "output" / "pending_runs.json"
            reported_path = root / "state" / "reported_runs.json"
            summary_path = root / "output" / "deferred_summary.json"
            atomic_write_json(pending_path, {
                "schema_version": 3,
                "retention_hours": 168,
                "runs": [{
                    "run_id": "run",
                    "generated_at_utc": "2026-09-03T18:00:00Z",
                    "health": "healthy",
                    "warnings": [],
                    "candidate_count": 4,
                    "parts": [source_ref],
                    "delivery_parts": [{
                        "part_id": "run-part-001",
                        "path": "output/delivery/run-part-001.json",
                        "candidate_count": 4,
                    }],
                }],
            })
            atomic_write_json(reported_path, {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {},
                "reported_jobs": {},
            })
            config = {
                "delivery_local_chunk_size": 1,
                "delivery_default_chunk_size": 8,
                "delivery_max_compact_chars": 14000,
                "delivery_excerpt_chars": 500,
                "run_archive_chunk_size": 25,
                "delivery_backlog_warning_candidates": 500,
            }

            result = reprioritize_pending_queue(
                config,
                pending_path=pending_path,
                reported_path=reported_path,
                summary_path=summary_path,
            )
            totals = result["summary"]["totals"]
            self.assertEqual(totals["input_lossless_candidates"], 4)
            self.assertEqual(totals["selected_candidates"], 3)
            self.assertEqual(totals["deferred_candidates"], 1)
            self.assertEqual(totals["protected_egypt_candidates"], 1)
            self.assertEqual(result["pending"]["backlog"]["pending_candidate_count"], 3)
            self.assertEqual(result["pending"]["integrity"]["status"], "validated")
            self.assertTrue(summary_path.exists())
            self.assertFalse(old_delivery.exists())
            parts = result["pending"]["runs"][0]["delivery_parts"]
            self.assertEqual(parts[0]["delivery_tier"], "local")
            self.assertEqual(parts[0]["candidate_count"], 1)
            self.assertTrue(all(part["compact_chars"] <= 14000 for part in parts))


if __name__ == "__main__":
    unittest.main()
