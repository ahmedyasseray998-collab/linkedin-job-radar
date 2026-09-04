import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from queue_integrity import atomic_write_json, read_json
from targeted_queue_v15 import (
    POLICY_VERSION,
    classify_candidate,
    prepare_seen_for_run,
    remote_eligibility_annotation,
    reprioritize_pending_queue,
)


class TargetedQueueV15Tests(unittest.TestCase):
    def candidate(
        self,
        job_id,
        *,
        title="Infrastructure Engineer",
        lane="remote_worldwide",
        location="Remote",
        description="This is a fully remote infrastructure role open worldwide.",
        title_roles=None,
        body_roles=None,
        skills=None,
        it_evidence=True,
    ):
        return {
            "linkedin_job_id": str(job_id),
            "title": title,
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

    def test_dubai_onsite_is_relocation_not_remote(self):
        candidate = self.strong_candidate(
            "dubai",
            title="Onsite IT Support Engineer",
            lane="remote_mena",
            location="Dubai, United Arab Emirates",
            description=(
                "Job Title: Onsite IT Support Engineer. Location: Dubai. "
                "This is a 100% on-site role supporting Windows and networks."
            ),
        )
        self.assertEqual(classify_candidate(candidate), (
            "relocation",
            "mena_onsite_or_hybrid_relocation_lead",
        ))
        self.assertEqual(
            remote_eligibility_annotation(candidate)["status"],
            "explicit_non_remote",
        )

    def test_paris_emea_or_cet_words_do_not_prove_remote_eligibility(self):
        candidate = self.strong_candidate(
            "paris",
            lane="remote_emea",
            location="Paris, France",
            description=(
                "Build object-storage infrastructure for customers across EMEA. "
                "The team collaborates during CET business hours."
            ),
        )
        tier, _ = classify_candidate(candidate)
        self.assertNotEqual(tier, "remote")

    def test_country_qualified_work_from_anywhere_is_blocked(self):
        candidate = self.strong_candidate(
            "georgia",
            title="Infrastructure Engineer (Remote)",
            location="Georgia",
            description="Remote role. Employees may work from anywhere in Georgia.",
        )
        self.assertEqual(classify_candidate(candidate), (
            None,
            "explicit_location_work_authorization_or_clearance_block",
        ))

    def test_country_specific_location_plus_bare_wfa_is_not_global(self):
        candidate = self.strong_candidate(
            "india",
            title="Database Reliability Engineer (Remote)",
            location="India",
            description="Location: Remote. Work from Anywhere. Maintain databases and backups.",
        )
        self.assertEqual(classify_candidate(candidate), (
            None,
            "country_specific_or_ambiguous_global_remote_eligibility",
        ))

    def test_true_worldwide_remote_is_allowed_even_with_country_location(self):
        candidate = self.strong_candidate(
            "global",
            title="Systems Engineer (Remote)",
            location="India",
            description=(
                "This is a fully remote role open worldwide. We hire from any country "
                "and provide a distributed infrastructure platform."
            ),
        )
        self.assertEqual(classify_candidate(candidate)[0], "remote")

    def test_explicit_remote_emea_is_allowed_from_egypt(self):
        candidate = self.strong_candidate(
            "emea",
            lane="remote_emea",
            location="Remote - EMEA",
            description="Fully remote EMEA role open to candidates based anywhere in EMEA.",
        )
        self.assertEqual(classify_candidate(candidate)[0], "remote")

    def test_onsite_wins_over_generic_global_company_language(self):
        candidate = self.strong_candidate(
            "onsite-global-company",
            title="System Administrator",
            location="Bucharest, Romania",
            description=(
                "Our company serves customers worldwide. This role is fully on-site "
                "in Bucharest and works from the office five days per week."
            ),
        )
        annotation = remote_eligibility_annotation(candidate)
        self.assertEqual(annotation["status"], "explicit_non_remote")
        self.assertEqual(annotation["work_model"], "onsite")

    def test_prepare_seen_releases_expired_deferrals(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "state").mkdir(parents=True)
            (root / "output").mkdir(parents=True)
            now = datetime.now(timezone.utc)
            atomic_write_json(root / "state" / "seen.json", {
                "config_version": POLICY_VERSION,
                "jobs": {
                    "expired": {
                        "status": "deferred_targeting_retryable",
                        "targeting_policy_version": POLICY_VERSION,
                        "retry_after_utc": (now - timedelta(minutes=1)).isoformat(),
                    },
                    "future": {
                        "status": "deferred_targeting_retryable",
                        "targeting_policy_version": POLICY_VERSION,
                        "retry_after_utc": (now + timedelta(hours=1)).isoformat(),
                    },
                },
            })
            atomic_write_json(root / "output" / "pending_runs.json", {"runs": []})
            atomic_write_json(root / "state" / "reported_runs.json", {
                "schema_version": 3,
                "reported_runs": {},
                "reported_parts": {},
                "reported_jobs": {},
            })

            result = prepare_seen_for_run(root)
            seen = read_json(root / "state" / "seen.json")
            self.assertEqual(result["released_deferred_job_ids"], ["expired"])
            self.assertNotIn("expired", seen["jobs"])
            self.assertIn("future", seen["jobs"])

    def test_legacy_reported_run_is_reopened_but_part_jobs_stay_acknowledged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "output" / "runs"
            delivery = root / "output" / "delivery"
            state = root / "state"
            runs.mkdir(parents=True)
            delivery.mkdir(parents=True)
            state.mkdir(parents=True)

            done = self.strong_candidate("done")
            newly_eligible = self.strong_candidate(
                "new",
                title="Network Engineer (Remote)",
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
            atomic_write_json(delivery / "legacy-part.json", {
                "schema_version": 4,
                "part_id": "legacy-part",
                "run_id": "legacy",
                "generated_at_utc": "2026-09-04T05:00:00Z",
                "candidate_count": 1,
                "expected_job_ids": ["done"],
                "integrity": {
                    "complete_candidate_list": True,
                    "job_id_count": 1,
                    "job_ids_sha256": "unused-in-this-test",
                },
                "review_candidates": [{"linkedin_job_id": "done"}],
            })
            pending_path = root / "output" / "pending_runs.json"
            reported_path = state / "reported_runs.json"
            summary_path = root / "output" / "deferred_summary.json"
            atomic_write_json(pending_path, {
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
                        "part_id": "legacy-part",
                        "path": "output/delivery/legacy-part.json",
                        "candidate_count": 1,
                        "job_ids": ["done"],
                    }],
                    "targeting_policy_version": 14,
                }],
            })
            atomic_write_json(reported_path, {
                "schema_version": 3,
                "reported_runs": {"legacy": "2026-09-04T06:00:00Z"},
                "reported_parts": {"legacy-part": "2026-09-04T06:00:00Z"},
                "reported_jobs": {},
            })
            atomic_write_json(state / "seen.json", {
                "config_version": 14,
                "jobs": {
                    "done": {"status": "review_candidate"},
                    "new": {"status": "review_candidate"},
                },
            })

            migration = prepare_seen_for_run(root)
            reported = read_json(reported_path)
            self.assertEqual(migration["retargeted_legacy_run_ids"], ["legacy"])
            self.assertNotIn("legacy", reported["reported_runs"])
            self.assertIn("legacy-part", reported["reported_parts"])
            self.assertIn("done", reported["reported_jobs"])

            result = reprioritize_pending_queue(
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
            parts = result["pending"]["runs"][0]["delivery_parts"]
            queued_ids = [job_id for part in parts for job_id in part["job_ids"]]
            self.assertEqual(queued_ids, ["new"])


if __name__ == "__main__":
    unittest.main()
