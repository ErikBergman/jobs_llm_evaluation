import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from telegram_notify import (
    accumulated_job_count,
    build_message,
    considered_since,
    current_hits,
    current_metadata,
    load_metadata_files,
    summary_message,
)


class TelegramNotifyTests(unittest.TestCase):
    def test_considered_since_sums_metadata_inside_window(self) -> None:
        metadata = [
            {"matched_at": "2026-04-25T17:00:00Z", "job_count": 3},
            {"matched_at": "2026-04-25T06:00:00Z", "job_count": 2},
            {"matched_at": "2026-04-24T23:00:00Z", "job_count": 99},
        ]

        self.assertEqual(
            considered_since(metadata, now=datetime(2026, 4, 25, 18, 0, tzinfo=timezone.utc), hours=12),
            5,
        )

    def test_accumulated_job_count_deduplicates_ids_across_memory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "discard" / "run").mkdir(parents=True)
            (root / "hits" / "run").mkdir(parents=True)
            (root / "discard" / "run" / "jobs.json").write_text(
                json.dumps([{"job_id": "1"}, {"job_id": "2"}]),
                encoding="utf-8",
            )
            (root / "hits" / "run" / "jobs_hits.json").write_text(
                json.dumps([{"job_id": "2"}, {"job_id": "3"}]),
                encoding="utf-8",
            )

            self.assertEqual(accumulated_job_count(root), 3)

    def test_current_hits_uses_reason_and_url_from_current_input(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "discard" / "20260425_170715" / "linkedin_jobs_sample.json"
            input_path.parent.mkdir(parents=True)
            input_path.write_text(
                json.dumps(
                    [
                        {
                            "job_id": "440123",
                            "title": "Backend Developer",
                            "url": "https://se.linkedin.com/jobs/view/backend-developer-440123",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            metadata = {
                "input_path": str(input_path),
                "decisions": [
                    {
                        "job_id": "440123",
                        "title": "Backend Developer",
                        "hit": True,
                        "reason": "Strong Python integration fit.",
                    }
                ],
            }

            self.assertEqual(
                current_hits(metadata),
                [
                    {
                        "job_id": "440123",
                        "title": "Backend Developer",
                        "url": "https://se.linkedin.com/jobs/view/backend-developer-440123",
                        "reason": "Strong Python integration fit.",
                    }
                ],
            )

    def test_summary_message_reports_counts_and_current_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = root / "discard" / "20260425_170715"
            run_dir.mkdir(parents=True)
            input_path = run_dir / "linkedin_jobs_sample.json"
            metadata_path = run_dir / "linkedin_jobs_sample_match_metadata.json"
            input_path.write_text(
                json.dumps(
                    [
                        {
                            "job_id": "cheat-mode-perfect-job",
                            "title": "Perfect Job",
                            "url": "cheat-mode://perfect-job-ad",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            metadata_path.write_text(
                json.dumps(
                    {
                        "matched_at": "2026-04-25T17:07:23Z",
                        "input_path": str(input_path),
                        "job_count": 1,
                        "decisions": [
                            {
                                "job_id": "cheat-mode-perfect-job",
                                "title": "Perfect Job",
                                "hit": True,
                                "reason": "Unusually exact match.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            message = summary_message(
                root,
                current_input=str(input_path),
                hours=12,
                now=datetime(2026, 4, 25, 18, 0, tzinfo=timezone.utc),
            )

        self.assertIn("Considered in last 12 hours: 1", message)
        self.assertIn("Ads in memory: 1", message)
        self.assertIn("New matches in this run: 1", message)
        self.assertIn("cheat-mode://perfect-job-ad", message)
        self.assertIn("Reason: Unusually exact match.", message)

    def test_summary_message_does_not_repeat_old_hits_without_current_input(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = root / "discard" / "20260425_170715"
            run_dir.mkdir(parents=True)
            input_path = run_dir / "linkedin_jobs_sample.json"
            metadata_path = run_dir / "linkedin_jobs_sample_match_metadata.json"
            input_path.write_text(
                json.dumps([{"job_id": "1", "title": "Old Match", "url": "https://example.com/job"}]),
                encoding="utf-8",
            )
            metadata_path.write_text(
                json.dumps(
                    {
                        "matched_at": "2026-04-25T17:07:23Z",
                        "input_path": str(input_path),
                        "job_count": 1,
                        "decisions": [{"job_id": "1", "title": "Old Match", "hit": True, "reason": "Old reason."}],
                    }
                ),
                encoding="utf-8",
            )

            message = summary_message(
                root,
                current_input=None,
                hours=12,
                now=datetime(2026, 4, 25, 18, 0, tzinfo=timezone.utc),
            )

        self.assertIn("New matches in this run: 0", message)
        self.assertNotIn("Old reason.", message)

    def test_current_metadata_prefers_matching_input_path(self) -> None:
        metadata = [
            {"input_path": "older.json", "matched_at": "2026-04-25T18:00:00Z"},
            {"input_path": "current.json", "matched_at": "2026-04-25T17:00:00Z"},
        ]

        self.assertEqual(current_metadata(metadata, current_input="current.json")["input_path"], "current.json")

    def test_build_message_handles_no_matches(self) -> None:
        message = build_message(considered_last_hours=0, accumulated_jobs=4, hits=[], hours=12)

        self.assertIn("Considered in last 12 hours: 0", message)
        self.assertIn("Ads in memory: 4", message)
        self.assertIn("New matches in this run: 0", message)

    def test_load_metadata_files_ignores_unreadable_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = root / "discard" / "run"
            run_dir.mkdir(parents=True)
            (run_dir / "good_match_metadata.json").write_text("{}", encoding="utf-8")
            (run_dir / "bad_match_metadata.json").write_text("{", encoding="utf-8")

            self.assertEqual(len(load_metadata_files(root)), 1)


if __name__ == "__main__":
    unittest.main()
