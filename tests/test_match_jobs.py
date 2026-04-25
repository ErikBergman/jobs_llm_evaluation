import json
import tempfile
import unittest
from pathlib import Path

from match_jobs import classify_file, input_from_latest, is_mock_hit, latest_discard_run, timestamp_from_input


class MockMatcherTests(unittest.TestCase):
    def test_engineer_word_is_hit(self) -> None:
        self.assertTrue(is_mock_hit({"description": "Engineer needed"}))

    def test_engineering_is_not_hit(self) -> None:
        self.assertFalse(is_mock_hit({"description": "software engineering"}))

    def test_missing_or_empty_description_is_discard(self) -> None:
        self.assertFalse(is_mock_hit({}))
        self.assertFalse(is_mock_hit({"description": ""}))

    def test_timestamp_is_derived_from_discard_input_path(self) -> None:
        input_path = Path("results/discard/20260425_120000/linkedin_jobs_sample.json")

        self.assertEqual(timestamp_from_input(input_path), "20260425_120000")

    def test_timestamp_is_derived_from_runtime_results_input_path(self) -> None:
        input_path = Path("runtime_results/discard/20260425_083223/linkedin_jobs_sample.json")

        self.assertEqual(timestamp_from_input(input_path), "20260425_083223")

    def test_classify_file_splits_results_into_buckets(self) -> None:
        jobs = [
            {"job_id": "1", "description": "Engineer needed"},
            {"job_id": "2", "description": "software engineering"},
            {"job_id": "3", "description": ""},
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "results" / "discard" / "20260425_120000" / "jobs.json"
            input_path.parent.mkdir(parents=True)
            input_path.write_text(json.dumps(jobs), encoding="utf-8")

            hits_path, discards_path = classify_file(input_path, root / "results")

            hits = json.loads(hits_path.read_text(encoding="utf-8"))
            discards = json.loads(discards_path.read_text(encoding="utf-8"))

        self.assertEqual(hits_path, root / "results" / "hits" / "20260425_120000" / "jobs_hits.json")
        self.assertEqual(discards_path, root / "results" / "discard" / "20260425_120000" / "jobs_discard.json")
        self.assertEqual([job["job_id"] for job in hits], ["1"])
        self.assertEqual([job["job_id"] for job in discards], ["2", "3"])

    def test_latest_discard_run_selects_newest_timestamp_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "results"
            (root / "discard" / "20260425_120000").mkdir(parents=True)
            (root / "discard" / "20260425_130000").mkdir()

            self.assertEqual(latest_discard_run(root), root / "discard" / "20260425_130000")

    def test_input_from_latest_selects_json_in_newest_timestamp_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "results"
            older = root / "discard" / "20260425_120000"
            newer = root / "discard" / "20260425_130000"
            older.mkdir(parents=True)
            newer.mkdir()
            (older / "older.json").write_text("[]", encoding="utf-8")
            (newer / "newer.json").write_text("[]", encoding="utf-8")
            (newer / "newer_discard.json").write_text("[]", encoding="utf-8")

            self.assertEqual(input_from_latest(root), newer / "newer.json")

    def test_latest_discard_run_errors_when_no_runs_exist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "No discard runs"):
                latest_discard_run(Path(temp_dir) / "results")

    def test_input_from_latest_errors_on_ambiguous_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "results"
            latest = root / "discard" / "20260425_130000"
            latest.mkdir(parents=True)
            (latest / "one.json").write_text("[]", encoding="utf-8")
            (latest / "two.json").write_text("[]", encoding="utf-8")
            (latest / "one_hits.json").write_text("[]", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Multiple unclassified"):
                input_from_latest(root)


if __name__ == "__main__":
    unittest.main()
