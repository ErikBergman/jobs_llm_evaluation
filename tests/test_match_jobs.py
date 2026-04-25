import json
import tempfile
import unittest
from pathlib import Path

from match_jobs import classify_file, is_mock_hit, timestamp_from_input


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


if __name__ == "__main__":
    unittest.main()
