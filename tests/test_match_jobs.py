import json
import tempfile
import unittest
from pathlib import Path

from match_jobs import (
    DEFAULT_OPENAI_MODEL,
    DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
    OPENAI_RESPONSES_URL,
    call_openai_title_vowel_matcher,
    classify_file,
    extract_response_text,
    input_from_latest,
    is_mock_hit,
    latest_discard_run,
    openai_title_vowel_prompt,
    parse_hit_response,
    split_jobs,
    summarize_response_shape,
    timestamp_from_input,
)


class MockMatcherTests(unittest.TestCase):
    def test_engineer_word_is_hit(self) -> None:
        self.assertTrue(is_mock_hit({"description": "Engineer needed"}))

    def test_engineering_is_not_hit(self) -> None:
        self.assertFalse(is_mock_hit({"description": "software engineering"}))

    def test_missing_or_empty_description_is_discard(self) -> None:
        self.assertFalse(is_mock_hit({}))
        self.assertFalse(is_mock_hit({"description": ""}))

    def test_openai_prompt_uses_job_title_only(self) -> None:
        prompt = openai_title_vowel_prompt({"title": "Analyst", "description": "Engineer needed"})

        self.assertIn("Return JSON only", prompt)
        self.assertIn("Job title: Analyst", prompt)
        self.assertNotIn("Engineer needed", prompt)

    def test_parse_hit_response_requires_boolean_hit(self) -> None:
        self.assertTrue(parse_hit_response('{"hit": true}'))
        self.assertFalse(parse_hit_response('{"hit": false}'))
        with self.assertRaisesRegex(ValueError, "boolean 'hit'"):
            parse_hit_response('{"hit": "true"}')

    def test_extract_response_text_reads_responses_output_shape(self) -> None:
        payload = {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {"type": "output_text", "text": '{"hit": true}'},
                    ],
                }
            ]
        }

        self.assertEqual(extract_response_text(payload), '{"hit": true}')

    def test_extract_response_text_accepts_text_field_without_output_text_type(self) -> None:
        payload = {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {"type": "text", "text": '{"hit": false}'},
                    ],
                }
            ]
        }

        self.assertEqual(extract_response_text(payload), '{"hit": false}')

    def test_missing_openai_output_text_reports_response_shape(self) -> None:
        payload = {
            "id": "resp_123",
            "status": "incomplete",
            "model": DEFAULT_OPENAI_MODEL,
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [{"type": "reasoning"}],
        }

        summary = summarize_response_shape(payload)

        self.assertIn('"status": "incomplete"', summary)
        self.assertIn('"reason": "max_output_tokens"', summary)
        self.assertIn('"reasoning"', summary)

    def test_openai_matcher_posts_structured_request_without_network(self) -> None:
        calls = []

        def fake_post(url, payload, headers):
            calls.append((url, payload, headers))
            return {"output_text": '{"hit": true}'}

        self.assertTrue(call_openai_title_vowel_matcher({"title": "Engineer"}, "test-key", post_json=fake_post))

        url, payload, headers = calls[0]
        self.assertEqual(url, OPENAI_RESPONSES_URL)
        self.assertEqual(payload["model"], DEFAULT_OPENAI_MODEL)
        self.assertIn("Job title: Engineer", payload["input"])
        self.assertEqual(payload["reasoning"], {"effort": "minimal"})
        self.assertEqual(payload["text"]["verbosity"], "low")
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")
        self.assertEqual(payload["max_output_tokens"], DEFAULT_OPENAI_MAX_OUTPUT_TOKENS)
        self.assertEqual(headers["Authorization"], "Bearer test-key")

    def test_openai_matcher_errors_with_response_shape_when_no_text(self) -> None:
        def fake_post(url, payload, headers):
            return {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}, "output": []}

        with self.assertRaisesRegex(ValueError, "did not include output text"):
            call_openai_title_vowel_matcher({"title": "Engineer"}, "test-key", post_json=fake_post)

    def test_split_jobs_accepts_custom_matcher(self) -> None:
        jobs = [{"title": "Analyst"}, {"title": "Engineer"}]

        hits, discards = split_jobs(jobs, matcher=lambda job: job["title"].startswith("A"))

        self.assertEqual(hits, [{"title": "Analyst"}])
        self.assertEqual(discards, [{"title": "Engineer"}])

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
