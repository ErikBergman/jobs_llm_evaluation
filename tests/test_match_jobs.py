import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from match_jobs import (
    CHEAT_JOB_ID,
    DEFAULT_OPENAI_MODEL,
    DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
    OPENAI_TRIAGE_MAX_OUTPUT_TOKENS,
    OPENAI_TRIAGE_MIN_OUTPUT_TOKENS,
    OPENAI_RESPONSES_URL,
    assert_cheat_mode_language_model_hit,
    call_openai_structured_json,
    call_openai_title_vowel_decision,
    call_openai_title_vowel_matcher,
    call_openai_unicorn_decision,
    call_openai_unicorn_triage,
    classify_file,
    clear_waiting_room,
    env_flag_enabled,
    extract_response_text,
    input_from_latest,
    is_mock_hit,
    latest_discard_run,
    load_job_profile,
    load_waiting_room_jobs,
    mock_match_decision,
    openai_two_stage_decisions,
    openai_title_vowel_prompt,
    openai_unicorn_triage_prompt,
    openai_unicorn_prompt,
    parse_match_response,
    parse_hit_response,
    parse_triage_response,
    profile_cache_key,
    prepare_waiting_room_input,
    rtf_to_text,
    response_usage,
    split_jobs,
    split_jobs_from_decisions,
    split_jobs_with_decisions,
    summarize_response_shape,
    timestamp_from_input,
    triage_max_output_tokens,
    waiting_room_root,
)


class MockMatcherTests(unittest.TestCase):
    def test_engineer_word_is_hit(self) -> None:
        self.assertTrue(is_mock_hit({"description": "Engineer needed"}))

    def test_engineering_is_not_hit(self) -> None:
        self.assertFalse(is_mock_hit({"description": "software engineering"}))

    def test_missing_or_empty_description_is_discard(self) -> None:
        self.assertFalse(is_mock_hit({}))
        self.assertFalse(is_mock_hit({"description": ""}))

    def test_env_flag_enabled_accepts_github_variable_true(self) -> None:
        self.assertTrue(env_flag_enabled("CHEAT_MODE", {"CHEAT_MODE": "true"}))
        self.assertTrue(env_flag_enabled("CHEAT_MODE", {"CHEAT_MODE": "1"}))
        self.assertFalse(env_flag_enabled("CHEAT_MODE", {"CHEAT_MODE": "false"}))
        self.assertFalse(env_flag_enabled("CHEAT_MODE", {}))

    def test_load_job_profile_reads_plain_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "profile.txt"
            profile_path.write_text("Candidate profile", encoding="utf-8")

            self.assertEqual(load_job_profile(profile_path), "Candidate profile")

    def test_load_job_profile_extracts_rtf_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "profile.rtf"
            profile_path.write_text(r"{\rtf1 Candidate\par Profile with \u246?}", encoding="utf-8")

            self.assertEqual(load_job_profile(profile_path), "Candidate\nProfile with ö")

    def test_profile_cache_key_is_stable_and_opaque(self) -> None:
        cache_key = profile_cache_key("Candidate profile")

        self.assertEqual(cache_key, profile_cache_key("Candidate profile"))
        self.assertNotEqual(cache_key, profile_cache_key("Different profile"))
        self.assertTrue(cache_key.startswith("job-profile:"))
        self.assertNotIn("Candidate", cache_key)

    def test_triage_max_output_tokens_scales_with_job_count(self) -> None:
        self.assertEqual(triage_max_output_tokens(0), OPENAI_TRIAGE_MIN_OUTPUT_TOKENS)
        self.assertEqual(triage_max_output_tokens(1), 280)
        self.assertEqual(triage_max_output_tokens(5), 760)
        self.assertEqual(triage_max_output_tokens(100), OPENAI_TRIAGE_MAX_OUTPUT_TOKENS)

    def test_rtf_to_text_ignores_rtf_metadata(self) -> None:
        profile = r"{\rtf1{\fonttbl{\f0 Helvetica;}}{\info{\title Hidden}}{\*\generator Hidden;}Visible\line Text}"

        self.assertEqual(rtf_to_text(profile).strip(), "Visible\nText")

    def test_openai_unicorn_prompt_uses_profile_and_full_job_ad(self) -> None:
        prompt = openai_unicorn_prompt(
            {
                "title": "Principal Safety Engineer",
                "company": "Example AB",
                "location": "Lund",
                "description": "Build regulated medical devices.",
            },
            "Candidate has regulated medical device and product leadership experience.",
        )

        self.assertIn("rare 'unicorn' match", prompt)
        self.assertIn("Prefer false unless the fit is unusually strong", prompt)
        self.assertIn("Candidate has regulated medical device", prompt)
        self.assertIn("Title: Principal Safety Engineer", prompt)
        self.assertIn("Company: Example AB", prompt)
        self.assertIn("Location: Lund", prompt)
        self.assertIn("Description: Build regulated medical devices.", prompt)

    def test_openai_unicorn_triage_prompt_batches_jobs_with_profile_once(self) -> None:
        prompt = openai_unicorn_triage_prompt(
            [
                {"job_id": "1", "title": "Principal Safety Engineer", "description": "Build regulated devices."},
                {"job_id": "2", "title": "Frontend Engineer", "description": "Maintain web UI."},
            ],
            "Candidate has regulated product leadership experience.",
        )

        self.assertIn("Read the candidate profile once", prompt)
        self.assertEqual(prompt.count("Candidate has regulated product leadership experience."), 1)
        self.assertIn('"candidate_id": "1"', prompt)
        self.assertIn('"candidate_id": "2"', prompt)
        self.assertIn("temporary candidate list", prompt)

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

    def test_parse_match_response_requires_reason(self) -> None:
        self.assertEqual(parse_match_response('{"hit": true, "reason": "Starts with E."}'), (True, "Starts with E."))
        with self.assertRaisesRegex(ValueError, "non-empty string 'reason'"):
            parse_match_response('{"hit": true, "reason": ""}')

    def test_parse_triage_response_validates_candidate_ids(self) -> None:
        parsed = parse_triage_response(
            '{"candidates": [{"candidate_id": "1", "reason": "Strong overlap."}]}',
            {"1", "2"},
        )

        self.assertEqual(parsed, [{"candidate_id": "1", "reason": "Strong overlap."}])
        with self.assertRaisesRegex(ValueError, "unknown candidate_id"):
            parse_triage_response('{"candidates": [{"candidate_id": "3", "reason": "Nope."}]}', {"1", "2"})

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

    def test_response_usage_returns_usage_object_only(self) -> None:
        usage = {"input_tokens": 10, "output_tokens": 2}

        self.assertEqual(response_usage({"usage": usage}), usage)
        self.assertIsNone(response_usage({"usage": []}))
        self.assertIsNone(response_usage({}))

    def test_openai_matcher_posts_structured_request_without_network(self) -> None:
        calls = []

        def fake_post(url, payload, headers):
            calls.append((url, payload, headers))
            return {"output_text": '{"hit": true, "reason": "Starts with E."}'}

        self.assertTrue(call_openai_title_vowel_matcher({"title": "Engineer"}, "test-key", post_json=fake_post))

        url, payload, headers = calls[0]
        self.assertEqual(url, OPENAI_RESPONSES_URL)
        self.assertEqual(payload["model"], DEFAULT_OPENAI_MODEL)
        self.assertIn("Job title: Engineer", payload["input"])
        self.assertEqual(payload["reasoning"], {"effort": "low"})
        self.assertEqual(payload["text"]["verbosity"], "low")
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")
        self.assertEqual(set(payload["text"]["format"]["schema"]["required"]), {"hit", "reason"})
        self.assertEqual(payload["max_output_tokens"], DEFAULT_OPENAI_MAX_OUTPUT_TOKENS)
        self.assertNotIn("prompt_cache_key", payload)
        self.assertEqual(headers["Authorization"], "Bearer test-key")

    def test_openai_structured_json_retries_when_output_tokens_are_exhausted(self) -> None:
        calls = []

        def fake_post(url, payload, headers):
            calls.append((url, payload, headers))
            if len(calls) == 1:
                return {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}, "output": []}
            return {"output_text": '{"hit": false, "reason": "Retry succeeded."}'}

        response_payload, response_text = call_openai_structured_json(
            "test-key",
            "Classify this job.",
            "job_unicorn_match",
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {"hit": {"type": "boolean"}, "reason": {"type": "string"}},
                "required": ["hit", "reason"],
            },
            max_output_tokens=10,
            post_json=fake_post,
        )

        self.assertEqual([call[1]["max_output_tokens"] for call in calls], [10, 20])
        self.assertEqual(response_payload["output_text"], '{"hit": false, "reason": "Retry succeeded."}')
        self.assertEqual(response_text, '{"hit": false, "reason": "Retry succeeded."}')

    def test_openai_unicorn_decision_posts_profile_based_request_without_network(self) -> None:
        calls = []

        def fake_post(url, payload, headers):
            calls.append((url, payload, headers))
            return {
                "model": "gpt-5.4-mini-test",
                "usage": {"input_tokens": 100, "output_tokens": 20},
                "output_text": '{"hit": true, "reason": "Rare overlap."}',
            }

        decision = call_openai_unicorn_decision(
            {"job_id": "1", "title": "Systems Lead", "description": "Own regulated product strategy."},
            "test-key",
            "Candidate profile: regulated products plus systems leadership.",
            model="gpt-5.4-mini",
            post_json=fake_post,
        )

        url, payload, headers = calls[0]
        self.assertEqual(url, OPENAI_RESPONSES_URL)
        self.assertEqual(payload["model"], "gpt-5.4-mini")
        self.assertIn("Candidate profile: regulated products", payload["input"])
        self.assertIn("Title: Systems Lead", payload["input"])
        self.assertEqual(payload["text"]["format"]["name"], "job_unicorn_match")
        self.assertNotIn("prompt_cache_key", payload)
        self.assertEqual(headers["Authorization"], "Bearer test-key")
        self.assertTrue(decision["hit"])
        self.assertEqual(decision["reason"], "Rare overlap.")
        self.assertEqual(decision["matcher"], "openai")
        self.assertEqual(decision["model"], "gpt-5.4-mini-test")
        self.assertEqual(decision["usage"], {"input_tokens": 100, "output_tokens": 20})

    def test_openai_triage_posts_batched_request_without_network(self) -> None:
        calls = []

        def fake_post(url, payload, headers):
            calls.append((url, payload, headers))
            return {
                "model": "gpt-5.4-mini-test",
                "usage": {"input_tokens": 400, "output_tokens": 60},
                "output_text": '{"candidates": [{"candidate_id": "1", "reason": "Worth confirmation."}]}',
            }

        triage = call_openai_unicorn_triage(
            [
                {"job_id": "1", "title": "Systems Lead"},
                {"job_id": "2", "title": "Support Engineer"},
            ],
            "test-key",
            "Candidate profile",
            model="gpt-5.4-mini",
            post_json=fake_post,
        )

        url, payload, headers = calls[0]
        self.assertEqual(url, OPENAI_RESPONSES_URL)
        self.assertEqual(payload["model"], "gpt-5.4-mini")
        self.assertEqual(payload["text"]["format"]["name"], "job_unicorn_triage")
        self.assertEqual(payload["max_output_tokens"], triage_max_output_tokens(2))
        self.assertNotIn("prompt_cache_key", payload)
        self.assertIn('"candidate_id": "1"', payload["input"])
        self.assertIn('"candidate_id": "2"', payload["input"])
        self.assertEqual(headers["Authorization"], "Bearer test-key")
        self.assertEqual(triage["candidates"], [{"candidate_id": "1", "reason": "Worth confirmation."}])
        self.assertEqual(triage["model"], "gpt-5.4-mini-test")
        self.assertEqual(triage["usage"], {"input_tokens": 400, "output_tokens": 60})

    def test_openai_two_stage_decisions_confirms_only_triage_candidates(self) -> None:
        calls = []

        def fake_post(url, payload, headers):
            calls.append(payload)
            if payload["text"]["format"]["name"] == "job_unicorn_triage":
                return {
                    "model": "gpt-5.4-mini-test",
                    "usage": {"input_tokens": 500, "output_tokens": 50},
                    "output_text": '{"candidates": [{"candidate_id": "1", "reason": "Rare domain overlap."}]}',
                }
            return {
                "model": "gpt-5.4-mini-test",
                "usage": {"input_tokens": 300, "output_tokens": 30},
                "output_text": '{"hit": true, "reason": "Confirmed rare fit."}',
            }

        decisions = openai_two_stage_decisions(
            [
                {"job_id": "1", "title": "Systems Lead", "description": "Regulated systems."},
                {"job_id": "2", "title": "Support Engineer", "description": "Customer tickets."},
            ],
            "test-key",
            "Candidate profile",
            model="gpt-5.4-mini",
            post_json=fake_post,
        )

        self.assertEqual([call["text"]["format"]["name"] for call in calls], ["job_unicorn_triage", "job_unicorn_match"])
        self.assertEqual(calls[0]["prompt_cache_key"], profile_cache_key("Candidate profile"))
        self.assertEqual(calls[1]["prompt_cache_key"], profile_cache_key("Candidate profile"))
        self.assertEqual([decision["hit"] for decision in decisions], [True, False])
        self.assertEqual(decisions[0]["stage"], "confirmation")
        self.assertEqual(decisions[0]["triage_reason"], "Rare domain overlap.")
        self.assertEqual(decisions[1]["stage"], "triage")
        self.assertEqual(decisions[1]["reason"], "Rejected by first-stage OpenAI triage.")
        self.assertEqual(decisions[0]["triage_candidate_count"], 1)
        self.assertEqual(decisions[1]["triage_candidate_count"], 1)
        self.assertEqual(decisions[0]["usage"], {"input_tokens": 300, "output_tokens": 30})
        self.assertEqual(decisions[0]["triage_usage"], {"input_tokens": 500, "output_tokens": 50})
        self.assertIsNone(decisions[1]["usage"])
        self.assertEqual(decisions[1]["triage_usage"], {"input_tokens": 500, "output_tokens": 50})

    def test_openai_decision_keeps_reason_and_raw_response(self) -> None:
        def fake_post(url, payload, headers):
            return {
                "model": "gpt-5-nano-test",
                "usage": {"input_tokens": 22, "output_tokens": 8},
                "output_text": '{"hit": false, "reason": "Starts with S."}',
            }

        decision = call_openai_title_vowel_decision(
            {"job_id": "1", "title": "Software Engineer"},
            "test-key",
            post_json=fake_post,
        )

        self.assertEqual(
            decision,
            {
                "job_id": "1",
                "title": "Software Engineer",
                "hit": False,
                "reason": "Starts with S.",
                "matcher": "openai",
                "model": "gpt-5-nano-test",
                "usage": {"input_tokens": 22, "output_tokens": 8},
                "raw_response": '{"hit": false, "reason": "Starts with S."}',
            },
        )

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

    def test_split_jobs_with_decisions_returns_audit_data(self) -> None:
        jobs = [{"job_id": "1", "title": "Analyst", "description": "Engineer needed"}]

        hits, discards, decisions = split_jobs_with_decisions(jobs, decisioner=mock_match_decision)

        self.assertEqual(hits, jobs)
        self.assertEqual(discards, [])
        self.assertEqual(decisions[0]["job_id"], "1")
        self.assertTrue(decisions[0]["hit"])
        self.assertEqual(decisions[0]["matcher"], "mock")

    def test_split_jobs_from_decisions_requires_same_count(self) -> None:
        jobs = [{"job_id": "1"}, {"job_id": "2"}]

        hits, discards = split_jobs_from_decisions(jobs, [{"hit": True}, {"hit": False}])

        self.assertEqual(hits, [{"job_id": "1"}])
        self.assertEqual(discards, [{"job_id": "2"}])
        with self.assertRaisesRegex(ValueError, "Decision count"):
            split_jobs_from_decisions(jobs, [{"hit": True}])

    def test_cheat_mode_language_model_hit_logs_and_passes(self) -> None:
        metadata = {
            "decisions": [
                {
                    "job_id": CHEAT_JOB_ID,
                    "title": "Perfect Job",
                    "hit": True,
                    "reason": "Strong fit.",
                    "matcher": "openai",
                }
            ]
        }

        output = io.StringIO()
        with redirect_stdout(output):
            assert_cheat_mode_language_model_hit(metadata)

        self.assertIn("[cheat-mode]", output.getvalue())
        self.assertIn("language_model_hit=true", output.getvalue())
        self.assertIn("Strong fit.", output.getvalue())

    def test_cheat_mode_language_model_miss_logs_and_errors(self) -> None:
        metadata = {
            "decisions": [
                {
                    "job_id": CHEAT_JOB_ID,
                    "title": "Perfect Job",
                    "hit": False,
                    "reason": "Rejected by triage.",
                    "matcher": "openai",
                }
            ]
        }

        output = io.StringIO()
        with redirect_stdout(output):
            with self.assertRaisesRegex(ValueError, "did not classify the cheat job as a match"):
                assert_cheat_mode_language_model_hit(metadata)

        self.assertIn("language_model_hit=false", output.getvalue())
        self.assertIn("Rejected by triage.", output.getvalue())

    def test_cheat_mode_requires_language_model_matcher(self) -> None:
        metadata = {
            "decisions": [
                {
                    "job_id": CHEAT_JOB_ID,
                    "title": "Perfect Job",
                    "hit": True,
                    "reason": "Mock hit.",
                    "matcher": "mock",
                }
            ]
        }

        with redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "requires the language-model matcher"):
                assert_cheat_mode_language_model_hit(metadata)

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

            hits_path, discards_path, metadata_path, metadata = classify_file(input_path, root / "results")

            hits = json.loads(hits_path.read_text(encoding="utf-8"))
            discards = json.loads(discards_path.read_text(encoding="utf-8"))
            metadata_json = json.loads(metadata_path.read_text(encoding="utf-8"))

        self.assertEqual(hits_path, root / "results" / "hits" / "20260425_120000" / "jobs_hits.json")
        self.assertEqual(discards_path, root / "results" / "discard" / "20260425_120000" / "jobs_discard.json")
        self.assertEqual(metadata_path, root / "results" / "discard" / "20260425_120000" / "jobs_match_metadata.json")
        self.assertEqual([job["job_id"] for job in hits], ["1"])
        self.assertEqual([job["job_id"] for job in discards], ["2", "3"])
        self.assertEqual(metadata["hits_count"], 1)
        self.assertEqual(metadata_json["discards_count"], 2)
        self.assertEqual([decision["job_id"] for decision in metadata_json["decisions"]], ["1", "2", "3"])

    def test_waiting_room_jobs_are_deduped_and_prepared_for_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "results"
            older = root / "waiting_room" / "20260425_120000"
            newer = root / "waiting_room" / "20260425_130000"
            older.mkdir(parents=True)
            newer.mkdir()
            (older / "jobs.json").write_text(
                json.dumps([
                    {"job_id": "1", "description": "Engineer needed"},
                    {"job_id": "2", "description": "Analyst"},
                ]),
                encoding="utf-8",
            )
            (newer / "jobs.json").write_text(
                json.dumps([
                    {"job_id": "2", "description": "Duplicate"},
                    {"job_id": "3", "description": "Engineer needed"},
                ]),
                encoding="utf-8",
            )

            jobs = load_waiting_room_jobs(root)
            input_path = prepare_waiting_room_input(root, "20260425_190000")

            self.assertEqual([job["job_id"] for job in jobs], ["1", "2", "3"])
            self.assertEqual(input_path, root / "discard" / "20260425_190000" / "waiting_room_jobs.json")
            self.assertEqual(
                [job["job_id"] for job in json.loads(input_path.read_text(encoding="utf-8"))],
                ["1", "2", "3"],
            )

    def test_clear_waiting_room_wipes_bucket_but_leaves_empty_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "results"
            room = waiting_room_root(root)
            (room / "20260425_120000").mkdir(parents=True)
            (room / "20260425_120000" / "jobs.json").write_text("[]", encoding="utf-8")

            clear_waiting_room(root)

            self.assertTrue(room.exists())
            self.assertEqual(list(room.iterdir()), [])

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
            (newer / "newer_match_metadata.json").write_text("{}", encoding="utf-8")

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
