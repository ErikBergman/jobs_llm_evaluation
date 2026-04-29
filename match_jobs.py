#!/usr/bin/env python3
"""Classify scraped job ads with a mock or OpenAI-backed matcher."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ENGINEER_WORD = re.compile(r"\bengineer\b", re.IGNORECASE)
DEFAULT_OUTPUT_ROOT = Path("results")
DEFAULT_MATCHER = "mock"
DEFAULT_OPENAI_MODEL = "gpt-5.4-mini"
DEFAULT_OPENAI_MAX_OUTPUT_TOKENS = 512
CHEAT_JOB_ID = "cheat-mode-perfect-job"
OPENAI_TRIAGE_BASE_OUTPUT_TOKENS = 160
OPENAI_TRIAGE_OUTPUT_TOKENS_PER_JOB = 120
OPENAI_TRIAGE_MIN_OUTPUT_TOKENS = 256
OPENAI_TRIAGE_MAX_OUTPUT_TOKENS = 2048
OPENAI_STRUCTURED_JSON_REASONING_EFFORT = "low"
OPENAI_RETRY_MAX_OUTPUT_TOKENS = 4096
DEFAULT_JOB_PROFILE_PATH = Path("job_profile.txt")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
RTF_DESTINATIONS = {
    "fonttbl",
    "colortbl",
    "datastore",
    "stylesheet",
    "info",
    "pict",
    "object",
    "header",
    "footer",
    "footnote",
}


def is_mock_hit(job: dict[str, Any]) -> bool:
    description = job.get("description", "")
    if not isinstance(description, str):
        return False
    return bool(ENGINEER_WORD.search(description))


def env_flag_enabled(name: str, environ: dict[str, str] | None = None) -> bool:
    value = (os.environ if environ is None else environ).get(name, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def rtf_to_text(rtf: str) -> str:
    output: list[str] = []
    ignored_groups = [False]
    pending_ignorable_destination = False
    unicode_skip_count = 1
    chars_to_skip = 0
    index = 0

    while index < len(rtf):
        char = rtf[index]
        if chars_to_skip:
            chars_to_skip -= 1
            index += 1
            continue

        if char == "{":
            ignored_groups.append(ignored_groups[-1])
            pending_ignorable_destination = False
            index += 1
            continue
        if char == "}":
            if len(ignored_groups) > 1:
                ignored_groups.pop()
            pending_ignorable_destination = False
            index += 1
            continue
        if char != "\\":
            if not ignored_groups[-1]:
                output.append(char)
            index += 1
            continue

        index += 1
        if index >= len(rtf):
            break

        escaped = rtf[index]
        if escaped in "\\{}":
            if not ignored_groups[-1]:
                output.append(escaped)
            index += 1
            continue
        if escaped == "'":
            hex_value = rtf[index + 1:index + 3]
            if len(hex_value) == 2:
                try:
                    if not ignored_groups[-1]:
                        output.append(bytes.fromhex(hex_value).decode("latin-1"))
                    index += 3
                    continue
                except ValueError:
                    pass
        if escaped == "~":
            if not ignored_groups[-1]:
                output.append(" ")
            index += 1
            continue
        if escaped in "-_":
            index += 1
            continue
        if escaped == "*":
            ignored_groups[-1] = True
            pending_ignorable_destination = True
            index += 1
            continue
        if not escaped.isalpha():
            index += 1
            continue

        word_start = index
        while index < len(rtf) and rtf[index].isalpha():
            index += 1
        word = rtf[word_start:index]
        sign = 1
        if index < len(rtf) and rtf[index] == "-":
            sign = -1
            index += 1
        number_start = index
        while index < len(rtf) and rtf[index].isdigit():
            index += 1
        number = rtf[number_start:index]
        numeric_value = sign * int(number) if number else None
        if index < len(rtf) and rtf[index] == " ":
            index += 1

        if pending_ignorable_destination or word in RTF_DESTINATIONS:
            ignored_groups[-1] = True
            pending_ignorable_destination = False
            continue
        pending_ignorable_destination = False

        if ignored_groups[-1]:
            continue
        if word in ("par", "line"):
            output.append("\n")
        elif word == "tab":
            output.append("\t")
        elif word == "uc" and numeric_value is not None:
            unicode_skip_count = max(numeric_value, 0)
        elif word == "u" and numeric_value is not None:
            output.append(chr(numeric_value if numeric_value >= 0 else numeric_value + 65536))
            chars_to_skip = unicode_skip_count

    return "".join(output)


def load_job_profile(profile_path: Path) -> str:
    if not profile_path.exists():
        raise ValueError(f"Job profile not found: {profile_path}")
    with open(profile_path, encoding="utf-8") as profile_file:
        raw_profile = profile_file.read()
    profile = rtf_to_text(raw_profile).strip() if raw_profile.lstrip().startswith("{\\rtf") else raw_profile.strip()
    if not profile:
        raise ValueError(f"{profile_path} is empty")
    return profile


def profile_cache_key(profile: str) -> str:
    profile_hash = hashlib.sha256(profile.encode("utf-8")).hexdigest()[:16]
    return f"job-profile:{profile_hash}"


def triage_max_output_tokens(job_count: int) -> int:
    scaled_limit = OPENAI_TRIAGE_BASE_OUTPUT_TOKENS + max(job_count, 0) * OPENAI_TRIAGE_OUTPUT_TOKENS_PER_JOB
    return min(OPENAI_TRIAGE_MAX_OUTPUT_TOKENS, max(OPENAI_TRIAGE_MIN_OUTPUT_TOKENS, scaled_limit))


def job_ad_text(job: dict[str, Any]) -> str:
    fields = [
        ("Title", job.get("title", "")),
        ("Company", job.get("company", "")),
        ("Location", job.get("location", "")),
        ("Description", job.get("description", "")),
    ]
    return "\n".join(f"{label}: {value}" for label, value in fields if isinstance(value, str) and value.strip())


def job_candidate_id(job: dict[str, Any], index: int) -> str:
    job_id = job.get("job_id")
    if isinstance(job_id, str) and job_id:
        return job_id
    if isinstance(job_id, int):
        return str(job_id)
    return f"index-{index}"


def compact_job_ad(job: dict[str, Any], candidate_id: str) -> dict[str, str]:
    fields = {
        "candidate_id": candidate_id,
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "description": job.get("description", ""),
    }
    return {key: value for key, value in fields.items() if isinstance(value, str) and value.strip()}


def openai_unicorn_prompt(job: dict[str, Any], profile: str) -> str:
    return (
        "You are a strict job-match classifier. Determine whether the job ad is a rare 'unicorn' match "
        "for the candidate profile. A unicorn match should be a small fraction of jobs, only when the ad "
        "strongly combines several of the candidate's distinctive strengths, background, and preferences. "
        "Do not mark a job as a hit merely because it contains one matching keyword, a generic engineering title, "
        "or one isolated overlap. Prefer false unless the fit is unusually strong.\n\n"
        "Return JSON only with:\n"
        "- hit: boolean\n"
        "- reason: one concise sentence explaining the decision\n\n"
        f"Candidate profile:\n{profile}\n\n"
        f"Job ad:\n{job_ad_text(job)}"
    )


def openai_unicorn_confirmation_prompt(job: dict[str, Any], profile: str) -> str:
    return (
        "You are a strict second-stage job-match classifier. This is a fresh, stateless review of one job ad "
        "that passed a broad first-stage triage. Determine whether it is truly a rare 'unicorn' match for the "
        "candidate profile. Only set hit=true when the ad strongly combines several distinctive parts of the "
        "candidate profile and looks unusually well suited. Prefer false for generic matches, plausible-but-normal "
        "matches, or jobs that only overlap on common keywords.\n\n"
        "Return JSON only with:\n"
        "- hit: boolean\n"
        "- reason: one concise sentence explaining the decision\n\n"
        f"Candidate profile:\n{profile}\n\n"
        f"Job ad:\n{job_ad_text(job)}"
    )


def openai_unicorn_triage_prompt(jobs: list[dict[str, Any]], profile: str) -> str:
    ads = [
        compact_job_ad(job, job_candidate_id(job, index))
        for index, job in enumerate(jobs)
    ]
    return (
        "You are doing first-stage triage for a strict job-match workflow. Read the candidate profile once, "
        "then scan all job ads. Build a temporary candidate list containing only jobs that might plausibly be "
        "rare 'unicorn' matches after stricter review. Be selective: generic engineering jobs, ordinary keyword "
        "matches, and weak single-factor overlaps should not be candidates. This stage may include uncertain but "
        "promising ads, but it should still keep the list small.\n\n"
        "Return JSON only with a candidates array. Each candidate must include:\n"
        "- candidate_id: exactly one candidate_id from the input ads\n"
        "- reason: one concise sentence explaining why it deserves second-stage review\n\n"
        f"Candidate profile:\n{profile}\n\n"
        "Job ads JSON:\n"
        f"{json.dumps(ads, ensure_ascii=False, indent=2)}"
    )


def openai_title_vowel_prompt(job: dict[str, Any]) -> str:
    title = job.get("title", "")
    if not isinstance(title, str):
        title = ""
    return (
        "Return JSON only. Set hit=true if the first letter of the job title is A, E, I, O, or U. "
        "Otherwise set hit=false. Also return a short reason explaining the decision in one sentence.\n\n"
        f"Job title: {title}"
    )


def extract_response_text(response_payload: dict[str, Any]) -> str:
    if isinstance(response_payload.get("output_text"), str):
        return response_payload["output_text"]

    chunks: list[str] = []
    for item in response_payload.get("output", []):
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []):
            if isinstance(content, dict) and content.get("type") == "output_text":
                text = content.get("text")
                if isinstance(text, str):
                    chunks.append(text)
            elif isinstance(content, dict) and isinstance(content.get("text"), str):
                chunks.append(content["text"])
    return "".join(chunks)


def summarize_response_shape(response_payload: dict[str, Any]) -> str:
    summary = {
        "id": response_payload.get("id"),
        "status": response_payload.get("status"),
        "model": response_payload.get("model"),
        "incomplete_details": response_payload.get("incomplete_details"),
        "error": response_payload.get("error"),
        "output_types": [
            item.get("type")
            for item in response_payload.get("output", [])
            if isinstance(item, dict)
        ],
    }
    return json.dumps(summary, ensure_ascii=False)


def response_usage(response_payload: dict[str, Any]) -> dict[str, Any] | None:
    usage = response_payload.get("usage")
    return usage if isinstance(usage, dict) else None


def parse_match_response(response_text: str) -> tuple[bool, str]:
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError as error:
        raise ValueError(f"OpenAI response was not valid JSON: {response_text!r}") from error

    hit = payload.get("hit")
    if not isinstance(hit, bool):
        raise ValueError(f"OpenAI response JSON must contain boolean 'hit': {response_text!r}")

    reason = payload.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError(f"OpenAI response JSON must contain non-empty string 'reason': {response_text!r}")
    return hit, reason.strip()


def parse_triage_response(response_text: str, valid_candidate_ids: set[str]) -> list[dict[str, str]]:
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError as error:
        raise ValueError(f"OpenAI triage response was not valid JSON: {response_text!r}") from error

    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError(f"OpenAI triage response JSON must contain array 'candidates': {response_text!r}")

    parsed: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            raise ValueError(f"OpenAI triage candidate must be an object: {response_text!r}")
        candidate_id = candidate.get("candidate_id")
        reason = candidate.get("reason")
        if not isinstance(candidate_id, str) or candidate_id not in valid_candidate_ids:
            raise ValueError(f"OpenAI triage returned unknown candidate_id: {candidate_id!r}")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(f"OpenAI triage candidate must contain non-empty string 'reason': {response_text!r}")
        if candidate_id in seen:
            continue
        seen.add(candidate_id)
        parsed.append({"candidate_id": candidate_id, "reason": reason.strip()})
    return parsed


def parse_hit_response(response_text: str) -> bool:
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError as error:
        raise ValueError(f"OpenAI response was not valid JSON: {response_text!r}") from error

    hit = payload.get("hit")
    if not isinstance(hit, bool):
        raise ValueError(f"OpenAI response JSON must contain boolean 'hit': {response_text!r}")
    return hit


def mock_match_decision(job: dict[str, Any]) -> dict[str, Any]:
    hit = is_mock_hit(job)
    return {
        "job_id": job.get("job_id"),
        "title": job.get("title"),
        "hit": hit,
        "reason": (
            "Description contains the standalone word 'engineer'."
            if hit
            else "Description does not contain the standalone word 'engineer'."
        ),
        "matcher": "mock",
    }


def call_openai_structured_json(
    api_key: str,
    prompt: str,
    schema_name: str,
    schema: dict[str, Any],
    model: str = DEFAULT_OPENAI_MODEL,
    max_output_tokens: int = DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
    prompt_cache_key: str | None = None,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], str]:
    if not api_key:
        raise ValueError("OPENAI_API_KEY is required when --matcher openai is used")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def request_structured_json(output_tokens: int) -> dict[str, Any]:
        payload = {
            "model": model,
            "input": prompt,
            "reasoning": {"effort": OPENAI_STRUCTURED_JSON_REASONING_EFFORT},
            "text": {
                "verbosity": "low",
                "format": {
                    "type": "json_schema",
                    "name": schema_name,
                    "strict": True,
                    "schema": schema,
                }
            },
            "max_output_tokens": output_tokens,
        }
        if prompt_cache_key:
            payload["prompt_cache_key"] = prompt_cache_key
        return (post_json or post_json_to_api)(OPENAI_RESPONSES_URL, payload, headers)

    response_payload = request_structured_json(max_output_tokens)
    response_text = extract_response_text(response_payload)
    if (
        not response_text
        and response_payload.get("incomplete_details", {}).get("reason") == "max_output_tokens"
        and max_output_tokens < OPENAI_RETRY_MAX_OUTPUT_TOKENS
    ):
        retry_output_tokens = min(max_output_tokens * 2, OPENAI_RETRY_MAX_OUTPUT_TOKENS)
        response_payload = request_structured_json(retry_output_tokens)
        response_text = extract_response_text(response_payload)
    if not response_text:
        raise ValueError(f"OpenAI response did not include output text: {summarize_response_shape(response_payload)}")
    return response_payload, response_text


def match_decision_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "hit": {"type": "boolean"},
            "reason": {"type": "string"},
        },
        "required": ["hit", "reason"],
    }


def triage_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "candidate_id": {"type": "string"},
                        "reason": {"type": "string"},
                    },
                    "required": ["candidate_id", "reason"],
                },
            },
        },
        "required": ["candidates"],
    }


def call_openai_match_decision(
    job: dict[str, Any],
    api_key: str,
    prompt: str,
    schema_name: str,
    model: str = DEFAULT_OPENAI_MODEL,
    prompt_cache_key: str | None = None,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    response_payload, response_text = call_openai_structured_json(
        api_key,
        prompt,
        schema_name,
        match_decision_schema(),
        model=model,
        prompt_cache_key=prompt_cache_key,
        post_json=post_json,
    )
    hit, reason = parse_match_response(response_text)
    return {
        "job_id": job.get("job_id"),
        "title": job.get("title"),
        "hit": hit,
        "reason": reason,
        "matcher": "openai",
        "model": response_payload.get("model", model),
        "usage": response_usage(response_payload),
        "raw_response": response_text,
    }


def call_openai_unicorn_decision(
    job: dict[str, Any],
    api_key: str,
    profile: str,
    model: str = DEFAULT_OPENAI_MODEL,
    prompt_cache_key: str | None = None,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return call_openai_match_decision(
        job,
        api_key,
        openai_unicorn_confirmation_prompt(job, profile),
        "job_unicorn_match",
        model=model,
        prompt_cache_key=prompt_cache_key,
        post_json=post_json,
    )


def call_openai_unicorn_triage(
    jobs: list[dict[str, Any]],
    api_key: str,
    profile: str,
    model: str = DEFAULT_OPENAI_MODEL,
    prompt_cache_key: str | None = None,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    candidate_ids = {
        job_candidate_id(job, index)
        for index, job in enumerate(jobs)
    }
    response_payload, response_text = call_openai_structured_json(
        api_key,
        openai_unicorn_triage_prompt(jobs, profile),
        "job_unicorn_triage",
        triage_schema(),
        model=model,
        max_output_tokens=triage_max_output_tokens(len(jobs)),
        prompt_cache_key=prompt_cache_key,
        post_json=post_json,
    )
    candidates = parse_triage_response(response_text, candidate_ids)
    return {
        "candidates": candidates,
        "model": response_payload.get("model", model),
        "usage": response_usage(response_payload),
        "raw_response": response_text,
    }


def openai_two_stage_decisions(
    jobs: list[dict[str, Any]],
    api_key: str,
    profile: str,
    model: str = DEFAULT_OPENAI_MODEL,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    cache_key = profile_cache_key(profile)
    triage = call_openai_unicorn_triage(
        jobs,
        api_key,
        profile,
        model=model,
        prompt_cache_key=cache_key,
        post_json=post_json,
    )
    triage_reasons = {
        candidate["candidate_id"]: candidate["reason"]
        for candidate in triage["candidates"]
    }
    candidate_id_by_index = {
        index: job_candidate_id(job, index)
        for index, job in enumerate(jobs)
    }
    job_by_candidate_id = {
        candidate_id: jobs[index]
        for index, candidate_id in candidate_id_by_index.items()
    }

    confirmation_decisions: dict[str, dict[str, Any]] = {}
    for candidate in triage["candidates"]:
        candidate_id = candidate["candidate_id"]
        confirmation = call_openai_unicorn_decision(
            job_by_candidate_id[candidate_id],
            api_key,
            profile,
            model=model,
            prompt_cache_key=cache_key,
            post_json=post_json,
        )
        confirmation["stage"] = "confirmation"
        confirmation["candidate_id"] = candidate_id
        confirmation["triage_reason"] = candidate["reason"]
        confirmation_decisions[candidate_id] = confirmation

    decisions: list[dict[str, Any]] = []
    for index, job in enumerate(jobs):
        candidate_id = candidate_id_by_index[index]
        if candidate_id in confirmation_decisions:
            decisions.append(confirmation_decisions[candidate_id])
        else:
            decisions.append({
                "job_id": job.get("job_id"),
                "title": job.get("title"),
                "hit": False,
                "reason": "Rejected by first-stage OpenAI triage.",
                "matcher": "openai",
                "model": triage["model"],
                "usage": None,
                "stage": "triage",
                "candidate_id": candidate_id,
            })

    for decision in decisions:
        decision["triage_candidate_count"] = len(triage["candidates"])
        decision["triage_usage"] = triage["usage"]
        if decision.get("candidate_id") in triage_reasons:
            decision["triage_reason"] = triage_reasons[decision["candidate_id"]]
    return decisions


def call_openai_title_vowel_decision(
    job: dict[str, Any],
    api_key: str,
    model: str = DEFAULT_OPENAI_MODEL,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return call_openai_match_decision(
        job,
        api_key,
        openai_title_vowel_prompt(job),
        "job_title_vowel_match",
        model=model,
        post_json=post_json,
    )


def call_openai_title_vowel_matcher(
    job: dict[str, Any],
    api_key: str,
    model: str = DEFAULT_OPENAI_MODEL,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> bool:
    return bool(call_openai_title_vowel_decision(job, api_key, model=model, post_json=post_json)["hit"])


def post_json_to_api(url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        detail = error.read().decode("utf-8", "replace")
        raise ValueError(f"OpenAI API request failed with HTTP {error.code}: {detail}") from error
    except URLError as error:
        raise ValueError(f"OpenAI API request failed: {error}") from error


def load_jobs(input_path: Path) -> list[dict[str, Any]]:
    with open(input_path, encoding="utf-8") as input_file:
        jobs = json.load(input_file)
    if not isinstance(jobs, list) or not all(isinstance(job, dict) for job in jobs):
        raise ValueError(f"{input_path} must contain a JSON array of job objects")
    return jobs


def split_jobs(
    jobs: list[dict[str, Any]],
    matcher: Callable[[dict[str, Any]], bool] = is_mock_hit,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    hits: list[dict[str, Any]] = []
    discards: list[dict[str, Any]] = []
    for job in jobs:
        if matcher(job):
            hits.append(job)
        else:
            discards.append(job)
    return hits, discards


def split_jobs_with_decisions(
    jobs: list[dict[str, Any]],
    decisioner: Callable[[dict[str, Any]], dict[str, Any]] = mock_match_decision,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    hits: list[dict[str, Any]] = []
    discards: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for job in jobs:
        decision = decisioner(job)
        decisions.append(decision)
        if decision["hit"]:
            hits.append(job)
        else:
            discards.append(job)
    return hits, discards, decisions


def split_jobs_from_decisions(
    jobs: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if len(jobs) != len(decisions):
        raise ValueError("Decision count must match job count")
    hits: list[dict[str, Any]] = []
    discards: list[dict[str, Any]] = []
    for job, decision in zip(jobs, decisions):
        if decision["hit"]:
            hits.append(job)
        else:
            discards.append(job)
    return hits, discards


def timestamp_from_input(input_path: Path) -> str:
    parts = input_path.parts
    for index in range(len(parts) - 1):
        if parts[index] == "discard":
            return parts[index + 1]
    raise ValueError("--timestamp is required unless --input is under <output-root>/discard/<timestamp>/")


def output_paths(input_path: Path, output_root: Path, timestamp: str) -> tuple[Path, Path, Path]:
    stem = input_path.stem
    return (
        output_root / "hits" / timestamp / f"{stem}_hits.json",
        output_root / "discard" / timestamp / f"{stem}_discard.json",
        output_root / "discard" / timestamp / f"{stem}_match_metadata.json",
    )


def write_json(path: Path, payload: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)


def write_object_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)


def cheat_mode_decision(metadata: dict[str, Any]) -> dict[str, Any] | None:
    decisions = metadata.get("decisions")
    if not isinstance(decisions, list):
        return None
    for decision in decisions:
        if isinstance(decision, dict) and decision.get("job_id") == CHEAT_JOB_ID:
            return decision
    return None


def assert_cheat_mode_language_model_hit(metadata: dict[str, Any]) -> None:
    decision = cheat_mode_decision(metadata)
    if decision is None:
        raise ValueError(f"CHEAT_MODE=true but {CHEAT_JOB_ID!r} was not present in matcher decisions")

    matcher = decision.get("matcher")
    hit = decision.get("hit")
    title = decision.get("title")
    reason = decision.get("reason")
    print(
        "[cheat-mode] "
        f"job_id={CHEAT_JOB_ID} "
        f"language_model_hit={str(hit).lower()} "
        f"matcher={matcher!r} "
        f"title={title!r} "
        f"reason={reason!r}",
        flush=True,
    )

    if matcher != "openai":
        raise ValueError(f"CHEAT_MODE=true requires the language-model matcher, but matcher was {matcher!r}")
    if hit is not True:
        raise ValueError("CHEAT_MODE=true but the language model did not classify the cheat job as a match")


def decisioner_from_args(
    matcher_name: str,
    api_key: str | None,
    model: str,
    profile_path: Path = DEFAULT_JOB_PROFILE_PATH,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    if matcher_name == "mock":
        return mock_match_decision
    if matcher_name == "openai":
        resolved_api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        profile = load_job_profile(profile_path)
        return lambda job: call_openai_unicorn_decision(job, resolved_api_key, profile, model=model)
    raise ValueError(f"Unsupported matcher: {matcher_name}")


def matcher_from_args(matcher_name: str, api_key: str | None, model: str) -> Callable[[dict[str, Any]], bool]:
    decisioner = decisioner_from_args(matcher_name, api_key, model)
    return lambda job: bool(decisioner(job)["hit"])


def classify_file(
    input_path: Path,
    output_root: Path,
    timestamp: str | None = None,
    decisioner: Callable[[dict[str, Any]], dict[str, Any]] = mock_match_decision,
    decisions_provider: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None = None,
    matcher_name: str = DEFAULT_MATCHER,
    model: str | None = None,
) -> tuple[Path, Path, Path, dict[str, Any]]:
    resolved_timestamp = timestamp or timestamp_from_input(input_path)
    jobs = load_jobs(input_path)
    if decisions_provider is None:
        hits, discards, decisions = split_jobs_with_decisions(jobs, decisioner=decisioner)
    else:
        decisions = decisions_provider(jobs)
        hits, discards = split_jobs_from_decisions(jobs, decisions)
    hits_path, discards_path, metadata_path = output_paths(input_path, output_root, resolved_timestamp)
    metadata = {
        "matcher": matcher_name,
        "model": model,
        "matched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "input_path": str(input_path),
        "timestamp": resolved_timestamp,
        "job_count": len(jobs),
        "hits_count": len(hits),
        "discards_count": len(discards),
        "decisions": decisions,
    }
    write_json(hits_path, hits)
    write_json(discards_path, discards)
    write_object_json(metadata_path, metadata)
    return hits_path, discards_path, metadata_path, metadata


def is_unclassified_jobs_json(path: Path) -> bool:
    return path.suffix == ".json" and not path.name.endswith(("_hits.json", "_discard.json", "_match_metadata.json"))


def latest_discard_run(output_root: Path) -> Path:
    discard_root = output_root / "discard"
    if not discard_root.exists():
        raise ValueError(f"No discard runs found under {discard_root}")
    runs = sorted((path for path in discard_root.iterdir() if path.is_dir()), key=lambda path: path.name)
    if not runs:
        raise ValueError(f"No discard runs found under {discard_root}")
    return runs[-1]


def input_from_latest(output_root: Path) -> Path:
    latest_run = latest_discard_run(output_root)
    candidates = sorted(path for path in latest_run.iterdir() if path.is_file() and is_unclassified_jobs_json(path))
    if not candidates:
        raise ValueError(f"No unclassified jobs JSON found in {latest_run}")
    if len(candidates) > 1:
        names = ", ".join(path.name for path in candidates)
        raise ValueError(f"Multiple unclassified jobs JSON files found in {latest_run}: {names}")
    return candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--input", type=Path, help="Scraped jobs JSON file")
    input_group.add_argument("--latest", action="store_true", help="Use the newest results/discard/<timestamp>/ scrape")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Results root directory")
    parser.add_argument("--timestamp", help="Result timestamp folder name")
    parser.add_argument("--matcher", choices=("mock", "openai"), default=DEFAULT_MATCHER, help="Matcher backend")
    parser.add_argument("--openai-model", default=DEFAULT_OPENAI_MODEL, help="OpenAI model for --matcher openai")
    parser.add_argument("--job-profile", type=Path, default=DEFAULT_JOB_PROFILE_PATH, help="Candidate profile text file")
    args = parser.parse_args()

    try:
        input_path = input_from_latest(args.output_root) if args.latest else args.input
        decisioner = mock_match_decision
        decisions_provider = None
        if args.matcher == "openai":
            api_key = os.environ.get("OPENAI_API_KEY", "")
            profile = load_job_profile(args.job_profile)
            decisions_provider = lambda jobs: openai_two_stage_decisions(jobs, api_key, profile, model=args.openai_model)
        hits_path, discards_path, metadata_path, metadata = classify_file(
            input_path,
            args.output_root,
            args.timestamp,
            decisioner=decisioner,
            decisions_provider=decisions_provider,
            matcher_name=args.matcher,
            model=args.openai_model if args.matcher == "openai" else None,
        )
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    for decision in metadata["decisions"]:
        print(
            "[decision] "
            f"job_id={decision.get('job_id')} "
            f"hit={str(decision.get('hit')).lower()} "
            f"title={decision.get('title')!r} "
            f"reason={decision.get('reason')!r}"
        )
    try:
        if env_flag_enabled("CHEAT_MODE"):
            assert_cheat_mode_language_model_hit(metadata)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"Wrote {hits_path}")
    print(f"Wrote {discards_path}")
    print(f"Wrote {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
