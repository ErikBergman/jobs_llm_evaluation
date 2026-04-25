#!/usr/bin/env python3
"""Classify scraped job ads with a mock or OpenAI-backed matcher."""

from __future__ import annotations

import argparse
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


def job_ad_text(job: dict[str, Any]) -> str:
    fields = [
        ("Title", job.get("title", "")),
        ("Company", job.get("company", "")),
        ("Location", job.get("location", "")),
        ("Description", job.get("description", "")),
    ]
    return "\n".join(f"{label}: {value}" for label, value in fields if isinstance(value, str) and value.strip())


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


def call_openai_match_decision(
    job: dict[str, Any],
    api_key: str,
    prompt: str,
    schema_name: str,
    model: str = DEFAULT_OPENAI_MODEL,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not api_key:
        raise ValueError("OPENAI_API_KEY is required when --matcher openai is used")

    payload = {
        "model": model,
        "input": prompt,
        "reasoning": {"effort": "minimal"},
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "hit": {"type": "boolean"},
                        "reason": {"type": "string"},
                    },
                    "required": ["hit", "reason"],
                },
            }
        },
        "max_output_tokens": DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    response_payload = (post_json or post_json_to_api)(OPENAI_RESPONSES_URL, payload, headers)
    response_text = extract_response_text(response_payload)
    if not response_text:
        raise ValueError(f"OpenAI response did not include output text: {summarize_response_shape(response_payload)}")
    hit, reason = parse_match_response(response_text)
    return {
        "job_id": job.get("job_id"),
        "title": job.get("title"),
        "hit": hit,
        "reason": reason,
        "matcher": "openai",
        "model": response_payload.get("model", model),
        "raw_response": response_text,
    }


def call_openai_unicorn_decision(
    job: dict[str, Any],
    api_key: str,
    profile: str,
    model: str = DEFAULT_OPENAI_MODEL,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return call_openai_match_decision(
        job,
        api_key,
        openai_unicorn_prompt(job, profile),
        "job_unicorn_match",
        model=model,
        post_json=post_json,
    )


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
    matcher_name: str = DEFAULT_MATCHER,
    model: str | None = None,
) -> tuple[Path, Path, Path, dict[str, Any]]:
    resolved_timestamp = timestamp or timestamp_from_input(input_path)
    jobs = load_jobs(input_path)
    hits, discards, decisions = split_jobs_with_decisions(jobs, decisioner=decisioner)
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
        decisioner = decisioner_from_args(
            args.matcher,
            os.environ.get("OPENAI_API_KEY"),
            args.openai_model,
            profile_path=args.job_profile,
        )
        hits_path, discards_path, metadata_path, metadata = classify_file(
            input_path,
            args.output_root,
            args.timestamp,
            decisioner=decisioner,
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
    print(f"Wrote {hits_path}")
    print(f"Wrote {discards_path}")
    print(f"Wrote {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
