#!/usr/bin/env python3
"""Classify scraped job ads with a mock CV matcher."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ENGINEER_WORD = re.compile(r"\bengineer\b", re.IGNORECASE)
DEFAULT_OUTPUT_ROOT = Path("results")
DEFAULT_MATCHER = "mock"
DEFAULT_OPENAI_MODEL = "gpt-5-nano"
DEFAULT_OPENAI_MAX_OUTPUT_TOKENS = 80
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"


def is_mock_hit(job: dict[str, Any]) -> bool:
    description = job.get("description", "")
    if not isinstance(description, str):
        return False
    return bool(ENGINEER_WORD.search(description))


def openai_title_vowel_prompt(job: dict[str, Any]) -> str:
    title = job.get("title", "")
    if not isinstance(title, str):
        title = ""
    return (
        "Determine whether this job title starts with a vowel. "
        "If the first letter of the title is A, E, I, O, or U, return hit=true. "
        "Otherwise return hit=false.\n\n"
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


def parse_hit_response(response_text: str) -> bool:
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError as error:
        raise ValueError(f"OpenAI response was not valid JSON: {response_text!r}") from error

    hit = payload.get("hit")
    if not isinstance(hit, bool):
        raise ValueError(f"OpenAI response JSON must contain boolean 'hit': {response_text!r}")
    return hit


def call_openai_title_vowel_matcher(
    job: dict[str, Any],
    api_key: str,
    model: str = DEFAULT_OPENAI_MODEL,
    post_json: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
) -> bool:
    if not api_key:
        raise ValueError("OPENAI_API_KEY is required when --matcher openai is used")

    payload = {
        "model": model,
        "input": openai_title_vowel_prompt(job),
        "text": {
            "format": {
                "type": "json_schema",
                "name": "job_title_vowel_match",
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "hit": {"type": "boolean"},
                    },
                    "required": ["hit"],
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
    return parse_hit_response(response_text)


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


def timestamp_from_input(input_path: Path) -> str:
    parts = input_path.parts
    for index in range(len(parts) - 1):
        if parts[index] == "discard":
            return parts[index + 1]
    raise ValueError("--timestamp is required unless --input is under <output-root>/discard/<timestamp>/")


def output_paths(input_path: Path, output_root: Path, timestamp: str) -> tuple[Path, Path]:
    stem = input_path.stem
    return (
        output_root / "hits" / timestamp / f"{stem}_hits.json",
        output_root / "discard" / timestamp / f"{stem}_discard.json",
    )


def write_json(path: Path, payload: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)


def matcher_from_args(matcher_name: str, api_key: str | None, model: str) -> Callable[[dict[str, Any]], bool]:
    if matcher_name == "mock":
        return is_mock_hit
    if matcher_name == "openai":
        resolved_api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        return lambda job: call_openai_title_vowel_matcher(job, resolved_api_key, model=model)
    raise ValueError(f"Unsupported matcher: {matcher_name}")


def classify_file(
    input_path: Path,
    output_root: Path,
    timestamp: str | None = None,
    matcher: Callable[[dict[str, Any]], bool] = is_mock_hit,
) -> tuple[Path, Path]:
    resolved_timestamp = timestamp or timestamp_from_input(input_path)
    hits, discards = split_jobs(load_jobs(input_path), matcher=matcher)
    hits_path, discards_path = output_paths(input_path, output_root, resolved_timestamp)
    write_json(hits_path, hits)
    write_json(discards_path, discards)
    return hits_path, discards_path


def is_unclassified_jobs_json(path: Path) -> bool:
    return path.suffix == ".json" and not path.name.endswith(("_hits.json", "_discard.json"))


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
    args = parser.parse_args()

    try:
        input_path = input_from_latest(args.output_root) if args.latest else args.input
        matcher = matcher_from_args(args.matcher, os.environ.get("OPENAI_API_KEY"), args.openai_model)
        hits_path, discards_path = classify_file(input_path, args.output_root, args.timestamp, matcher=matcher)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"Wrote {hits_path}")
    print(f"Wrote {discards_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
