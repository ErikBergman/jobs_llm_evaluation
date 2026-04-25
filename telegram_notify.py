#!/usr/bin/env python3
"""Send Telegram summaries for job-search workflow runs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


TELEGRAM_MAX_MESSAGE_LENGTH = 4096


def load_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as input_file:
        return json.load(input_file)


def parse_utc_timestamp(value: str) -> datetime | None:
    try:
        if value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def metadata_files(results_root: Path) -> list[Path]:
    if not results_root.exists():
        return []
    return sorted(results_root.rglob("*_match_metadata.json"))


def load_metadata_files(results_root: Path) -> list[dict[str, Any]]:
    metadata: list[dict[str, Any]] = []
    for path in metadata_files(results_root):
        try:
            payload = load_json(path)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            payload["_metadata_path"] = str(path)
            metadata.append(payload)
    return metadata


def considered_since(metadata: list[dict[str, Any]], now: datetime, hours: int = 12) -> int:
    cutoff = now.astimezone(timezone.utc) - timedelta(hours=hours)
    total = 0
    for item in metadata:
        matched_at = item.get("matched_at")
        if not isinstance(matched_at, str):
            continue
        parsed = parse_utc_timestamp(matched_at)
        if parsed is None or parsed < cutoff:
            continue
        job_count = item.get("job_count", 0)
        if isinstance(job_count, int):
            total += job_count
    return total


def collect_job_ids(payload: Any) -> set[str]:
    job_ids: set[str] = set()
    if isinstance(payload, dict):
        job_id = payload.get("job_id")
        if job_id not in (None, ""):
            job_ids.add(str(job_id))
        for value in payload.values():
            job_ids.update(collect_job_ids(value))
    elif isinstance(payload, list):
        for item in payload:
            job_ids.update(collect_job_ids(item))
    return job_ids


def accumulated_job_count(results_root: Path) -> int:
    job_ids: set[str] = set()
    if not results_root.exists():
        return 0
    for path in results_root.rglob("*.json"):
        try:
            job_ids.update(collect_job_ids(load_json(path)))
        except (OSError, json.JSONDecodeError):
            continue
    return len(job_ids)


def current_metadata(metadata: list[dict[str, Any]], current_input: str | None = None) -> dict[str, Any] | None:
    if current_input:
        current_input_path = str(Path(current_input))
        for item in metadata:
            if item.get("input_path") == current_input_path:
                return item
    if not metadata:
        return None
    return max(metadata, key=lambda item: str(item.get("matched_at", "")))


def jobs_from_input(metadata: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not metadata:
        return {}
    input_path = metadata.get("input_path")
    if not isinstance(input_path, str):
        return {}
    path = Path(input_path)
    try:
        jobs = load_json(path)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(jobs, list):
        return {}
    output: dict[str, dict[str, Any]] = {}
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_id = job.get("job_id")
        if job_id not in (None, ""):
            output[str(job_id)] = job
    return output


def current_hits(metadata: dict[str, Any] | None) -> list[dict[str, str]]:
    if not metadata:
        return []
    jobs = jobs_from_input(metadata)
    decisions = metadata.get("decisions")
    if not isinstance(decisions, list):
        return []

    hits: list[dict[str, str]] = []
    for decision in decisions:
        if not isinstance(decision, dict) or decision.get("hit") is not True:
            continue
        job_id = decision.get("job_id")
        job = jobs.get(str(job_id), {}) if job_id not in (None, "") else {}
        title = decision.get("title") or job.get("title") or "Untitled job"
        reason = decision.get("reason") or ""
        url = job.get("url") or ""
        hits.append(
            {
                "job_id": str(job_id),
                "title": str(title),
                "url": str(url),
                "reason": str(reason),
            }
        )
    return hits


def truncate_message(message: str, limit: int = TELEGRAM_MAX_MESSAGE_LENGTH) -> str:
    if len(message) <= limit:
        return message
    suffix = "\n\n[truncated]"
    return f"{message[: limit - len(suffix)]}{suffix}"


def build_message(
    considered_last_hours: int,
    accumulated_jobs: int,
    hits: list[dict[str, str]],
    hours: int = 12,
) -> str:
    lines = [
        "Job search summary",
        f"Considered in last {hours} hours: {considered_last_hours}",
        f"Ads in memory: {accumulated_jobs}",
        f"New matches in this run: {len(hits)}",
    ]
    for index, hit in enumerate(hits, start=1):
        lines.extend(
            [
                "",
                f"{index}. {hit['title']}",
                hit["url"] or "No URL in scraped ad.",
                f"Reason: {hit['reason']}",
            ]
        )
    return truncate_message("\n".join(lines))


def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN is required")
    if not chat_id:
        raise ValueError("TELEGRAM_CHAT_ID is required")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text, "disable_web_page_preview": True}).encode("utf-8")
    request = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=30) as response:
            response.read()
    except HTTPError as error:
        detail = error.read().decode("utf-8", "replace")
        raise ValueError(f"Telegram API request failed with HTTP {error.code}: {detail}") from error
    except URLError as error:
        raise ValueError(f"Telegram API request failed: {error}") from error


def summary_message(results_root: Path, current_input: str | None, hours: int, now: datetime) -> str:
    metadata = load_metadata_files(results_root)
    latest_metadata = current_metadata(metadata, current_input=current_input) if current_input else None
    return build_message(
        considered_last_hours=considered_since(metadata, now=now, hours=hours),
        accumulated_jobs=accumulated_job_count(results_root),
        hits=current_hits(latest_metadata),
        hours=hours,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", type=Path, default=Path("results"))
    parser.add_argument("--current-input", default=None)
    parser.add_argument("--hours", type=int, default=12)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        message = summary_message(
            args.results_root,
            current_input=args.current_input,
            hours=args.hours,
            now=datetime.now(timezone.utc),
        )
        print(message)
        if not args.dry_run:
            send_telegram_message(
                os.environ.get("TELEGRAM_BOT_TOKEN", ""),
                os.environ.get("TELEGRAM_CHAT_ID", ""),
                message,
            )
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
