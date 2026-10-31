#!/usr/bin/env python3
"""Send Telegram summaries for job-search workflow runs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from job_source_audit import job_source_label
from match_jobs import load_waiting_room_jobs
from job_deduplication import atomic_json, collection_time


TELEGRAM_MAX_MESSAGE_LENGTH = 4096
REPORT_STATE_PATH = Path('notifications/telegram_report_state.json')


@dataclass(frozen=True)
class DiscoverySummary:
    known_job_ids: frozenset[str]
    memory_count: int
    new_ads: int | None
    since: datetime
    first_report: bool = False


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
    now = now.astimezone(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    evaluated_ids: set[str] = set()
    legacy_batches: dict[tuple[str, str], int] = {}
    for item in metadata:
        matched_at = item.get("matched_at")
        if not isinstance(matched_at, str):
            continue
        parsed = parse_utc_timestamp(matched_at)
        if parsed is None or not cutoff <= parsed <= now:
            continue
        decisions = item.get('decisions')
        if isinstance(decisions, list):
            completed = [d for d in decisions if isinstance(d, dict) and isinstance(d.get('hit'), bool)]
            evaluated_ids.update(str(d['job_id']) for d in completed if d.get('job_id') not in (None, ''))
            anonymous = sum(d.get('job_id') in (None, '') for d in completed)
            if anonymous:
                key = (str(item.get('input_path') or item.get('_metadata_path') or ''), matched_at)
                legacy_batches[key] = max(legacy_batches.get(key, 0), anonymous)
            continue
        job_count = item.get("job_count", 0)
        if isinstance(job_count, int):
            key = (str(item.get('input_path') or item.get('_metadata_path') or ''), matched_at)
            legacy_batches[key] = max(legacy_batches.get(key, 0), job_count)
    return len(evaluated_ids) + sum(legacy_batches.values())


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


def memory_job_ids(results_root: Path) -> set[str]:
    job_ids: set[str] = set()
    if not results_root.exists():
        return job_ids
    for path in results_root.rglob("*.json"):
        try:
            job_ids.update(collect_job_ids(load_json(path)))
        except (OSError, json.JSONDecodeError):
            continue
    return job_ids


def accumulated_job_count(results_root: Path) -> int:
    return len(memory_job_ids(results_root))


def discovery_since_previous_report(
    results_root: Path, metadata: list[dict[str, Any]], current_input: str | None,
    now: datetime, hours: int = 12,
) -> DiscoverySummary:
    now = now.astimezone(timezone.utc)
    current_ids = memory_job_ids(results_root)
    state_path = results_root / REPORT_STATE_PATH
    if state_path.exists():
        try:
            state = load_json(state_path)
            sent_at = parse_utc_timestamp(state['sent_at']) if isinstance(state['sent_at'], str) else None
            known = state['known_job_ids']
            if (state.get('version') != 1 or sent_at is None or sent_at > now
                    or not isinstance(known, list) or any(not isinstance(x, str) or not x for x in known)):
                raise ValueError('Invalid report checkpoint')
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError('Invalid Telegram report checkpoint; existing state left untouched.') from error
        previous_ids = set(known)
        return DiscoverySummary(frozenset(previous_ids | current_ids), len(current_ids),
                                len(current_ids - previous_ids), sent_at)

    # First deployment: previous matching reports and saved collection times
    # provide a baseline. Never label all historical/backlogged ads as new.
    prior_reports = []
    for item in metadata:
        if current_input and str(Path(str(item.get('input_path') or ''))) == str(Path(current_input)):
            continue
        value = item.get('matched_at')
        stamp = parse_utc_timestamp(value) if isinstance(value, str) else None
        if stamp is not None and stamp <= now:
            prior_reports.append(stamp)
    since = max(prior_reports) if prior_reports else now - timedelta(hours=hours)
    first_seen: dict[str, datetime] = {}
    for bucket in ('waiting_room', 'discard', 'hits', 'duplicates'):
        for path in (results_root / bucket).rglob('*.json'):
            try:
                records = load_json(path)
            except (OSError, ValueError):
                continue
            if not isinstance(records, list):
                continue
            for job in records:
                if not isinstance(job, dict) or job.get('job_id') in (None, ''):
                    continue
                job_id = str(job['job_id'])
                candidates = [collection_time({key: job.get(key)}) for key in
                              ('first_seen_at', 'indexed_at', 'collected_at', 'scraped_at', 'fetched_at')]
                candidates.append(collection_time({}, path))
                valid = [stamp for stamp in candidates if stamp is not None and stamp <= now]
                if valid:
                    stamp = min(valid)
                    first_seen[job_id] = min(first_seen.get(job_id, stamp), stamp)
    new_count = sum(since < stamp <= now for stamp in first_seen.values()) if current_ids <= first_seen.keys() else None
    return DiscoverySummary(frozenset(current_ids), len(current_ids), new_count, since, not prior_reports)


def save_report_checkpoint(results_root: Path, discovery: DiscoverySummary, sent_at: datetime) -> None:
    atomic_json(results_root / REPORT_STATE_PATH, {
        'version': 1,
        'sent_at': sent_at.astimezone(timezone.utc).isoformat(),
        'known_job_ids': sorted(discovery.known_job_ids),
        'new_ads': discovery.new_ads,
        'previous_report_at': discovery.since.isoformat(),
    })


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
                "source_name": job_source_label(job or decision),
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
    evaluated_this_run: int | None = None,
    llm_reviewed_this_run: int | None = None,
    pending_evaluation: int | None = None,
    new_ads_since_previous_report: int | None = None,
    first_report: bool = False,
) -> str:
    lines = ["✅ New match!", ""] if hits else []
    lines.extend([
        "Job search summary",
        (f"New ads in last {hours} hours (first report): " if first_report else "New ads since previous report: ")
        + (str(new_ads_since_previous_report) if new_ads_since_previous_report is not None else 'unavailable (establishing baseline)'),
        f"Evaluated in last {hours} hours: {considered_last_hours}",
        f"Ads in memory: {accumulated_jobs}",
    ])
    if evaluated_this_run is not None:
        detail = f" ({llm_reviewed_this_run} sent to AI)" if llm_reviewed_this_run is not None else ''
        lines.append(f"Evaluated this run: {evaluated_this_run}{detail}")
    if pending_evaluation is not None:
        lines.append(f"Waiting for evaluation: {pending_evaluation}")
    if hits:
        lines.extend([f"New matches in this run: {len(hits)}", ""])
        counts = Counter(hit.get("source_name") or job_source_label(hit) for hit in hits)
        for source in sorted(counts, key=lambda name: (name != "LinkedIn", name.casefold())):
            lines.append(f"- From {source}: {counts[source]}")
    else:
        lines.append("0 new matches found this run")
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


def summary_message(results_root: Path, current_input: str | None, hours: int, now: datetime,
                    discovery: DiscoverySummary | None = None) -> str:
    metadata = load_metadata_files(results_root)
    discovery = discovery or discovery_since_previous_report(results_root, metadata, current_input, now, hours)
    latest_metadata = current_metadata(metadata, current_input=current_input) if current_input else None
    current_decisions = latest_metadata.get('decisions', []) if latest_metadata else []
    llm_count = sum(d.get('stage') != 'prefilter' and d.get('matcher', latest_metadata.get('matcher')) == 'openai'
                    for d in current_decisions if isinstance(d, dict)) if latest_metadata else 0
    return build_message(
        considered_last_hours=considered_since(metadata, now=now, hours=hours),
        accumulated_jobs=discovery.memory_count,
        hits=current_hits(latest_metadata),
        hours=hours,
        evaluated_this_run=latest_metadata.get('job_count', 0) if latest_metadata else 0,
        llm_reviewed_this_run=llm_count,
        pending_evaluation=len(load_waiting_room_jobs(results_root, exclude_evaluated=True)),
        new_ads_since_previous_report=discovery.new_ads,
        first_report=discovery.first_report,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", type=Path, default=Path("results"))
    parser.add_argument("--current-input", default=None)
    parser.add_argument("--hours", type=int, default=12)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        now = datetime.now(timezone.utc)
        discovery = discovery_since_previous_report(
            args.results_root, load_metadata_files(args.results_root), args.current_input, now, args.hours)
        message = summary_message(
            args.results_root,
            current_input=args.current_input,
            hours=args.hours,
            now=now,
            discovery=discovery,
        )
        print(message)
        if not args.dry_run:
            send_telegram_message(
                os.environ.get("TELEGRAM_BOT_TOKEN", ""),
                os.environ.get("TELEGRAM_CHAT_ID", ""),
                message,
            )
            save_report_checkpoint(args.results_root, discovery, datetime.now(timezone.utc))
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
