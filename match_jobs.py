#!/usr/bin/env python3
"""Classify scraped job ads with a mock CV matcher."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


ENGINEER_WORD = re.compile(r"\bengineer\b", re.IGNORECASE)
DEFAULT_OUTPUT_ROOT = Path("results")


def is_mock_hit(job: dict[str, Any]) -> bool:
    description = job.get("description", "")
    if not isinstance(description, str):
        return False
    return bool(ENGINEER_WORD.search(description))


def load_jobs(input_path: Path) -> list[dict[str, Any]]:
    with open(input_path, encoding="utf-8") as input_file:
        jobs = json.load(input_file)
    if not isinstance(jobs, list) or not all(isinstance(job, dict) for job in jobs):
        raise ValueError(f"{input_path} must contain a JSON array of job objects")
    return jobs


def split_jobs(jobs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    hits: list[dict[str, Any]] = []
    discards: list[dict[str, Any]] = []
    for job in jobs:
        if is_mock_hit(job):
            hits.append(job)
        else:
            discards.append(job)
    return hits, discards


def timestamp_from_input(input_path: Path) -> str:
    parts = input_path.parts
    for index in range(len(parts) - 2):
        if parts[index] == "results" and parts[index + 1] == "discard":
            return parts[index + 2]
    raise ValueError("--timestamp is required unless --input is under results/discard/<timestamp>/")


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


def classify_file(input_path: Path, output_root: Path, timestamp: str | None = None) -> tuple[Path, Path]:
    resolved_timestamp = timestamp or timestamp_from_input(input_path)
    hits, discards = split_jobs(load_jobs(input_path))
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
    args = parser.parse_args()

    try:
        input_path = input_from_latest(args.output_root) if args.latest else args.input
        hits_path, discards_path = classify_file(input_path, args.output_root, args.timestamp)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"Wrote {hits_path}")
    print(f"Wrote {discards_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
