"""Source-aware job logs and a GitHub Actions run summary; no network calls."""
from __future__ import annotations

import argparse
import os
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


def job_site(job: dict[str, Any]) -> str:
    try:
        host = (urlsplit(str(job.get("url") or "")).hostname or "").lower()
    except ValueError:
        host = ""
    if host == "linkedin.com" or host.endswith(".linkedin.com"):
        return "linkedin.com"
    return host or "unknown"


def job_source_label(job: dict[str, Any]) -> str:
    job_id = str(job.get("job_id") or "")
    if job.get("source") == "employer" or job_id.startswith("employer:"):
        company = job.get("company")
        if isinstance(company, str) and company.strip():
            return company.strip()
        source_id = job.get("source_id") or (job_id.split(":")[1] if job_id.startswith("employer:") else "")
        return str(source_id).replace("-", " ").title() if source_id else "Company career pages"
    if job.get("source") == "linkedin" or job_site(job) == "linkedin.com":
        return "LinkedIn"
    return "Unknown source"


def job_log_context(job: dict[str, Any]) -> str:
    return (f"source={job_source_label(job)!r} site={job_site(job)!r} "
            f"job_id={job.get('job_id')!r} url={job.get('url') or ''!r}")


def log_found_jobs(jobs: list[dict[str, Any]], include_summary: bool = True) -> None:
    counts: dict[tuple[str, str], Counter] = {}
    for job in jobs:
        row = counts.setdefault((job_source_label(job), job_site(job)), Counter())
        row['new_ads'] += 1
        passed = job.get('prefilter_pass') is not False
        row['prefilter_passed' if passed else 'prefilter_rejected'] += 1
        print(f"[ad-found] {job_log_context(job)} title={job.get('title')!r} "
              f"prefilter={'passed' if passed else 'rejected'} reason={job.get('prefilter_reason') or ''!r}")
    if include_summary:
        for (source, site), row in sorted(counts.items()):
            print(f"[source-scrape] source={source!r} site={site!r} new_ads={row['new_ads']} "
                  f"prefilter_passed={row['prefilter_passed']} prefilter_rejected={row['prefilter_rejected']}")


def format_source_evaluation_summary(jobs: list[dict[str, Any]], decisions: list[dict[str, Any]]) -> str:
    if len(jobs) != len(decisions):
        raise ValueError("Decision count must match job count")
    counts: dict[tuple[str, str], Counter] = {}
    for job, decision in zip(jobs, decisions):
        row = counts.setdefault((job_source_label(job), job_site(job)), Counter())
        row['evaluated'] += 1
        if decision.get('hit') is True:
            row['matched'] += 1
        else:
            row['discarded'] += 1
            stage = decision.get('stage')
            row[f"rejected_{stage}" if stage in {'prefilter', 'triage', 'confirmation'} else 'rejected_other'] += 1
    lines = []
    for (source, site), row in sorted(counts.items()):
        values = ' '.join(f"{key}={row[key]}" for key in (
            'evaluated', 'matched', 'discarded', 'rejected_prefilter',
            'rejected_triage', 'rejected_confirmation', 'rejected_other'))
        lines.append(f"[source-evaluation] source={source!r} site={site!r} {values}")
    return '\n'.join(lines)


def log_duplicate_jobs(jobs: list[dict[str, Any]]) -> None:
    counts = Counter((job_source_label(job), job_site(job)) for job in jobs)
    for job in jobs:
        decision = job.get('deduplication') or {}
        print(f"[duplicate-discard] {job_log_context(job)} "
              f"duplicate_of={decision.get('duplicate_of')!r} reason={decision.get('reason') or ''!r}")
    for (source, site), count in sorted(counts.items()):
        print(f"[source-duplicates] source={source!r} site={site!r} discarded_duplicates={count}")


def write_actions_summary(log_paths: list[Path], summary_path: Path) -> None:
    summary_rows, decisions, missing_logs = [], [], []
    for path in log_paths:
        if not path.is_file():
            missing_logs.append(path.name)
            continue
        for line in path.read_text(encoding='utf-8', errors='replace').splitlines():
            if line.startswith(('[source-scrape]', '[source-evaluation]', '[source-duplicates]', '[evaluation-memory]', '[evaluation-queue]')):
                summary_rows.append(line)
            elif line.startswith(('[decision]', '[duplicate-discard]', '[source-error]')):
                decisions.append(line)
    lines = ['## Job source audit', '',
             'Scrape counts describe newly collected ads; evaluation counts describe the current classification batch. '
             'Duplicates, location/expiry skips, and model rejections are reported separately.', '']
    if missing_logs:
        lines.extend(['Logs not produced (step skipped, not configured, or failed before logging): '
                      + ', '.join(f'`{name}`' for name in missing_logs) + '.', ''])
    if summary_rows:
        lines.extend(['```text', *summary_rows, '```', ''])
    else:
        lines.extend(['No source audit was produced. Check the scrape and classification steps for skipped work or errors.', ''])
    if decisions:
        lines.extend(['<details><summary>Job URLs and discard reasons</summary>', '', '```text',
                      *decisions, '```', '', '</details>', ''])
    with summary_path.open('a', encoding='utf-8') as summary:
        summary.write('\n'.join(lines) + '\n')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--logs', nargs='+', type=Path, required=True)
    args = parser.parse_args()
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        write_actions_summary(args.logs, Path(summary))


if __name__ == '__main__':
    main()
