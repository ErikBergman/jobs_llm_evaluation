#!/usr/bin/env python3
"""Remove confirmed newer cross-source copies from the shared waiting room."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import job_deduplication as dedup
from match_jobs import is_unclassified_jobs_json, load_waiting_room_jobs


def process_waiting_room(results_root, config, decider=None, now=None, dry_run=False):
    now = now or datetime.now(timezone.utc)
    jobs = load_waiting_room_jobs(results_root)
    if dry_run:
        history, issues = dedup.load_history(results_root, now)
        ids = {str(j['job_id']) for j in jobs if j.get('job_id')}
        known = {r.record_id: r for r in history}
        index = dedup.RecentJobs([r for r in history if r.record_id not in ids], config)
        candidates = 0
        ordered = sorted(jobs, key=lambda j: known[str(j['job_id'])].first_seen
                         if str(j.get('job_id')) in known else dedup.collection_time(j) or now)
        for job in ordered:
            stamp = known[str(job['job_id'])].first_seen if str(job.get('job_id')) in known else dedup.collection_time(job) or now
            if index.candidates(job, stamp):
                candidates += 1
            if job.get('job_id'):
                index.add(dedup.SavedJob(job, stamp))
        return {'mode': 'dry-run', 'new_ads': len(jobs), 'ads_with_candidates': candidates,
                'maximum_calls': min(candidates, config.max_calls), 'llm_calls': 0,
                'duplicates': 0, 'history_issues': issues}
    accepted, duplicates, audit = dedup.filter_new_jobs(jobs, results_root, config, decider, now)
    stamp = now.strftime('%Y%m%d_%H%M%S_%f')
    # Archive full discarded records before removing any waiting-room copy. Their IDs stay in seen memory.
    archive = dedup.write_duplicate_archive(duplicates, results_root, stamp)
    if archive:
        audit['duplicate_archive'] = str(archive)
    accepted_by_id = {str(j['job_id']): j for j in accepted if j.get('job_id')}
    retired = {str(j['job_id']) for j in duplicates}
    rewrites = []
    for path in sorted((Path(results_root)/'waiting_room').rglob('*.json')):
        if not is_unclassified_jobs_json(path):
            continue
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            continue  # Existing loader also reports and preserves unreadable inputs.
        if not isinstance(payload, list) or not all(isinstance(j, dict) for j in payload):
            continue
        revised = [accepted_by_id.get(str(j.get('job_id')), j) for j in payload
                   if str(j.get('job_id')) not in retired]
        if revised != payload:
            rewrites.append((path, revised))
    for path, revised in rewrites:
        dedup.atomic_json(path, revised)
    path = Path(results_root)/'deduplication'/stamp/'deduplication_metadata.json'
    dedup.atomic_json(path, audit)
    return audit


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results-root', type=Path, default=Path('results'))
    parser.add_argument('--config', type=Path, default=dedup.CONFIG_PATH)
    parser.add_argument('--model', help='Override the configured small model')
    parser.add_argument('--mode', choices=('openai', 'off'), default='openai')
    parser.add_argument('--dry-run', action='store_true', help='Show candidate counts without LLM calls or file changes')
    args = parser.parse_args(argv)
    if args.mode == 'off':
        print('Cross-source deduplication disabled.')
        return 0
    try:
        config = dedup.load_config(args.config)
        if args.model:
            config = replace(config, model=args.model)
        audit = process_waiting_room(args.results_root, config, dry_run=args.dry_run)
    except (ValueError, OSError) as exc:
        print(f'Deduplication failed: {exc}', file=sys.stderr)
        return 1
    print(json.dumps({k: v for k, v in audit.items() if k not in {'decisions', 'history_issues'}}, ensure_ascii=False))
    if audit.get('error_kept'):
        print('Some duplicate comparisons were unavailable; affected ads were retained.', file=sys.stderr)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
