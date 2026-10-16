"""Find recent cross-source job copies in the application's saved result memory."""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlsplit

CONFIG_PATH = Path(__file__).with_name('job_deduplication.json')
HISTORY_BUCKETS = ('waiting_room', 'hits', 'discard')
LEGAL_SUFFIXES = {'ab', 'publ', 'inc', 'incorporated', 'ltd', 'limited', 'plc', 'llc', 'corp', 'corporation'}


def normalized_words(value):
    text = unicodedata.normalize('NFKD', str(value or '').casefold())
    text = ''.join(c for c in text if not unicodedata.combining(c))
    return re.findall(r'[a-z0-9]+', text)


def employer_key(name):
    words = normalized_words(name)
    while words and words[-1] in LEGAL_SUFFIXES:
        words.pop()
    return ' '.join(words)


@dataclass(frozen=True)
class DeduplicationConfig:
    lookback_days: int = 7
    max_candidates: int = 3
    max_calls: int = 20
    excerpt_chars: int = 1600
    max_output_tokens: int = 512
    model: str = 'gpt-5.4-mini'
    employer_aliases: tuple = ()

    def company_key(self, name):
        key = employer_key(name)
        for canonical, aliases in self.employer_aliases:
            if key in (employer_key(canonical), *(employer_key(a) for a in aliases)):
                return employer_key(canonical)
        return key


def load_config(path=CONFIG_PATH):
    data = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(data, dict) or data.get('version') != 1:
        raise ValueError('Expected version 1 deduplication configuration')
    allowed = set(DeduplicationConfig.__dataclass_fields__)
    if set(data) - allowed - {'version'}:
        raise ValueError('Unknown deduplication configuration field')
    values = {k: v for k, v in data.items() if k != 'version'}
    aliases = values.pop('employer_aliases', {})
    if not isinstance(aliases, dict) or any(not isinstance(k, str) or not isinstance(v, list)
                                         or any(not isinstance(a, str) for a in v) for k, v in aliases.items()):
        raise ValueError('Employer aliases must map names to arrays of names')
    config = DeduplicationConfig(**values, employer_aliases=tuple((k, tuple(v)) for k, v in aliases.items()))
    for name, low, high in [('lookback_days', 1, 90), ('max_candidates', 1, 5), ('max_calls', 0, 100),
                            ('excerpt_chars', 200, 2000), ('max_output_tokens', 128, 1024)]:
        value = getattr(config, name)
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f'{name} must be an integer between {low} and {high}')
    if not isinstance(config.model, str) or not config.model.strip():
        raise ValueError('Deduplication model must be a nonempty string')
    owners = {}
    for canonical, variants in config.employer_aliases:
        for name in (canonical, *variants):
            key = employer_key(name)
            if not key or key in owners and owners[key] != employer_key(canonical):
                raise ValueError('Empty or conflicting employer alias')
            owners[key] = employer_key(canonical)
    return config


def utc_time(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)


def collection_time(job, path=None):
    # Posting date is deliberately excluded: recently discovered ads can be older.
    for key in ('first_seen_at', 'indexed_at', 'collected_at', 'scraped_at', 'fetched_at'):
        stamp = utc_time(job.get(key))
        if stamp:
            return stamp
    if path is not None:
        # Legacy GitHub runner timestamps are UTC. Never use mtime (cloud copies reset it).
        for part in reversed(Path(path).parts):
            if re.fullmatch(r'\d{8}_\d{6}(?:_\d{1,6})?', part):
                fmt = '%Y%m%d_%H%M%S_%f' if part.count('_') == 2 else '%Y%m%d_%H%M%S'
                try:
                    return datetime.strptime(part, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
    return None


def source_kind(job):
    if job.get('source') == 'employer' or str(job.get('job_id', '')).startswith('employer:'):
        return 'employer'
    host = (urlsplit(str(job.get('url') or '')).hostname or '').lower()
    if host == 'linkedin.com' or host.endswith('.linkedin.com'):
        return 'linkedin'
    return 'unknown'


@dataclass
class SavedJob:
    job: dict
    first_seen: datetime

    @property
    def record_id(self):
        return str(self.job['job_id'])


class RecentJobs:
    def __init__(self, records, config):
        self.config = config
        self.by_company = {}
        self.by_id = {}
        for record in records:
            self.add(record)

    def add(self, record):
        key = self.config.company_key(record.job.get('company'))
        if not key or source_kind(record.job) == 'unknown':
            return
        old = self.by_id.get(record.record_id)
        if old and old.first_seen <= record.first_seen:
            return
        if old:
            old_key = self.config.company_key(old.job.get('company'))
            self.by_company[old_key].remove(old)
        self.by_id[record.record_id] = record
        self.by_company.setdefault(key, []).append(record)

    def candidates(self, job, first_seen):
        kind = source_kind(job)
        if kind == 'unknown':
            return []
        earliest = first_seen - timedelta(days=self.config.lookback_days)
        possible = []
        for old in self.by_company.get(self.config.company_key(job.get('company')), []):
            if (old.record_id == str(job.get('job_id')) or source_kind(old.job) == kind
                    or not earliest <= old.first_seen <= first_seen):
                continue
            similarity = title_similarity(job.get('title'), old.job.get('title'))
            # Ranking only controls LLM spending; it never declares a duplicate.
            if similarity >= 0.35:
                possible.append((similarity, old))
        possible.sort(key=lambda x: (-x[0], x[1].first_seen, x[1].record_id))
        return [r for _, r in possible[:self.config.max_candidates]]


def title_similarity(left, right):
    a, b = normalized_words(left), normalized_words(right)
    if not a or not b:
        return 0.0
    overlap = len(set(a) & set(b)) / len(set(a) | set(b))
    sequence = SequenceMatcher(None, ' '.join(a), ' '.join(b)).ratio()
    return max(overlap, sequence)


def load_history(results_root, now=None):
    now = now or datetime.now(timezone.utc)
    records, issues = {}, []
    for bucket in HISTORY_BUCKETS:
        for path in sorted((Path(results_root) / bucket).rglob('*.json')):
            if path.name.endswith(('_metadata.json', '_search_audit.json')) or path.name == 'metadata.json':
                continue
            try:
                payload = json.loads(path.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                issues.append({'path': str(path), 'issue': 'unreadable_history'})
                continue
            if not isinstance(payload, list):
                continue
            for job in payload:
                if (not isinstance(job, dict) or not job.get('job_id') or not job.get('company')
                        or not job.get('title') or not job.get('description')):
                    continue
                stamp = collection_time(job, path)
                if stamp is None or stamp > now:
                    issues.append({'record_id': str(job['job_id']), 'issue': 'unknown_or_future_collection_time'})
                    continue
                key = str(job['job_id'])
                old = records.get(key)
                if old is None or stamp < old.first_seen:
                    records[key] = SavedJob(dict(job), stamp)
    return list(records.values()), issues
