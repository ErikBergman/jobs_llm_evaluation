import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import job_deduplication as d

NOW = datetime(2026, 10, 16, 12, tzinfo=timezone.utc)
TEXT = 'Build Python data integrations for clinical laboratory systems. Work with REST APIs and automated testing.'


def job(key, source='linkedin', company='Axis Communications AB', title='Backend Software Engineer', **extra):
    result = {'job_id': key, 'company': company, 'title': title, 'description': TEXT,
              'location': 'Lund, Sweden', 'url': 'https://www.linkedin.com/jobs/view/' + key}
    if source == 'employer':
        result.update(source='employer', url='https://careers.example.org/jobs/' + key)
    result.update(extra)
    return result


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = d.load_config()

    def save(self, relative, payload):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding='utf-8')
        return path

    def test_employer_case_accents_legal_suffixes_and_explicit_aliases(self):
        self.assertEqual(d.employer_key('  ALFA-LAVAL AB (publ) '), d.employer_key('Alfa Laval'))
        self.assertEqual(d.employer_key('Region Skåne'), d.employer_key('region skane'))
        self.assertEqual(self.config.company_key('Axis Communications AB'), self.config.company_key('Axis'))
        self.assertNotEqual(self.config.company_key('Axis Labs'), self.config.company_key('Axis'))

    def test_only_opposite_sources_same_employer_and_recent_first_collection(self):
        records = [d.SavedJob(job('recent'), NOW - timedelta(days=6)),
                   d.SavedJob(job('old'), NOW - timedelta(days=8)),
                   d.SavedJob(job('future'), NOW + timedelta(seconds=1)),
                   d.SavedJob(job('other', company='Other'), NOW),
                   d.SavedJob(job('native', source='employer', company='Axis'), NOW)]
        index = d.RecentJobs(records, self.config)
        found = index.candidates(job('new', source='employer', company='Axis'), NOW)
        self.assertEqual([r.record_id for r in found], ['recent'])

    def test_seven_day_boundary_and_reverse_source_order(self):
        old = d.SavedJob(job('older', source='employer', company='Axis'), NOW - timedelta(days=7))
        found = d.RecentJobs([old], self.config).candidates(job('new'), NOW)
        self.assertEqual([r.record_id for r in found], ['older'])
        self.assertFalse(d.RecentJobs([d.SavedJob(old.job, old.first_seen - timedelta(microseconds=1))], self.config).candidates(job('new'), NOW))

    def test_rank_and_cap_candidates_not_all_jobs_at_a_large_employer(self):
        records = [d.SavedJob(job(str(i), title=t), NOW - timedelta(days=1)) for i, t in enumerate([
            'Backend Software Engineer', 'Backend Engineer', 'Senior Backend Software Engineer',
            'Software Engineer', 'Medical Office Receptionist'])]
        found = d.RecentJobs(records, self.config).candidates(job('new', source='employer', company='Axis'), NOW)
        self.assertEqual(len(found), 3)
        self.assertEqual(found[0].job['title'], 'Backend Software Engineer')
        self.assertNotIn('Medical Office Receptionist', [r.job['title'] for r in found])

    def test_unrelated_titles_and_unknown_sources_do_not_trigger_a_comparison(self):
        index = d.RecentJobs([d.SavedJob(job('old', title='Medical Receptionist'), NOW)], self.config)
        self.assertFalse(index.candidates(job('new', source='employer', company='Axis'), NOW))
        self.assertEqual(d.source_kind({'url': 'https://linkedin.com.evil.test/jobs'}), 'unknown')
        self.assertEqual(d.source_kind({'job_id': 'employer:axis:123'}), 'employer')

    def test_history_prefers_oldest_copy_and_ignores_reclassification_and_posting_dates(self):
        self.save('discard/20261008_100000/raw.json', [job('1', posted_date='2026-10-16')])
        self.save('hits/20261016_100000/hits.json', [job('1')])
        records, issues = d.load_history(self.root, NOW)
        self.assertFalse(issues)
        self.assertEqual(records[0].first_seen, datetime(2026, 10, 8, 10, tzinfo=timezone.utc))
        self.assertFalse(d.RecentJobs(records, self.config).candidates(job('new', source='employer', company='Axis'), NOW))

    def test_explicit_collection_time_survives_moving_files(self):
        self.save('hits/20261016_100000/hits.json', [job('1', first_seen_at='2026-10-01T10:00:00Z', indexed_at='2026-10-15T10:00:00Z')])
        records, _ = d.load_history(self.root, NOW)
        self.assertEqual(records[0].first_seen, datetime(2026, 10, 1, 10, tzinfo=timezone.utc))

    def test_missing_dates_never_fall_back_to_mtime_or_posted_date(self):
        self.save('waiting_room/imported/jobs.json', [job('1', posted_date='2026-10-16')])
        records, issues = d.load_history(self.root, NOW)
        self.assertFalse(records)
        self.assertEqual(issues[0]['issue'], 'unknown_or_future_collection_time')

    def test_metadata_rejected_duplicates_and_malformed_files_are_not_candidates(self):
        self.save('discard/20261016_100000/run_metadata.json', [job('meta')])
        self.save('duplicates/20261016_100000/duplicate_jobs.json', [job('dropped')])
        self.save('waiting_room/20261016_100000/jobs.json', [job('real')])
        self.save('hits/20261016_100000/decisions.json', [{'job_id': 'fake', 'title': 'Engineer', 'hit': True}])
        records, _ = d.load_history(self.root, NOW)
        self.assertEqual([r.record_id for r in records], ['real'])

    def test_alias_collisions_and_invalid_bounds_fail_before_network(self):
        for config in [{'version': 1, 'max_candidates': 100}, {'version': 1, 'max_calls': True},
                       {'version': 1, 'employer_aliases': {'Axis': ['Acme'], 'Other': ['Acme']}}]:
            path = self.save('config.json', config)
            with self.assertRaises(ValueError):
                d.load_config(path)


if __name__ == '__main__':
    unittest.main()
