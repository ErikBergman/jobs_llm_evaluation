import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import match_jobs
from job_source_audit import format_source_evaluation_summary, log_found_jobs, write_actions_summary


class SourceAuditTests(unittest.TestCase):
    def test_evaluation_groups_by_origin_site_and_accounts_for_every_discard_stage(self):
        linkedin = {'company': 'Axis', 'url': 'https://se.linkedin.com/jobs/view/1'}
        axis = {'source': 'employer', 'company': 'Axis', 'url': 'https://axis.example.org/job/2'}
        jobs = [linkedin, linkedin, axis, axis, axis]
        decisions = [
            {'hit': True, 'stage': 'confirmation'},
            {'hit': False, 'stage': 'triage'},
            {'hit': False, 'stage': 'prefilter'},
            {'hit': False, 'stage': 'confirmation'},
            {'hit': False, 'matcher': 'mock'},
        ]
        lines = format_source_evaluation_summary(jobs, decisions).splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn("source='LinkedIn' site='linkedin.com' evaluated=2 matched=1 discarded=1", lines[1])
        self.assertIn('rejected_triage=1', lines[1])
        self.assertIn("source='Axis' site='axis.example.org' evaluated=3 matched=0 discarded=3", lines[0])
        self.assertIn('rejected_prefilter=1 rejected_triage=0 rejected_confirmation=1 rejected_other=1', lines[0])

    def test_matching_cli_logs_source_url_reason_and_current_batch_counts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / 'discard' / '20261028_100000' / 'jobs.json'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps([
                {'job_id': '1', 'company': 'Axis', 'title': 'Engineer', 'url': 'https://se.linkedin.com/jobs/view/1', 'description': 'We need an engineer.'},
                {'job_id': 'employer:axis:2', 'source': 'employer', 'company': 'Axis', 'title': 'Other role', 'url': 'https://axis.example.org/job/2', 'description': 'Not a match.'},
            ]))
            output = io.StringIO()
            with patch('sys.argv', ['match_jobs.py', '--input', str(path), '--output-root', str(root)]), \
                 patch.object(match_jobs, 'DEFAULT_MATCHING_CONFIG_PATH', root / 'missing.json'), \
                 patch.dict('os.environ', {'CHEAT_MODE': 'false'}), redirect_stdout(output):
                code = match_jobs.main()
            text = output.getvalue()
            self.assertEqual(code, 0)
            decision_lines = [line for line in text.splitlines() if line.startswith('[decision]')]
            self.assertEqual(len(decision_lines), 2)
            self.assertIn("source='Axis' site='axis.example.org' job_id='employer:axis:2' url='https://axis.example.org/job/2' outcome=discarded", decision_lines[1])
            self.assertIn('Description does not contain', decision_lines[1])
            self.assertIn("source='LinkedIn' site='linkedin.com' evaluated=1 matched=1 discarded=0", text)
            self.assertIn("source='Axis' site='axis.example.org' evaluated=1 matched=0 discarded=1", text)

    def test_found_logs_keep_prefilter_rejections_and_escape_untrusted_newlines(self):
        jobs = [{'job_id': '1', 'url': 'https://linkedin.com/jobs/view/1', 'title': 'Role\n::error::fake',
                 'prefilter_pass': False, 'prefilter_reason': 'Excluded location'}]
        output = io.StringIO()
        with redirect_stdout(output):
            log_found_jobs(jobs)
        text = output.getvalue()
        self.assertEqual(len(text.splitlines()), 2)
        self.assertIn('prefilter=rejected', text)
        self.assertIn("reason='Excluded location'", text)
        self.assertIn('new_ads=1 prefilter_passed=0 prefilter_rejected=1', text)

    def test_actions_summary_only_collects_audits_from_supplied_current_logs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            scrape = root / 'scrape.log'
            scrape.write_text("PRIVATE AD BODY\n[source-scrape] source='LinkedIn' new_ads=2\n")
            dedup = root / 'deduplicate.log'
            dedup.write_text("[source-duplicates] source='Axis' discarded_duplicates=1\n[duplicate-discard] url='https://example.org/2' reason='Same vacancy'\n")
            classify = root / 'classify.log'
            classify.write_text("[source-evaluation] source='LinkedIn' matched=0 discarded=1\n[decision] url='https://linkedin.com/jobs/view/1' outcome=discarded reason='No fit'\n")
            summary = root / 'summary.md'
            summary.write_text('Previous step summary\n')
            write_actions_summary([scrape, dedup, classify, root / 'missing.log'], summary)
            text = summary.read_text()
            self.assertTrue(text.startswith('Previous step summary\n'))
            self.assertIn('## Job source audit', text)
            self.assertIn("discarded_duplicates=1", text)
            self.assertIn("url='https://linkedin.com/jobs/view/1'", text)
            self.assertNotIn('PRIVATE AD BODY', text)
            self.assertIn('`missing.log`', text)

    def test_actions_summary_does_not_treat_missing_logs_as_zero_ads(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            summary = root / 'summary.md'
            write_actions_summary([root / 'missing.log'], summary)
            self.assertIn('No source audit was produced', summary.read_text())
            self.assertNotIn('new_ads=0', summary.read_text())


if __name__ == '__main__':
    unittest.main()
