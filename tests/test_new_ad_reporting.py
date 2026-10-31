import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import telegram_notify as notify


NOW = datetime(2026, 10, 31, 13, tzinfo=timezone.utc)


def save(root, relative, payload):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding='utf-8')
    return path


class NewAdReportingTests(unittest.TestCase):
    def test_bootstrap_reports_three_new_ads_separately_from_forty_evaluated_and_seventy_one_pending(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            old = [{'job_id': f'old-{i}', 'collected_at': '2026-10-28T17:00:00Z'} for i in range(108)]
            fresh = [{'job_id': f'fresh-{i}', 'collected_at': stamp} for i, stamp in enumerate([
                '2026-10-31T00:10:00Z', '2026-10-31T06:00:00Z', '2026-10-31T12:00:00Z'])]
            current = save(root, 'discard/20261031_125000/jobs.json', old[:40])
            save(root, 'waiting_room/20261031_125000/jobs.json', old[40:] + fresh)
            save(root, 'discard/previous/jobs_match_metadata.json', {
                'matched_at': '2026-10-30T22:24:00Z', 'input_path': 'previous.json', 'decisions': []})
            save(root, 'discard/20261031_125000/jobs_match_metadata.json', {
                'matched_at': '2026-10-31T12:50:00Z', 'input_path': str(current),
                'job_count': 40, 'matcher': 'openai', 'decisions': [
                    {'job_id': j['job_id'], 'hit': False, 'stage': 'triage', 'matcher': 'openai'} for j in old[:40]]})
            message = notify.summary_message(root, str(current), 12, NOW)
        self.assertIn('New ads since previous report: 3', message)
        self.assertIn('Evaluated this run: 40 (40 sent to AI)', message)
        self.assertIn('Waiting for evaluation: 71', message)

    def test_checkpoint_counts_unique_new_ids_even_without_timestamps_or_after_old_copies_reappear(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            baseline = notify.DiscoverySummary(frozenset({'old'}), 1, 0, NOW - timedelta(days=1))
            notify.save_report_checkpoint(root, baseline, NOW - timedelta(hours=2))
            save(root, 'waiting_room/run/jobs.json', [{'job_id': 'new'}, {'job_id': 'new'}])
            discovery = notify.discovery_since_previous_report(root, [], None, NOW)
            self.assertEqual(discovery.new_ads, 1)
            self.assertEqual(discovery.memory_count, 1)
            notify.save_report_checkpoint(root, discovery, NOW)
            save(root, 'hits/run/jobs_hits.json', [{'job_id': 'old'}, {'job_id': 'new'}])
            next_report = notify.discovery_since_previous_report(root, [], None, NOW + timedelta(hours=1))
            self.assertEqual(next_report.new_ads, 0)
            self.assertEqual(notify.accumulated_job_count(root), 2)

    def test_dry_run_and_failed_send_preserve_checkpoint_and_successful_send_advances_it(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            now = datetime.now(timezone.utc)
            baseline = notify.DiscoverySummary(frozenset({'old'}), 1, 0, now - timedelta(days=1))
            notify.save_report_checkpoint(root, baseline, now - timedelta(hours=2))
            save(root, 'waiting_room/run/jobs.json', [{'job_id': 'old'}, {'job_id': 'new'}])
            state_path = root / notify.REPORT_STATE_PATH
            before = state_path.read_bytes()
            args = ['telegram_notify.py', '--results-root', str(root)]
            with patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'test-only-token', 'TELEGRAM_CHAT_ID': 'test-chat'}):
                with patch('sys.argv', args + ['--dry-run']), patch.object(notify, 'send_telegram_message') as send, redirect_stdout(io.StringIO()):
                    self.assertEqual(notify.main(), 0)
                    send.assert_not_called()
                self.assertEqual(state_path.read_bytes(), before)
                with patch('sys.argv', args), patch.object(notify, 'send_telegram_message', side_effect=ValueError('Test send failure')), \
                     redirect_stdout(io.StringIO()), patch('sys.stderr', new_callable=io.StringIO):
                    self.assertEqual(notify.main(), 1)
                self.assertEqual(state_path.read_bytes(), before)
                with patch('sys.argv', args), patch.object(notify, 'send_telegram_message') as send, redirect_stdout(io.StringIO()):
                    self.assertEqual(notify.main(), 0)
                    self.assertIn('New ads since previous report: 1', send.call_args.args[2])
                self.assertEqual(json.loads(state_path.read_text())['known_job_ids'], ['new', 'old'])
                self.assertEqual(notify.discovery_since_previous_report(root, [], None, datetime.now(timezone.utc)).new_ads, 0)

    def test_bootstrap_uses_earliest_collection_evidence_and_never_posting_dates(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            save(root, 'discard/20261031_120000/jobs.json', [
                {'job_id': 'backlog', 'first_seen_at': '2026-10-31T12:00:00Z', 'indexed_at': '2026-10-28T12:00:00Z'},
                {'job_id': 'new', 'collected_at': '2026-10-31T12:00:00Z', 'posted_date': '2024-01-01'},
            ])
            previous = [{'matched_at': '2026-10-30T22:00:00Z', 'input_path': 'previous.json'}]
            self.assertEqual(notify.discovery_since_previous_report(root, previous, None, NOW).new_ads, 1)

    def test_unknown_legacy_collection_dates_establish_baseline_without_inventing_new_ads(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            save(root, 'discard/undated/jobs.json', [{'job_id': 'old', 'posted_date': '2026-10-31'}])
            discovery = notify.discovery_since_previous_report(root, [], None, NOW)
            self.assertIsNone(discovery.new_ads)
            notify.save_report_checkpoint(root, discovery, NOW)
            self.assertEqual(notify.discovery_since_previous_report(root, [], None, NOW + timedelta(seconds=1)).new_ads, 0)

    def test_corrupt_checkpoint_is_not_silently_reset(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = save(root, notify.REPORT_STATE_PATH, {'version': 1, 'sent_at': None, 'known_job_ids': []})
            before = state.read_bytes()
            with self.assertRaisesRegex(ValueError, 'existing state left untouched'):
                notify.discovery_since_previous_report(root, [], None, NOW)
            self.assertEqual(state.read_bytes(), before)

    def test_genuinely_first_report_labels_its_initial_window(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            save(root, 'waiting_room/run/jobs.json', [
                {'job_id': 'old', 'collected_at': (NOW - timedelta(hours=13)).isoformat()},
                {'job_id': 'new', 'collected_at': (NOW - timedelta(hours=1)).isoformat()},
            ])
            message = notify.summary_message(root, None, 12, NOW)
        self.assertIn('New ads in last 12 hours (first report): 1', message)


if __name__ == '__main__':
    unittest.main()
