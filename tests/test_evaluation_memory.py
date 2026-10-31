import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import match_jobs as m


class EvaluationMemoryTests(unittest.TestCase):
    def test_only_completed_outputs_and_valid_decisions_mark_ids_as_evaluated(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            folder = root / 'discard' / 'run'
            folder.mkdir(parents=True)
            (folder / 'raw.json').write_text(json.dumps([{'job_id': 'raw-only'}]))
            (folder / 'jobs_discard.json').write_text(json.dumps([{'job_id': 'discarded'}]))
            (folder / 'jobs_match_metadata.json').write_text(json.dumps({'decisions': [
                {'job_id': 'final', 'hit': False}, {'job_id': 'incomplete', 'hit': None}]}))
            self.assertEqual(m.evaluated_job_ids(root), {'discarded', 'final'})

    def test_automatic_evaluation_never_sends_an_already_recorded_ad_to_the_model(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            previous = root / 'discard' / 'previous'
            previous.mkdir(parents=True)
            (previous / 'jobs_match_metadata.json').write_text(json.dumps({
                'matcher': 'openai', 'decisions': [{'job_id': 'old', 'hit': False}]}))
            queue = root / 'waiting_room' / 'run'
            queue.mkdir(parents=True)
            old = {'job_id': 'old', 'title': 'Previously reviewed', 'description': 'DO NOT SEND AGAIN'}
            fresh = {'job_id': 'fresh', 'title': 'New job', 'description': 'Fresh ad'}
            (queue / 'jobs.json').write_text(json.dumps([old, fresh]))
            profile = root / 'profile.txt'
            profile.write_text('Candidate profile')
            requests = []
            def fake_model(jobs, *args, **kwargs):
                requests.append(jobs)
                return [{'job_id': j['job_id'], 'title': j['title'], 'hit': False,
                         'stage': 'triage', 'matcher': 'openai', 'reason': 'No fit'} for j in jobs]
            arguments = ['match_jobs.py', '--waiting-room', '--output-root', str(root), '--matcher', 'openai',
                         '--job-profile', str(profile), '--timestamp', 'first', '--llm-limit', '40']
            with patch('sys.argv', arguments), patch.dict('os.environ', {'CHEAT_MODE': 'false'}), \
                 patch.object(m, 'DEFAULT_MATCHING_CONFIG_PATH', root / 'missing.json'), \
                 patch.object(m, 'openai_prefiltered_two_stage_decisions', side_effect=fake_model), \
                 redirect_stdout(io.StringIO()):
                self.assertEqual(m.main(), 0)
                queue = root / 'waiting_room' / 'again'
                queue.mkdir(parents=True)
                (queue / 'jobs.json').write_text(json.dumps([fresh]))
                self.assertEqual(m.main(), 0)
            self.assertEqual(len(requests), 1)
            self.assertEqual([j['job_id'] for j in requests[0]], ['fresh'])
            self.assertEqual(m.load_waiting_room_jobs(root, exclude_evaluated=True), [])
            metadata = json.loads((root / 'discard' / 'first' / 'waiting_room_jobs_match_metadata.json').read_text())
            self.assertEqual(metadata['waiting_room_before'], 1)
            self.assertEqual(metadata['deferred_job_count'], 0)


if __name__ == '__main__':
    unittest.main()
