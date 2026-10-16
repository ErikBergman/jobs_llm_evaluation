import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import deduplicate_jobs as cli
import employer_jobs as employers
import job_deduplication as d
from linkedin_guest_jobs import load_seen_job_ids
from match_jobs import load_waiting_room_jobs, classify_file, prepare_waiting_room_input
from tests.test_job_deduplication import NOW, job


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = d.load_config()
        self.older = job('older', first_seen_at=(NOW-timedelta(days=1)).isoformat())
        self.new = job('employer:axis:new', source='employer', company='Axis', indexed_at=NOW.isoformat(), source_id='axis')

    def save(self, path, jobs):
        target=self.root/path
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(json.dumps(jobs))
        return target

    @staticmethod
    def duplicate(new, candidates):
        return {'decision':'duplicate','duplicate_of':candidates[0].record_id,'reason':'Matching role-specific details.'}

    def test_shared_waiting_room_removes_only_newer_copy_then_classifies_once(self):
        self.save('waiting_room/20261015_100000/linkedin.json',[self.older])
        self.save('waiting_room/20261016_100000/employer_jobs.json',[self.new])
        audit=cli.process_waiting_room(self.root,self.config,self.duplicate,NOW)
        self.assertEqual(audit['duplicates'],1)
        waiting=load_waiting_room_jobs(self.root)
        self.assertEqual([j['job_id'] for j in waiting],['older'])
        archive=json.loads(next((self.root/'duplicates').rglob('duplicate_jobs.json')).read_text())
        self.assertEqual(archive[0]['job_id'],self.new['job_id'])
        self.assertEqual(archive[0]['deduplication']['duplicate_of'],'older')
        self.assertIn(self.new['job_id'],load_seen_job_ids(self.root))
        input_path=prepare_waiting_room_input(self.root,'test')
        *_,metadata=classify_file(input_path,self.root,'test')
        self.assertEqual(metadata['job_count'],1)

    def test_existing_hit_survives_and_new_company_ad_is_removed(self):
        old_path=self.save('hits/20261015_100000/jobs_hits.json',[self.older])
        original=old_path.read_bytes()
        self.save('waiting_room/20261016_100000/jobs.json',[self.new])
        cli.process_waiting_room(self.root,self.config,self.duplicate,NOW)
        self.assertEqual(old_path.read_bytes(),original)
        self.assertFalse(load_waiting_room_jobs(self.root))

    def test_reverse_order_keeps_company_ad_and_discards_later_linkedin_copy(self):
        old_company=dict(self.new,first_seen_at=(NOW-timedelta(days=1)).isoformat())
        new_linkedin=job('newer',first_seen_at=NOW.isoformat())
        self.save('hits/20261015_100000/jobs_hits.json',[old_company])
        self.save('waiting_room/20261016_100000/linkedin.json',[new_linkedin])
        audit=cli.process_waiting_room(self.root,self.config,self.duplicate,NOW)
        self.assertEqual(audit['duplicates'],1)
        archived=json.loads(next((self.root/'duplicates').rglob('duplicate_jobs.json')).read_text())
        self.assertEqual(archived[0]['deduplication']['duplicate_of'],old_company['job_id'])

    def test_different_roles_are_retained_and_repeat_uses_cache(self):
        self.save('discard/20261015_100000/jobs.json',[self.older])
        self.save('waiting_room/20261016_100000/jobs.json',[self.new])
        def different(*args):
            return {'decision':'different','duplicate_of':None,'reason':'Separate requisitions for distinct teams.'}
        first=cli.process_waiting_room(self.root,self.config,different,NOW)
        def forbidden(*args):
            self.fail('Repeat must use saved decision')
        repeat=cli.process_waiting_room(self.root,self.config,forbidden,NOW+timedelta(minutes=1))
        self.assertEqual(first['llm_calls'],1)
        self.assertEqual(repeat['cache_hits'],1)
        self.assertEqual(repeat['llm_calls'],0)
        self.assertEqual(len(load_waiting_room_jobs(self.root)),1)

    def test_archive_is_written_before_any_removal_and_recovers_interrupted_rewrite(self):
        self.save('hits/20261015_100000/hits.json',[self.older])
        waiting=self.save('waiting_room/20261016_100000/jobs.json',[self.new])
        real_write=d.atomic_json
        def fail_waiting(path,value):
            if 'waiting_room' in Path(path).parts:
                raise OSError('simulated disk error')
            return real_write(path,value)
        with patch.object(d,'atomic_json',side_effect=fail_waiting):
            with self.assertRaises(OSError):
                cli.process_waiting_room(self.root,self.config,self.duplicate,NOW)
        self.assertTrue(list((self.root/'duplicates').rglob('duplicate_jobs.json')))
        self.assertEqual(len(json.loads(waiting.read_text())),1)
        def forbidden(*args):
            self.fail('Previously confirmed duplicate must not need another LLM call')
        audit=cli.process_waiting_room(self.root,self.config,forbidden,NOW+timedelta(minutes=1))
        self.assertEqual(audit['previously_retired'],1)
        self.assertFalse(load_waiting_room_jobs(self.root))

    def test_dry_run_and_off_mode_do_not_write_or_call_the_llm(self):
        self.save('hits/20261015_100000/hits.json',[self.older])
        self.save('waiting_room/20261016_100000/jobs.json',[self.new])
        before={str(p):p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        audit=cli.process_waiting_room(self.root,self.config,now=NOW,dry_run=True)
        self.assertEqual(audit['ads_with_candidates'],1)
        self.assertEqual(audit['llm_calls'],0)
        self.assertEqual(cli.main(['--results-root',str(self.root),'--mode','off']),0)
        after={str(p):p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(before,after)

    def test_budget_zero_and_anonymous_inputs_remain_in_queue(self):
        self.save('hits/20261015_100000/hits.json',[self.older])
        anonymous={'title':'Imported ad','company':'Example','description':'Description without an ID'}
        self.save('waiting_room/20261016_100000/jobs.json',[self.new,anonymous])
        audit=cli.process_waiting_room(self.root,replace(self.config,max_calls=0),self.duplicate,NOW)
        self.assertEqual(audit['budget_kept'],1)
        self.assertEqual(audit['anonymous_kept'],1)
        self.assertEqual(len(load_waiting_room_jobs(self.root)),2)

    def test_employer_importer_hook_archives_before_waiting_room_and_preserves_metadata(self):
        self.save('hits/20261015_100000/hits.json',[self.older])
        audit={'source_id':'axis','employer':'Axis','indexed':1,'status':'ok'}
        with patch.object(employers,'index_sources',return_value=([self.new],[audit])), \
             patch.object(d,'OpenAIDecider',return_value=self.duplicate):
            status=employers.main(['--source','axis','--results-root',str(self.root)])
        self.assertEqual(status,0)
        self.assertFalse(load_waiting_room_jobs(self.root))
        metadata=json.loads(next((self.root/'waiting_room').rglob('*metadata.json')).read_text())
        self.assertEqual(metadata['deduplication']['duplicates'],1)
        self.assertEqual(metadata['sources'][0]['indexed_before_deduplication'],1)
        self.assertEqual(metadata['sources'][0]['indexed'],0)
        self.assertIn(self.new['job_id'],load_seen_job_ids(self.root))


    def test_workflow_checks_duplicates_before_profile_classification(self):
        workflow=(Path(__file__).resolve().parents[1]/'.github/workflows/job-search.yml').read_text()
        duplicate_step=workflow.index('- name: Deduplicate cross-source waiting-room jobs')
        classify_step=workflow.index('- name: Classify waiting-room jobs')
        self.assertLess(duplicate_step,classify_step)
        block=workflow[duplicate_step:classify_step]
        self.assertIn("steps.schedule_gate.outputs.report_job == 'true'",block)
        self.assertIn('secrets.OPENAI_API_KEY',block)
        self.assertIn('--results-root "$RUNTIME_RESULTS_DIR"',block)
        self.assertIn('JOB_DEDUPLICATION_MODE',block)


if __name__=='__main__':
    unittest.main()
