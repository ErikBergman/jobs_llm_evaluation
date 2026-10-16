"""Synthetic fixtures retain recruitment structures observed on 2026-10-16; no live HTTP in tests."""
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import employer_jobs as e
from linkedin_guest_jobs import default_matching_config
from match_jobs import load_waiting_room_jobs, prepare_waiting_room_input, classify_file

AD = '<h2>Responsibilities</h2><p>Build clinical data integrations and reliable automated services.</p><h2>Qualifications</h2><ul><li>Python and REST API experience.</li><li>Engineer with laboratory systems knowledge.</li></ul><h2>We offer</h2><p>Flexible working hours and training.</p>'
PLAIN_AD = 'Responsibilities\nBuild clinical data integrations and reliable automated services.\nQualifications\nPython and REST API experience.\nEngineer with laboratory systems knowledge.\nWe offer\nFlexible working hours and training.'


def rss(items):
    return '<rss xmlns:g="http://base.google.com/ns/1.0" xmlns:tt="https://teamtailor.com/locations"><channel><title>Careers</title>' + ''.join(items) + '</channel></rss>'


def item(url, location='Lund', description=AD, extra=''):
    return f'<item><title>Integration Engineer</title><link>{url}</link><description><![CDATA[{description}]]></description><g:location>{location}</g:location>{extra}</item>'


class FakeClient:
    responses = {}

    def __init__(self, hosts, delay=0, timeout=30):
        self.requests = 0

    def get(self, url, data=None):
        self.requests += 1
        key = (url, data['offset']) if data is not None else url
        result = self.responses[key]
        if isinstance(result, Exception):
            raise result
        return result


class EmployerIndexTests(unittest.TestCase):
    def setUp(self):
        self.sources = {s['id']: s for s in e.load_sources()}
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = default_matching_config()
        FakeClient.responses = {}

    def run_source(self, source, **kwargs):
        return e.index_sources([self.sources[source]], self.root, self.config, delay=0,
                               client_factory=FakeClient, **kwargs)

    def test_teamtailor_feed_and_delegated_jsonld_share_normalization(self):
        source = self.sources['cellavision']
        direct = source['url'] + '/1001-support'
        delegated = 'https://rekrytering.adeptus.se/jobs/1002-developer'
        job_ld = {'@context': 'https://schema.org', '@graph': [{'@type': 'BreadcrumbList'}, {
            '@type': ['Thing', 'JobPosting'], 'title': 'Backend Engineer', 'description': AD,
            'datePosted': '2026-10-01', 'jobLocation': {'address': {'addressLocality': 'Lund', 'addressCountry': 'SE'}}}]}
        FakeClient.responses = {
            source['url']: f'<a href="{direct}">Support</a><a href="{delegated}">Developer</a>',
            source['feed_url']: rss([item(direct, 'Other', extra='<tt:locations><tt:location><tt:city>Lund</tt:city></tt:location></tt:locations>')]),
            delegated: '<script type="application/ld+json">' + json.dumps(job_ld) + '</script>',
        }
        jobs, audits = self.run_source('cellavision')
        self.assertEqual(len(jobs), 2)
        self.assertEqual(audits[0]['discovered'], 2)
        self.assertEqual({j['extraction_method'] for j in jobs}, {'rss', 'jsonld'})
        for job in jobs:
            self.assertTrue(job['prefilter_pass'])
            self.assertIn('Python and REST API', job['requirements_text'])
            self.assertNotIn('Flexible working hours', job['requirements_text'])
            self.assertIn('Flexible working hours', job['description'])
            self.assertNotIn('<p>', job['description'])

    def test_varbi_joins_translated_urls_by_native_id_and_filters_metadata(self):
        source = self.sources['lund-university']
        urls = ['https://lu.varbi.com/en/what:job/jobID:1001/', 'https://lu.varbi.com/en/what:job/jobID:1002/']
        FakeClient.responses = {
            source['feed_url']: rss([item(u, '', PLAIN_AD + '\nLund University has offices across Skåne.') for u in urls]),
            source['url']: '<table>' + ''.join(f'<tr><td class="pos-title"><a href="{u.replace("/en/", "/se/")}">Engineer</a></td><td class="pos-town">{city}</td><td class="pos-ends">2099-10-31</td></tr>' for u, city in zip(urls, ['Lund', 'Helsingborg'])) + '</table>',
        }
        jobs, audits = self.run_source('lund-university')
        self.assertEqual([j['source_job_id'] for j in jobs], ['1001'])
        self.assertEqual(audits[0]['location_filtered'], 1)
        self.assertIn('Python and REST API', jobs[0]['requirements_text'])

    def test_successfactors_encoded_feed_and_expiration(self):
        source = self.sources['tetra-pak']
        FakeClient.responses[source['feed_url']] = rss([
            item('https://jobs.tetrapak.com/job/Lund-Engineer/1001/', description=e.escape(AD)),
            item('https://jobs.tetrapak.com/job/Lund-Old/1002/', extra='<g:expiration_date>2020-01-01</g:expiration_date>'),
            item('https://jobs.tetrapak.com/job/Pune-Engineer/1003/', 'Pune, IN', AD + '<p>Our headquarters is in Lund.</p>'),
        ])
        jobs, audits = self.run_source('tetra-pak')
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['source_job_id'], '1001')
        self.assertNotIn('&lt;', jobs[0]['description'])
        self.assertEqual(audits[0]['expired'], 1)
        self.assertEqual(audits[0]['location_filtered'], 1)

    def test_workday_pagination_and_multilocation_details(self):
        source = self.sources['axis']
        base = source['api_url']
        FakeClient.responses = {
            (base + '/jobs', 0): json.dumps({'total': 2, 'facets': [{'facetParameter': 'locationMainGroup', 'values': [{'facetParameter': 'locations', 'values': [{'id': 'lund-id', 'descriptor': 'Sweden - Lund'}]}]}], 'jobPostings': [{'externalPath': '/job/Other/Engineer_R-1', 'title': 'Engineer', 'locationsText': '2 Locations'}]}),
            (base + '/jobs', 1): json.dumps({'total': 2, 'jobPostings': [{'externalPath': '/job/Remote/Engineer_R-2', 'title': 'Engineer', 'locationsText': 'London'}]}),
            base + '/job/Other/Engineer_R-1': json.dumps({'jobPostingInfo': {'jobReqId': 'R-1', 'title': 'Integration Engineer', 'jobDescription': AD, 'location': 'Sweden - Lund', 'canApply': True, 'startDate': '2026-10-01'}}),
            base + '/job/Remote/Engineer_R-2': json.dumps({'jobPostingInfo': {'jobReqId': 'R-2', 'title': 'Integration Engineer', 'jobDescription': AD, 'location': 'London', 'canApply': True}}),
        }
        jobs, audits = self.run_source('axis', page_size=1)
        self.assertEqual([j['job_id'] for j in jobs], ['employer:axis:R-1'])
        self.assertEqual(audits[0]['requests'], 5)
        self.assertEqual(audits[0]['location_filtered'], 1)
        jobs, audits = self.run_source('axis', page_size=1, max_pages=1)
        self.assertFalse(jobs)
        self.assertEqual(audits[0]['status'], 'failed')
        self.assertIn('pagination cap', audits[0]['errors'][0]['message'])

    def reachmee_responses(self, country='Sweden', text=AD + '<p>The position is based in Lund, Sweden.</p>'):
        source = self.sources['ess']
        script = 'https://web103.reachmee.com/customerjs/I003-307-5.js'
        base = 'https://web103.reachmee.com/ext/I003/307/'
        board = base + 'main?site=5&validator=public&lang=UK'
        detail = base + 'job?job_id=1001'
        return {
            source['url']: f'<script src="{script}"></script>',
            script: "var iFrameUrl='https://web103.reachmee.com/ext/';var iid='I003';var customer='307';var site='5';var validator='public';var langDef='UK';",
            board: f'<table id="jobsTable"><tr class="jobs"><td><a href="{detail}">Integration Engineer</a></td><td class="jobcountry">{country}</td></tr></table>',
            detail: '<h1>Integration Engineer</h1><div class="jobDescription">' + text + '</div>',
        }

    def test_reachmee_bootstrap_and_explicit_workplace(self):
        FakeClient.responses = self.reachmee_responses()
        jobs, audits = self.run_source('ess')
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['location'], 'Lund, Sweden')
        self.assertEqual(jobs[0]['location_extraction_method'], 'explicit_workplace_sentence')
        self.assertEqual(audits[0]['status'], 'ok')

    def test_reachmee_mention_of_facility_does_not_establish_workplace(self):
        for text in [AD + '<p>ESS has a research facility in Lund.</p>', AD + '<p>The position is not based in Lund.</p>']:
            FakeClient.responses = self.reachmee_responses(text=text)
            jobs, audits = self.run_source('ess')
            self.assertFalse(jobs)
            self.assertEqual(audits[0]['location_unknown'], 1)
            self.assertEqual(audits[0]['status'], 'partial')
        FakeClient.responses = self.reachmee_responses(country='Denmark')
        jobs, audits = self.run_source('ess')
        self.assertFalse(jobs)
        self.assertEqual(audits[0]['location_filtered'], 1)
        self.assertEqual(audits[0]['status'], 'ok')

    def test_invalid_response_is_failure_but_valid_empty_feed_is_success(self):
        source = self.sources['tetra-pak']
        for body in ['<html>Access denied</html>', 'broken xml']:
            FakeClient.responses[source['feed_url']] = body
            jobs, audits = self.run_source('tetra-pak')
            self.assertFalse(jobs)
            self.assertEqual(audits[0]['status'], 'failed')
        FakeClient.responses[source['feed_url']] = rss([])
        jobs, audits = self.run_source('tetra-pak')
        self.assertFalse(jobs)
        self.assertEqual(audits[0]['status'], 'ok')

    def test_failure_in_one_source_preserves_another_sources_jobs(self):
        source = self.sources['tetra-pak']
        FakeClient.responses = {
            (self.sources['axis']['api_url'] + '/jobs', 0): json.dumps(['unexpected provider error response']),
            source['feed_url']: rss([item('https://jobs.tetrapak.com/job/Lund-Engineer/1001/')]),
        }
        jobs, audits = e.index_sources([self.sources['axis'], source], self.root, self.config, delay=0, client_factory=FakeClient)
        self.assertEqual(len(jobs), 1)
        self.assertEqual([a['status'] for a in audits], ['failed', 'ok'])

    def test_repeated_runs_skip_indexed_ads_and_audits_do_not_mark_failures_seen(self):
        source = self.sources['tetra-pak']
        url = 'https://jobs.tetrapak.com/job/Lund-Engineer/1001/'
        FakeClient.responses[source['feed_url']] = rss([item(url)])
        jobs, audits = self.run_source('tetra-pak')
        e.write_results(jobs, audits, self.root)
        again, audits = self.run_source('tetra-pak')
        self.assertEqual(again, [])
        self.assertEqual(audits[0]['already_seen'], 1)
        self.assertEqual(e.load_seen_job_ids(self.root), {'employer:tetra-pak:1001'})

    def test_employer_and_linkedin_ads_share_existing_matcher_without_id_collision(self):
        source = self.sources['tetra-pak']
        FakeClient.responses[source['feed_url']] = rss([item('https://jobs.tetrapak.com/job/Lund-Engineer/1001/')])
        jobs, audits = self.run_source('tetra-pak')
        e.write_results(jobs, audits, self.root)
        waiting = self.root / 'waiting_room' / 'linkedin'
        waiting.mkdir()
        (waiting / 'linkedin_jobs_sample.json').write_text(json.dumps([{'job_id': '1001', 'title': 'Engineer', 'company': 'Another employer', 'location': 'Lund', 'description': 'We need an engineer to build reliable software.'}]))
        self.assertEqual(len(load_waiting_room_jobs(self.root)), 2)
        path = prepare_waiting_room_input(self.root, 'test')
        _, _, _, metadata = classify_file(path, self.root)
        self.assertEqual(metadata['job_count'], 2)
        self.assertEqual(metadata['hits_count'], 2)

    def test_exact_canonical_url_dedup_and_tracking_parameter_removal(self):
        self.assertEqual(e.canonical_url('https://example.com/job?job_id=42&utm_source=test&lang=en#apply'),
                         'https://example.com/job?job_id=42&lang=en')
        self.assertNotEqual(e.canonical_url('https://example.com/job?job_id=42'),
                            e.canonical_url('https://example.com/job?job_id=43'))

    def test_host_allowlist_blocks_external_and_non_https_job_links(self):
        for url in ['http://lu.varbi.com/', 'https://lu.varbi.com.evil.example/', 'https://lu.varbi.com@127.0.0.1/', 'https://lu.varbi.com:444/']:
            with self.subTest(url=url), self.assertRaises(e.SourceError):
                e.validate_url(url, ['lu.varbi.com'])
        with self.assertRaises(e.SourceError):
            e.SafeRedirect(['lu.varbi.com']).redirect_request(None, None, 302, '', {}, 'https://example.com/')

    def test_limit_reports_truncation_without_marking_unvisited_jobs_seen(self):
        source = self.sources['tetra-pak']
        FakeClient.responses[source['feed_url']] = rss([item(f'https://jobs.tetrapak.com/job/Lund-Engineer/{i}/') for i in [1001, 1002]])
        jobs, audits = self.run_source('tetra-pak', limit=1)
        self.assertEqual(len(jobs), 1)
        self.assertTrue(audits[0]['selection_truncated'])
        e.write_results(jobs, audits, self.root)
        jobs, _ = self.run_source('tetra-pak', limit=1)
        self.assertEqual(jobs[0]['source_job_id'], '1002')

    def test_workday_location_facet_is_required_and_repeated_pages_fail(self):
        source = self.sources['axis']
        base = source['api_url'] + '/jobs'
        FakeClient.responses = {(base, 0): json.dumps({'total': 2, 'facets': [], 'jobPostings': []})}
        jobs, audits = self.run_source('axis')
        self.assertFalse(jobs)
        self.assertIn('location facet', audits[0]['errors'][0]['message'])
        self.assertEqual(audits[0]['requests'], 1)
        page = {'total': 2, 'facets': [{'facetParameter': 'locations', 'values': [{'id': 'lund', 'descriptor': 'Lund'}]}],
                'jobPostings': [{'externalPath': '/job/Lund/Engineer_R-1', 'title': 'Engineer', 'locationsText': 'Lund'}]}
        FakeClient.responses = {(base, 0): json.dumps(page), (base, 1): json.dumps(page)}
        jobs, audits = self.run_source('axis', page_size=1)
        self.assertFalse(jobs)
        self.assertIn('repeated job IDs', audits[0]['errors'][0]['message'])

    def test_declared_rss_pagination_is_followed_and_cap_is_explicit(self):
        source = self.sources['tetra-pak']
        first = source['feed_url']
        second = first + '?page=2'
        head = rss([item('https://jobs.tetrapak.com/job/Lund-Engineer/1001/')])
        head = head.replace('</channel>', f'<atom:link xmlns:atom="http://www.w3.org/2005/Atom" rel="next" href="{second}"/></channel>')
        FakeClient.responses = {first: head, second: rss([item('https://jobs.tetrapak.com/job/Lund-Engineer/1002/')])}
        jobs, audits = self.run_source('tetra-pak')
        self.assertEqual(len(jobs), 2)
        jobs, audits = self.run_source('tetra-pak', max_pages=1)
        self.assertFalse(jobs)
        self.assertIn('pagination cap', audits[0]['errors'][0]['message'])

    def test_rate_limit_retry_and_long_server_delay_are_observed(self):
        from io import BytesIO
        url = 'https://lu.varbi.com/'
        client = e.Client(['lu.varbi.com'], delay=0)
        response = MagicMock()
        response.__enter__.return_value = response
        response.headers.get_content_charset.return_value = 'utf-8'
        response.read.return_value = b'<rss/>'
        error = e.HTTPError(url, 429, 'Too Many Requests', {'Retry-After': '1'}, BytesIO())
        client.opener.open = MagicMock(side_effect=[error, response])
        with patch('employer_jobs.time.sleep') as sleep:
            self.assertEqual(client.get(url), '<rss/>')
            sleep.assert_any_call(1)
        self.assertEqual(client.requests, 2)
        error.close()
        error = e.HTTPError(url, 429, 'Too Many Requests', {'Retry-After': '61'}, BytesIO())
        client.opener.open = MagicMock(side_effect=error)
        with patch('employer_jobs.time.sleep'), self.assertRaises(e.SourceError):
            client.get(url)
        self.assertEqual(client.opener.open.call_count, 1)
        error.close()

    def test_expiry_date_is_inclusive_and_time_is_respected(self):
        now = datetime(2026, 10, 16, 12, tzinfo=timezone.utc)
        self.assertFalse(e.expired('2026-10-16', now))
        self.assertTrue(e.expired('2026-10-15', now))
        self.assertTrue(e.expired('2026-10-16T10:00:00Z', now))
        with self.assertRaises(e.SourceError):
            e.expired('not-a-date', now)


if __name__ == '__main__':
    unittest.main()
