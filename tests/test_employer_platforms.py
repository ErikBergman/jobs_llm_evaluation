"""Observed public ATS shapes, synthetic content; these tests make no network calls."""
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import employer_jobs as e
from linkedin_guest_jobs import default_matching_config
from tests.test_employer_jobs import AD, item, rss


class Client:
    responses = {}
    calls = []

    def __init__(self, hosts, delay=0, timeout=30):
        self.requests = 0

    def get(self, url, data=None):
        self.requests += 1
        self.calls.append((url, data))
        value = self.responses(url, data) if callable(self.responses) else self.responses[url]
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, str) else json.dumps(value)


def ld(city='Lund'):
    return '<script type="application/ld+json">' + json.dumps({
        '@type': 'JobPosting', 'title': 'Integration Engineer', 'description': AD,
        'jobLocation': {'address': {'addressLocality': city}}}) + '</script>'


def flight(chunks):
    return ''.join('<script>self.__next_f.push(' + json.dumps([1, c]) + ')</script>' for c in chunks)


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.sources = {s['id']: s for s in e.load_sources()}
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        Client.responses = {}
        Client.calls = []

    def run_source(self, name, **kwargs):
        return e.index_sources([self.sources[name]], self.root, default_matching_config(),
                               client_factory=Client, delay=0, **kwargs)

    def test_catalog_config_covers_every_employer_once(self):
        catalog = json.loads(e.CONFIG_PATH.with_name('job_listing_pages_lund.json').read_text())
        self.assertEqual(len(self.sources), 48)
        self.assertEqual({s['employer'] for s in self.sources.values()}, {s['employer'] for s in catalog})

    def test_workspace_literal_data_and_country_only_exclusion(self):
        source = self.sources['camurus']
        script = 'https://app.workspacerecruit.com/api/publish/portal/js/public-id'
        url = 'https://app.workspacerecruit.com/api/v1/ad/12'
        Client.responses = {source['url']: f'<script src="{script}"></script>',
            script: 'const recruitments = ' + json.dumps([
                {'id': 12, 'title': 'Integration Engineer', 'city': 'Lund', 'country': 'Sweden', 'lastDate': '2099-01-01'},
                {'id': 13, 'title': 'Researcher', 'city': '', 'country': 'Switzerland'}]) + '; alert("never executed")',
            url: '<h1>Integration Engineer</h1><div class="content">' + AD + '</div>'}
        jobs, audits = self.run_source('camurus')
        self.assertEqual(len(jobs), 1)
        self.assertEqual(audits[0]['location_filtered'], 1)
        self.assertEqual(audits[0]['requests'], 3)

    def test_workspace_missing_array_is_failure(self):
        source = self.sources['camurus']
        script = 'https://app.workspacerecruit.com/api/publish/portal/js/public-id'
        Client.responses = {source['url']: f'<script src="{script}"></script>', script: 'const recruitments = getPrivateData()'}
        jobs, audits = self.run_source('camurus')
        self.assertEqual(audits[0]['status'], 'failed')

    def eightfold(self, repeat=False):
        def respond(url, data):
            if '/api/pcsx/search?' in url:
                start = int(parse_qs(urlsplit(url).query)['start'][0])
                key = 1 if repeat else start + 1
                return {'status': 200, 'data': {'count': 2, 'positions': [
                    {'id': key, 'name': 'Integration Engineer', 'locations': ['Lund' if start == 0 else 'Malmö'],
                     'positionUrl': '/careers/job/' + str(key)}]}}
            return ld()
        return respond

    def test_eightfold_paginates_then_checks_actual_city(self):
        Client.responses = staticmethod(self.eightfold())
        jobs, audits = self.run_source('ericsson', page_size=1)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(audits[0]['discovered'], 2)
        self.assertEqual(audits[0]['location_filtered'], 1)

    def test_eightfold_repeated_page_is_failure(self):
        Client.responses = staticmethod(self.eightfold(repeat=True))
        jobs, audits = self.run_source('ericsson', page_size=1)
        self.assertEqual(audits[0]['status'], 'failed')
        self.assertFalse(jobs)

    def test_jibe_additional_city_full_fields_and_language_count(self):
        source = self.sources['schneider-electric']
        rows = [{'slug': '12', 'title': 'Integration Engineer', 'city': 'Stockholm',
                 'additional_locations': [{'city': 'Lund'}], 'description': AD,
                 'qualifications': '<p>Laboratory experience is required.</p>'}]
        def respond(url, data):
            if parse_qs(urlsplit(url).query)['page'][0] == '1':
                return {'jobs': [{'data': r} for r in rows], 'count': 1, 'totalCount': 2}
            return {'jobs': [{'data': {'slug': '13', 'title': 'Ingenieur', 'city': 'Berlin', 'description': AD}}], 'count': 1, 'totalCount': 2}
        Client.responses = staticmethod(respond)
        jobs, audits = self.run_source('schneider-electric')
        self.assertEqual(len(jobs), 1)
        self.assertIn('Laboratory experience', jobs[0]['description'])
        self.assertIn('Lund', jobs[0]['location'])
        self.assertEqual(audits[0]['requests'], 2)
        self.assertEqual(audits[0]['location_filtered'], 1)

    def test_bamboohr_empty_schema_and_advertised_count(self):
        source = self.sources['probi']
        Client.responses = {source['api_url']: {'meta': {'totalCount': 0}, 'result': []}}
        self.assertEqual(self.run_source('probi')[1][0]['status'], 'ok')
        Client.responses[source['api_url']]['meta']['totalCount'] = 1
        self.assertEqual(self.run_source('probi')[1][0]['status'], 'failed')

    def test_hailey_byte_length_text_reassembled_across_chunks(self):
        source = self.sources['modelon']
        url = source['url'] + 'en-GB/job/company/recruitment/ad'
        board = '4:{"jobAds":' + json.dumps([{'jobAdId': 'ad', 'link': url, 'title': 'Engineer', 'locationName': 'Lund'}]) + '}\n'
        text = '# Qualifications\nPython integrations and laboratory systems.\nAutomatisering och försök med åäö.'
        stream = 'f:T' + format(len(text.encode('utf-8')), 'x') + ',' + text
        Client.responses = {source['url']: flight([board[:35], board[35:]]), url: flight([stream[:50], stream[50:]])}
        jobs, audits = self.run_source('modelon')
        self.assertEqual(len(jobs), 1)
        self.assertIn('åäö', jobs[0]['description'])
        self.assertEqual(audits[0]['status'], 'ok')

    def test_hailey_missing_description_is_failure(self):
        raw = e.RawJob('ad', 'https://example.org/job', location='Lund', method='hailey-flight')
        Client.responses = {raw.url: flight(['4:{"heading":"Engineer"}\n'])}
        with self.assertRaises(e.SourceError):
            e.complete_detail(raw, {'adapter': 'hailey'}, Client([]))

    def test_etteplan_embedded_duplicates_and_metadata(self):
        source = self.sources['etteplan']
        url = 'https://emp.jobylon.com/jobs/12-engineer/'
        row = {'title': 'Engineer', 'openPositionMeta': {'apiId': '12', 'advertisementUrl': url, 'applicationDue': '31/12/2099'},
               'openPositionLocations': {'nodes': [{'name': 'Lund'}, {'name': 'Malmö'}]}}
        data = {'props': {'initial': [row], 'cached': [row]}}
        Client.responses = {source['url']: '<script id="__NEXT_DATA__" type="application/json">' + json.dumps(data) + '</script>', url: ld('')}
        jobs, audits = self.run_source('etteplan')
        self.assertEqual(audits[0]['discovered'], 1)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['valid_through'], '2099-12-31')
        self.assertIn('Lund', jobs[0]['location'])

    def test_html_empty_state_and_missing_container_differ(self):
        source = self.sources['vantive']
        Client.responses = {source['url']: '<section id="search-results" data-total-results="0" data-total-pages="1">0 jobs found</section>'}
        self.assertEqual(self.run_source('vantive')[1][0]['status'], 'ok')
        Client.responses[source['url']] = '<html>Access denied</html>'
        self.assertEqual(self.run_source('vantive')[1][0]['status'], 'failed')

    def test_talentbrew_pagination_and_total_check(self):
        source = self.sources['arm']
        def respond(url, data):
            second = 'p=2' in url
            num = 2 if second else 1
            return (f'<section id="search-results" data-total-results="2" data-total-pages="2" data-current-page="{num}">'
                    f'<a href="/job/lund/engineer/33/{num}">Engineer</a></section>') if '/job/' not in url else ld()
        Client.responses = staticmethod(respond)
        jobs, audits = self.run_source('arm')
        self.assertEqual(len(jobs), 2)
        self.assertEqual(audits[0]['status'], 'ok')
        self.assertTrue(any('p=2' in call[0] for call in Client.calls))

    def test_http_disappeared_ad_is_withdrawn_but_403_is_error(self):
        raw = e.RawJob('12', 'https://example.org/job', location='Lund')
        Client.responses = {raw.url: e.HTTPSourceError(404, raw.url)}
        self.assertFalse(e.complete_detail(raw, {'adapter': 'html'}, Client([])).active)
        Client.responses[raw.url] = e.HTTPSourceError(403, raw.url)
        with self.assertRaises(e.SourceError):
            e.complete_detail(raw, {'adapter': 'html'}, Client([]))

    def test_personio_html_fallback_uses_location_component(self):
        raw = e.RawJob('12', 'https://lime-tech.jobs.personio.de/job/12')
        Client.responses = {raw.url: '<h1 class="job-position-title">Engineer</h1><span class="TruncatedLocations_locationText__abc">Köln</span><div class="detail-content-block-conditions">' + AD + '</div>'}
        result = e.complete_detail(raw, {'adapter': 'html'}, Client([]))
        self.assertEqual(result.location, 'Köln')
        self.assertEqual(result.method, 'personio-html')
        self.assertIn('Python', result.description)

    def test_sigma_dates_and_pagination(self):
        def respond(url, data):
            start = int(parse_qs(urlsplit(url).query)['offset'][0])
            r = {'id': str(start), 'title': 'Engineer', 'link': 'https://sigmatechnology.com/open-position-detail/?id=' + str(start),
                 'cities': 'Lund', 'description': AD, 'qualifications': '<p>Laboratory experience.</p>', 'expirationDate': '31 December 2099'}
            return {'data': [r], 'total': '2'}
        Client.responses = staticmethod(respond)
        jobs, audits = self.run_source('sigma-technology', page_size=1)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]['valid_through'], '2099-12-31')
        self.assertIn('Laboratory experience', jobs[0]['description'])

    def test_expired_metadata_avoids_fetching_removed_detail(self):
        source = self.sources['afry']
        Client.responses = {source['api_url']: {'Adverts': [{'Id': 12, 'Title': 'Engineer', 'Cities': [{'Name': 'Lund'}],
                            'DetailUrl': '/old', 'LastApplyDate': '2020-01-01'}], 'CacheCold': False}}
        jobs, audits = self.run_source('afry')
        self.assertFalse(jobs)
        self.assertEqual(audits[0]['expired'], 1)
        self.assertEqual(audits[0]['requests'], 1)
        self.assertEqual(audits[0]['status'], 'ok')

    def test_dokumentera_published_ids_and_structured_location(self):
        source = self.sources['dokumentera']
        block = {'name': 'acf/job-listings', 'jobs': [{'position': 12}, {'position': 13}]}
        def doc(blocks):
            return '<script id="__NEXT_DATA__" type="application/json">' + json.dumps({'props': {'pageProps': {'acfBlocks': blocks}}}) + '</script>'
        url = source['url'] + '/engineer'
        Client.responses = {source['url']: doc([block]),
            source['api_url'] + '?include=12%2C13&per_page=100': [{'id': 12, 'status': 'publish', 'slug': 'engineer', 'title': {'rendered': 'Engineer'}}],
            url: doc([{'name': 'acf/header-align', 'title': 'Engineer', 'icon_text': [{'icon': 666, 'text': 'Stockholm'}]},
                      {'name': 'acf/long-content', 'content': AD}])}
        jobs, audits = self.run_source('dokumentera')
        self.assertFalse(jobs)
        self.assertEqual(audits[0]['discovered'], 1)
        self.assertEqual(audits[0]['location_filtered'], 1)

    def test_blocked_source_never_masquerades_as_empty_or_calls_network(self):
        jobs, audits = self.run_source('alligator-bioscience')
        self.assertEqual(audits[0]['status'], 'blocked')
        self.assertEqual(audits[0]['requests'], 0)
        self.assertIn('certificate', audits[0]['errors'][0]['message'])
        e.write_results(jobs, audits, self.root)
        self.assertFalse(e.load_seen_job_ids(self.root))

    def test_clinch_double_encoding_and_dates(self):
        self.assertEqual(e.plain_text(e.description_fragment(r'\u003cp\u003ePython\u003c/p\u003e')), 'Python')
        self.assertEqual(e.long_date('31 oktober 2026'), '2026-10-31')
        self.assertEqual(e.long_date('31 October 2026'), '2026-10-31')
        with self.assertRaises(e.SourceError):
            e.long_date('next month')


    def test_dassault_index_pagination_and_full_detail_not_summary(self):
        source = self.sources['dassault-systemes']
        def metas(d):
            return {'metas': [{'name': k, 'value': v} for k, v in d.items()]}
        def respond(url, data):
            if '/GetCareerCardDetailV2/' in url:
                return {'hits': [metas({'external_description': AD, 'external_qualification': '<p>Data integration skills.</p>',
                                       'location': 'Europe, Sweden, Lund', 'posting_title': 'Engineer'})]}
            offset = int(parse_qs(urlsplit(url).query)['b'][0])
            return {'nhits': 2, 'hits': [metas({'content_type': 'career', 'card_id': str(offset + 1),
                'content_cta_1_url': 'https://www.3ds.com/careers/jobs/engineer-' + str(offset + 1),
                'content_title': 'Engineer', 'content_info_2_value': 'Lund' if offset == 0 else 'Paris',
                'content_summary': 'A truncated teaser.'})]}
        Client.responses = staticmethod(respond)
        jobs, audits = self.run_source('dassault-systemes')
        self.assertEqual(len(jobs), 1)
        self.assertIn('Data integration skills', jobs[0]['description'])
        self.assertNotIn('truncated teaser', jobs[0]['description'])
        self.assertEqual(audits[0]['discovered'], 2)
        self.assertEqual(audits[0]['location_filtered'], 1)
        self.assertEqual(audits[0]['status'], 'ok')

    def test_combitech_cards_use_their_own_city_and_count(self):
        source = self.sources['combitech']
        a = source['url'] + 'engineer-job_posting-3-123'
        b = source['url'] + 'test-job_posting-3-124'
        Client.responses = {source['url']: '<div id="job-list"><h3>2 tillgängliga tjänster</h3>' +
            '<div><h3>Engineer</h3><a href="' + a + '">More</a><h5>Lund</h5><h5>Sista ansökningsdag: 31 oktober 2099</h5></div>' +
            '<div><h3>Test</h3><a href="' + b + '">More</a><h5>Malmö</h5><h5>Sista ansökningsdag: 31 oktober 2099</h5></div></div>',
            a: '<h1>Engineer</h1><div id="workday-content-container">' + AD + '</div>'}
        jobs, audits = self.run_source('combitech')
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['location'], 'Lund')
        self.assertEqual(audits[0]['location_filtered'], 1)
        Client.responses[source['url']] = Client.responses[source['url']].replace('2 tillgängliga', '3 tillgängliga')
        self.assertEqual(self.run_source('combitech')[1][0]['status'], 'failed')

    def test_kraftringen_uses_facts_not_footer(self):
        source = self.sources['kraftringen']
        raw = e.RawJob('123', source['url'] + 'ledigt-jobb?jobid=123')
        Client.responses = {raw.url: '<h1>Engineer</h1><div class="fact-box"><p>Stad: Malmö<br>Sista ansökningsdag: 31 oktober 2099</p></div>' +
            '<div data-editorial-content>' + AD + '</div><footer>Headquarters Lund</footer>'}
        result = e.complete_detail(raw, source, Client([]))
        self.assertEqual(result.location, 'Malmö')
        self.assertEqual(result.valid_through, '2099-10-31')
        self.assertNotIn('Headquarters', result.description)

    def test_labelled_location_in_full_ad_not_generic_headquarters_mention(self):
        source = self.sources['cellavision']
        url = source['url'] + '/123-engineer'
        for body, expected in [(AD + '<h2>Location</h2><p>Lund, Sweden</p>', 1),
                               (AD + '<p>Our headquarters is in Lund, Sweden.</p>', 0)]:
            Client.responses = {source['url']: '', source['feed_url']: rss([item(url, '', body)]), url: '<html></html>'}
            jobs, audits = self.run_source('cellavision')
            self.assertEqual(len(jobs), expected)
            if expected:
                self.assertEqual(jobs[0]['location_extraction_method'], 'labelled_ad_location')
            else:
                self.assertEqual(audits[0]['location_unknown'], 1)

    def test_workday_country_fallback_still_requires_lund_in_detail(self):
        source = self.sources['sony']
        def respond(url, data):
            if data is not None:
                return {'facets': [{'facetParameter': 'locationCountry', 'values': [{'id': 'se', 'descriptor': 'Sweden'}]}],
                        'total': 1, 'jobPostings': [{'externalPath': '/job/Malmo/Engineer_123', 'title': 'Engineer', 'locationsText': 'Malmö'}]}
            return {'jobPostingInfo': {'jobReqId': '123', 'title': 'Engineer', 'location': 'Malmö', 'jobDescription': AD}}
        Client.responses = staticmethod(respond)
        jobs, audits = self.run_source('sony')
        self.assertFalse(jobs)
        self.assertEqual(audits[0]['location_filtered'], 1)
        self.assertEqual(Client.calls[1][1]['appliedFacets'], {'locationCountry': ['se']})
        self.assertEqual(Client.calls[1][1]['searchText'], '')

    def test_configuration_rejects_missing_api_before_network(self):
        config = {'version': 1, 'sources': [dict(self.sources['ericsson'])]}
        config['sources'][0].pop('api_url')
        path = self.root / 'config.json'
        path.write_text(json.dumps(config))
        with self.assertRaises(e.SourceError):
            e.load_sources(path)


    def test_bamboohr_uses_public_full_detail_json(self):
        raw = e.RawJob('12', 'https://probi.bamboohr.com/careers/12', location='Lund', method='bamboohr-json')
        Client.responses = {raw.url + '/detail': {'result': {'jobOpening': {
            'jobOpeningName': 'Engineer', 'description': AD, 'jobOpeningStatus': 'Open',
            'location': {'city': 'Lund'}}}}}
        result = e.complete_detail(raw, self.sources['probi'], Client([]))
        self.assertIn('Python', result.description)
        self.assertEqual(result.location, 'Lund')
        self.assertTrue(result.active)
        Client.responses[raw.url + '/detail']['result']['jobOpening']['jobOpeningStatus'] = 'Closed'
        self.assertFalse(e.complete_detail(raw, self.sources['probi'], Client([])).active)

    def test_afry_scoped_full_description_avoids_navigation(self):
        raw = e.RawJob('12', 'https://afry.com/en/careers/available-jobs/engineer', location='Lund')
        Client.responses = {raw.url: '<nav>Other careers and news</nav><h1>Engineer</h1><div class="advert--body">' + AD + '</div><footer>Contact us</footer>'}
        result = e.complete_detail(raw, self.sources['afry'], Client([]))
        self.assertIn('Python', result.description)
        self.assertNotIn('Other careers', result.description)
        self.assertNotIn('Contact us', result.description)
        self.assertEqual(result.location, 'Lund')


if __name__ == '__main__':
    unittest.main()
