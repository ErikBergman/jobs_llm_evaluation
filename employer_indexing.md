# Employer indexing pilot

On 2026-10-16 the five-source pilot indexed 116 advertisements with an explicit Lund workplace. A second pass against the same memory indexed zero new ads and recognised all 116 previously stored ads. These are discovered advertisements, not 116 profile matches. The local prefilter passed 102 and rejected 14; no OpenAI calls or Telegram messages were made during validation. The complete 116-ad batch also passed through the existing free mock classifier successfully. The source snapshot includes student/research opportunities and two Tetra Pak expressions of interest; these are retained as advertisements for the existing matching stage to assess, rather than presented as confirmed suitable vacancies.

[employer_indexing_strategies.json](employer_indexing_strategies.json) gives an individual strategy for every one of the 48 catalogue entries. Five are `implemented-pilot`; the other 43 are `planned-unverified`. Provider guesses and future discovery steps are not active configuration. [employer_scraping.json](employer_scraping.json) contains only the five tested sources.

## The five strategies and observed results

| Employer | Recruitment platform | Discovery and detail strategy | Ads discovered | Lund ads indexed |
| --- | --- | --- | ---: | ---: |
| Axis | Workday | Discover the Lund location facet in public CXS JSON, enumerate offset pages with no keyword restriction, then fetch each job's detail JSON. | 29 | 29 |
| CellaVision | Teamtailor | RSS contains the native ad. Also follow delegated recruiter links on the official board and read their JobPosting JSON-LD. | 3 | 3 |
| Lund University / MAX IV | Varbi | Full-description RSS plus a join to the listing's workplace/deadline table using native job ID, independent of URL language. | 85 | 79 |
| Tetra Pak | SuccessFactors | `sitemap.xml` is an RSS job feed, including full descriptions, Google-namespace location and expiry fields. It is not a conventional URL-only sitemap. | 245 | 5 |
| ESS | ReachMee | Read the public bootstrap script as data, resolve its iframe board, parse job rows and scoped detail HTML. | 1 | 0 |

ESS's sole listed ad was in Denmark. Its description mentions the Lund facility, which must not cause the ad to pass a Lund workplace filter. Six university ads and 240 Tetra Pak ads were outside Lund. No pilot source hit the 200-ad validation limit or its pagination cap. These are observations of the provider responses at the recorded validation time, not a guarantee of every advertisement across every recruitment channel.

The full discovery pass used 42 public HTTP requests. The machine-readable run summaries are in [employer_indexing_validation.json](employer_indexing_validation.json). Full public advertisements used for live validation remain in temporary local results, rather than in the repository.

## What has been streamlined

Five different recruitment platforms needed three discovery adapters: RSS, Workday JSON and ReachMee HTML. Three of the five employers share the feed reader. A small Varbi metadata join and CellaVision's delegated-link discovery handle differences the feeds alone do not cover. The ReachMee adapter uses narrowly scoped job markup; it does not scrape a whole corporate page as an advertisement.

All adapters share HTTP handling, source isolation, allowed-host redirects, date and workplace checks, description cleanup, stable job identity, result-memory checks, requirements extraction, prefiltering and output format. Of the 116 ads, 85 descriptions came from RSS, 29 from Workday JSON and two from delegated JobPosting JSON-LD. The existing requirements extractor found requirement sections in 94 ads and retained the full description for the remaining 22.

This is a useful reduction in per-employer work. It is not a universal HTML scraper. The next employers on a proven platform should mostly need verified configuration and fixtures; an unfamiliar ATS still needs inspection of its listing, detail, pagination and empty-state behaviour. The remaining strategy file describes those investigations individually.

## Using it alongside LinkedIn

Use Python 3.12, matching the existing GitHub Actions runtime. The employer importer uses the standard library and the existing local parser/prefilter; it adds no package dependencies.

Run the scrapers against the same result root and waiting room:

```bash
python3 linkedin_guest_jobs.py --results-root runtime_results --results-bucket waiting_room --card-limit 100 --detail-limit 100 --max-pages 10
python3 employer_jobs.py --results-root runtime_results --limit 25
python3 match_jobs.py --waiting-room --output-root runtime_results --matcher mock
```

The final command demonstrates the existing free mock matcher. Use the existing OpenAI matcher configuration when you intend to evaluate against the private candidate profile. An offline integration test mixes employer and LinkedIn records in the same waiting room and runs the existing classifier without changing `match_jobs.py`.

The scheduled workflow has not been changed or deployed in this pilot. To schedule these sources, run the employer command after scraping LinkedIn and before the existing waiting-room classification, using the same runtime result root and `job_matching.json` path. Source failures return nonzero while preserving successfully extracted ads and their diagnostics; orchestration should report that source failure while still allowing healthy inputs to be classified.

Select sources individually with repeated `--source` flags, for example:

```bash
python3 employer_jobs.py --source axis --source cellavision --results-root runtime_results --limit 10
```

The default is a maximum of 25 new ads per source per invocation, with a one-second request interval. The audit's `selection_truncated` flag indicates that more candidates remained. Later invocations skip stored records and can continue through the current source snapshot. A large first-time import should use an explicit higher limit, as in the bounded 200-per-source validation. Feeds are provider snapshots: increasing this cap does not recover ads a provider has already removed.

Use `--matching-config` as with the LinkedIn scraper. If omitted, a local `job_matching.json` is picked up when present. Existing keyword and metadata-based location exclusions remain in effect after the employer importer verifies a Lund workplace.

## Normalised records and indexing rules

The importer writes `waiting_room/<timestamp>/employer_jobs.json`, an array compatible with the existing matcher. Records carry `job_id`, `title`, `company`, `location`, `url`, full `description`, requirement text and prefilter fields, plus source provenance and extraction method. IDs are namespaced as `employer:<source-id>:<native-id>` so an employer requisition cannot collide with a numeric LinkedIn ID.

Repeat indexing suppresses records with the same stable ID or exact canonical ad URL anywhere in result memory, including queued and classified files. Only tracking parameters and fragments are removed; identity-bearing query parameters such as ReachMee's `job_id` remain. Error diagnostics contain no `job_id` fields, so the existing recursive seen-ID loader cannot accidentally mark failed ads as indexed.

Workplace comes from provider fields, feed location metadata or the Varbi workplace table. For ESS country-only metadata, a narrowly matched explicit sentence such as “The position is based in Lund” can supply the city; the record identifies that extraction method. A general description mentioning Lund is insufficient. Ambiguous locations are withheld and listed in `location_review` for investigation, not silently marked as seen. Expired ads and Workday ads that cannot be applied to are excluded where that metadata is available.

This follows the app's existing seen-once memory policy: edits to an already stored advertisement are not automatically re-evaluated. A future refresh policy should compare provider modification dates or content hashes.

Cross-posting on LinkedIn and an employer site can still produce two records when their URLs and IDs differ. This pilot deliberately does not collapse jobs solely by title and company: separate requisitions can share both. A later improvement should extract employer application URLs/requisition IDs from LinkedIn and merge only strong identity matches, preserving both source labels.

## Failures, pagination and maintenance

Each source has its own audit in `employer_jobs_metadata.json`. A malformed feed, HTTP error, missing expected container, incomplete pagination, repeated Workday page or schema change is a failed/partial source, not a healthy empty board. Valid empty feeds remain healthy. Successfully parsed ads from other sources are preserved. This metadata filename is excluded by the existing waiting-room job loader.

The HTTP client limits response size and request rate, follows only configured public HTTPS hosts, retains ordinary session cookies and retries temporary server/rate-limit responses. It honours short `Retry-After` values and defers rather than immediately retrying a server-requested wait over 30 seconds. It does not execute downloaded scripts or submit application/contact forms. External recruitment hosts must be added explicitly after verification.

Workday pagination checks its advertised total and rejects missing/repeated pages. RSS follows declared next links with loop/cap detection. The inspected ReachMee board renders its current set as a complete table; another ReachMee customer must be checked for different pagination before reuse. Unusual RSS truncation or a provider-specific alternative feed also needs per-employer validation; a well-formed feed alone does not prove all recruitment channels are covered.

For the next batch, prioritise the other identified Workday, Varbi and Teamtailor sources. Confirm their endpoints, location metadata and any external recruiter cards before adding configuration. For every other catalogue entry, follow its recorded discovery strategy and try public feeds or structured detail data before introducing another HTML adapter.

## Verification

```bash
python3 -m unittest tests.test_employer_jobs
python3 test_all.py
```

All 129 tests passed on Python 3.12.14, including the 17 added offline tests. These new tests use synthetic examples preserving structures observed on the live providers. They cover all five source configurations, full-description/requirements extraction, delegated ads, workplace-versus-company mentions, expired ads, stable IDs and canonical URLs, repeated indexing, mixed LinkedIn/employer classification, pagination failure, source isolation, allowed hosts and rate-limit handling.

The Mac's default Python 3.9 can run the new tests and live importer, but three pre-existing LinkedIn HTTP-error fixture tests fail there. Use the project's Python 3.12 runtime for the full suite; the existing tests were left unchanged.
