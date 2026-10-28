# Employer job indexing

All 48 employers in the Lund catalogue now have an indexing configuration and an individual strategy. Live validation on 2026-10-16 indexed **261 ads with an explicit Lund workplace**, using 197 public HTTP requests. These are advertisements for the existing matcher to assess, not confirmed profile matches. The prefilter passed 236 and rejected 25. No OpenAI evaluation or notifications were used during validation.

The run completed cleanly for 44 sources. Schneider Electric produced 12 Lund ads and withheld three country-wide ads whose metadata specifies only Sweden. Region Skåne, RISE and Alligator are explicitly blocked. A successful rerun recognised the stored ads without adding duplicates; see [employer_indexing_validation.json](employer_indexing_validation.json) for exact repeat counts and current source diagnostics.

- [employer_scraping.json](employer_scraping.json): all 48 configured sources, including explicit blocked-source reasons.
- [employer_indexing_strategies.json](employer_indexing_strategies.json): discovery, full-description, workplace and pagination strategy for every employer.
- [employer_jobs.py](employer_jobs.py): standard-library importer into the existing LinkedIn matcher's waiting room.

## Shared adapters and verified differences

| Adapter | Employers | Strategy |
| --- | ---: | --- |
| RSS | 17 | Full descriptions, stable provider IDs and location namespaces. Varbi joins workplace/deadline table cells by native ID; CellaVision also follows delegated recruiters. |
| Workday CXS | 3 | Resolve Lund facets and page JSON. Sony currently has only a Malmö role: its Sweden country facet is a discovery fallback, with detail cities still checked. |
| Declarative HTML listings | 11 | Scoped job links and JSON-LD, with explicit empty markers and bounded pagination. Includes Clinch, TalentBrew, Lime's mixed providers and three small careers-page monitors. |
| Workspace Recruit | 2 | Parse literal portal recruitment arrays, then scoped public advertisement HTML. Camurus and Axiell share this adapter. |
| Eightfold | 2 | Ericsson and Qlik public search JSON, followed by full JobPosting JSON-LD. Verify actual cities because search results can include nearby/remote roles. |
| Other structured/platform adapters | 10 | ReachMee, Dassault, Etteplan, AFRY, Combitech, Dokumentera, Hailey, BambooHR, Sigma Profiler and Jibe. |
| Blocked | 3 | Retain actionable diagnostics; do not report a successful zero-ad result. |

There are 15 active discovery adapter types, rather than 48 unrelated scrapers. Feed and Workday configuration alone cover 20 employers. All adapters share HTTP handling, source isolation, allowed hosts, dates, workplace checks, canonical URLs, persistent deduplication, description cleanup, requirements extraction, prefiltering and output schema.

The additional platforms exposed several important differences:

- **Modelon / Hailey:** the full Markdown is a UTF-8 byte-length-delimited Next Flight text record, sometimes split across several script calls. Reassemble the data stream; do not execute scripts or rely on visible text alone.
- **Etteplan:** deduplicate repeated positions in `__NEXT_DATA__`, retain all listed workplaces and deadlines, then fetch full Jobylon ads.
- **Lime:** the corporate board mixes Jobylon and Personio. Some Personio details lack JobPosting JSON-LD, so a scoped title, description and location-component fallback is necessary.
- **Schneider / Jibe:** retain primary and additional cities; combine full description, responsibilities and qualifications. Paginate against `totalCount`, including non-default-language records, rather than the default-language `count`.
- **AFRY:** the official job cache supplies full discovery metadata; details use the scoped `advert--body` HTML. Skip expired metadata before fetching a stale detail. A representative active non-Lund detail was also checked.
- **Probi / BambooHR:** `/careers/<id>` returns a JavaScript shell; `/careers/<id>/detail` supplies the full public JSON. A Redmond ad was checked because no Lund ad was listed.
- **Dassault:** enumerate English career cards to avoid per-language duplicates; use its detail API for full description/qualifications. Card summaries and the page's short JSON-LD description are insufficient. No Lund jobs appeared in the complete 694-card snapshot; a representative non-Lund full detail was checked separately.
- **Dokumentera:** resolve the Next page's job IDs through public WordPress REST, then read scoped Next detail blocks. One stale unpublished/deleted ID is absent from public REST. All five published positions specify Stockholm.
- **Kraftringen and Combitech:** extract each ad's own city/deadline fields; corporate addresses and navigation are excluded from the job description.
- **OptiCept:** the linked Field Service Engineer ad explicitly expired on 2024-06-16. It must not enter the waiting room merely because the link remains live.

This substantially reduces per-employer work, but unfamiliar providers still require inspecting listing, detail, pagination, locations and empty/error states. The HTML adapter is configurable, not a claim that arbitrary HTML can be scraped reliably without verification.

## Empty boards, monitors and access limitations

BioInvent, Vantive, Qlik, Svep and Intui returned explicit empty feeds or listing states. Other employers returned jobs outside Lund; these are counted separately from empty boards. FlatFrog and Qlucore are monitored through explicit no-vacancies messages. InCoax currently offers a standing application form; it is monitored for discrete ads rather than emitted as a current vacancy. Unsupported future markup triggers a source failure for investigation.

| Source | Current limitation | Next action |
| --- | --- | --- |
| Region Skåne | Both supplied and alternate official listings return HTTP 403. | Verify permitted browser access or an employer-published feed/API before implementing the remaining listing parser. |
| RISE | Official Swedish page returns a bot challenge; English page returns HTTP 403. The RISE Varbi portal exposes only thesis expressions of interest and cannot establish complete vacancy coverage. | Recover access to the complete official vacancy list; do not substitute the limited portal silently. |
| Alligator Bioscience | Official career portal and `/jobs` fail TLS certificate validation; the employer contact page still links there. | Recheck when the portal certificate is repaired or an official replacement is published. |
| Schneider Electric | Three ads have Sweden-only metadata. | Keep them in `location_review`; ingest only after an explicit Lund workplace is available. |

These conditions are recorded in configuration, strategies and validation. No certificate checks or bot challenges are bypassed. Blocked entries deliberately make an all-source CLI run return nonzero, while preserving healthy jobs. Remove the blockage only after the source has been reverified; it is not a transient empty state.

## Running alongside LinkedIn

Use the project's Python 3.12 runtime. No new package dependencies are required. Run both importers against the same result root:

```bash
python3 linkedin_guest_jobs.py --results-root runtime_results --results-bucket waiting_room --card-limit 100 --detail-limit 100 --max-pages 10
python3 employer_jobs.py --results-root runtime_results --limit 25
python3 deduplicate_jobs.py --results-root runtime_results
python3 match_jobs.py --waiting-room --output-root runtime_results --matcher mock
```

The last command demonstrates compatibility using the free mock matcher. The preceding duplicate stage can make bounded small-model calls when recent candidates exist; use `--mode off` there and `--deduplication-mode off` on employer imports for entirely offline/mock operation. Use the existing OpenAI matcher configuration for actual candidate-profile evaluation. Source failures return nonzero without discarding successful ads, so orchestration should retain the source failure and still allow healthy waiting-room inputs to be processed.

The scheduled workflow imports company career pages after LinkedIn on every scrape run, using the same Koofr-backed results root and matching configuration. Import-time duplicate checking is disabled there; the shared cross-source stage runs once before profile classification on evaluation/report runs. The `--allow-partial-sources` workflow option preserves healthy ads and emits a warning for blocked or partial sources. An outage affecting every employer source still fails the employer step. Both importers can contribute healthy ads even when the other fails; failures remain visible, and collected ads are synced back to Koofr.

Workflow limits can be overridden with repository variables `JOB_EMPLOYER_LIMIT` (25 new Lund ads per source), `JOB_EMPLOYER_MAX_PAGES` (20), `JOB_EMPLOYER_PAGE_SIZE` (20), `JOB_EMPLOYER_REQUEST_DELAY` (1 second), and `JOB_EMPLOYER_TIMEOUT` (30 seconds per request). Company scraping has a 30-minute step timeout. The ordinary CLI remains strict about partial sources unless `--allow-partial-sources` is explicitly supplied.

Select individual sources with repeated flags:

```bash
python3 employer_jobs.py --source axis --source camurus --source modelon --results-root runtime_results --limit 25
```

The default is 25 new Lund ads per source, 20 discovery pages, and a one-second request interval. `selection_truncated` flags remaining candidates when the per-source limit is reached; subsequent invocations skip stored records and continue through the current snapshot. The complete validation used `--limit 1000 --max-pages 50 --request-delay 1`, with no truncation. Feeds represent provider snapshots; larger caps cannot recover removed advertisements.

`--matching-config` uses the same matching/prefilter configuration as LinkedIn. A local `job_matching.json` is used automatically when present.

## Records, identity and failure handling

Output is an array in `waiting_room/<timestamp>/employer_jobs.json`, with the existing job fields plus source provenance and extraction method. IDs use `employer:<source-id>:<native-id>` to avoid collisions with numeric LinkedIn IDs. `employer_jobs_metadata.json` contains per-source counts, errors and location-review records; the waiting-room loader ignores that metadata filename.

Stored IDs and exact canonical URLs are checked across queued and classified result files. Tracking parameters and fragments are removed; identity-bearing parameters remain. Error/review records contain no `job_id`, so unsuccessful ads cannot poison the existing recursive seen-ID memory. Missing full descriptions, location ambiguity, changed schemas and incomplete pagination remain visible and eligible for retry.

Workplace must come from explicit provider metadata, verified table cells or a labelled Location / Lund, Sweden section within the full ad. ReachMee also accepts narrowly matched explicit workplace sentences. A general headquarters/facility mention does not qualify. Expired/inactive ads and HTML detail URLs returning HTTP 404/410 are excluded; other HTTP errors remain failures. Unknown expiry formats are errors rather than guessed dates.

The HTTP client limits response size and request rate, follows only configured HTTPS hosts, retains ordinary cookies, retries temporary failures and honours short `Retry-After` values. It does not execute downloaded scripts or submit application/contact forms. New external recruitment hosts require explicit configuration after verification.

Repeat indexing follows the app's existing seen-once policy: edited ads are not automatically reevaluated. A bounded small-model check now compares plausible recent cross-source ads and archives the newer copy only after an explicit duplicate verdict. Distinct requisitions, uncertain results and unavailable comparisons are retained. See [job_deduplication.md](job_deduplication.md) for the seven-day window, cost controls and audit format.

## Verification

```bash
python3 -m unittest tests.test_employer_jobs tests.test_employer_platforms
python3 test_all.py
```

All 156 tests pass under Python 3.12, including 27 additional platform tests and the original 17 employer tests. Synthetic fixtures retain observed provider structures without saving full public advertisements in the repository. Coverage includes UTF-8 Flight chunks, provider pagination/count failures, mixed language totals, full detail extraction, metadata-only expiry, empty/error distinctions, location exclusions, country fallback, blocked sources, stable IDs and mixed employer/LinkedIn waiting-room classification.

The complete live batch was also passed through the existing free mock classifier in a separate temporary result root. See the validation JSON for the processed count and repeat-run evidence. Full public advertisements remain in temporary local results. The repository contains configuration, strategies, tests and concise audits.
