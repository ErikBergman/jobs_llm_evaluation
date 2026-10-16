# Lund employer source catalogue

`job_listing_pages_lund.json` contains 48 employer career sources: the 27 supplied on 2026-10-09 and 21 researched additions reviewed on 2026-10-15 (Europe/Stockholm).

This is a research catalogue for adding employer sources. Employer indexing for all 48 sources is now configured in `employer_jobs.py` (45 operational or monitored, three explicitly blocked); see [employer_indexing.md](employer_indexing.md) for strategies, live results and use alongside LinkedIn. The scheduled workflow still runs LinkedIn only. The catalogue entries describe career sources, not individual job ads or confirmed job matches.

## Selection and location

The additions target employers with evidence of Lund operations and possible overlap with Python/backend integration, clinical/laboratory systems, process engineering, technical information and test automation. `focus` values are our assessment of useful search areas based on the candidate profile and employer activity; they are not promises of vacancies, employment interest or qualification fit. No CV text or private profile data is included.

Preserve employer discovery broadly and filter individual advertisements by workplace. Global and regional boards also include jobs outside Lund. The supplied Sydvatten, VA SYD and Dokumentera entries are retained with explicit location cautions; the reviewed Dokumentera page did not establish Lund presence. Open applications, student theses and research positions requiring further degrees must be assessed separately from ordinary vacancies.

## JSON fields

| Field | Meaning |
| --- | --- |
| `employer` | Employer or recruiting business unit; one entry per source. |
| `url` | Career/listing entry point. Prefer a direct stable board or location page when verified. |
| `checked` | Date this entry was reviewed, including an unsuccessful access attempt. Read `verification` and `note` before using the URL. |
| `verification` | Access/content result for `url` at review time, as defined below. |
| `focus` | Prospective search areas: `software-integration`, `clinical-systems`, `process-engineering`, `technical-information`, `test-automation`, `life-science`. |
| `note` | Location instructions, inferred relevance, empty-board observations and access limitations. |
| `evidence_urls` | Optional supporting employer pages, employer-authored posts or employer-owned recruitment ads. These may establish location, source ownership or a board redirect; historical ads are not current vacancies. |
| `fallback_url` | Optional readable corporate careers entry point when the supplied filtered URL could not be read. |
| `original_url` | Supplied URL retained as provenance when a more useful verified entry point replaced it. |

## Verification results

| Value | Count | Meaning |
| --- | ---: | --- |
| `verified` | 36 | Meaningful employer career or listing content was readable. This may be a careers landing page or an explicitly empty board; it does not certify every linked listing or automated scraping. |
| `browser_required` | 6 | Employer page/search identity was confirmed, but the text view exposed no vacancy records. Browser or API investigation is needed. |
| `access_limited` | 5 | This URL could not be fetched during review. Retained seed URLs or supporting employer evidence are noted; the error is not proof the site is down. |
| `linked_only` | 1 | The employer's corporate page links the recruitment destination, but the destination could not be read. |

ESS now uses its directly linked `/careers/vacancies` page. VA SYD now uses its readable Varbi board. Etteplan now points to Open Positions rather than a generic application page. The Tetra Pak and Alfa Laval entries retain the supplied Lund searches and add readable official fallback pages. Intui uses the current destination of the former IntuiCell recruitment domain.

The `verified` catalogue flag alone does not enable ingestion. All 48 indexing configurations are in `employer_scraping.json`; individual strategies and current indexing health are in `employer_indexing_strategies.json`. The catalogue verification counts above describe the earlier research pass; current live indexing results are in `employer_indexing_validation.json`. Any further adapter must extract job IDs, canonical ad URLs, full descriptions and workplace metadata, handle pagination and empty/error responses, and feed the app's existing prefilter/matching flow. Do not pass employer URLs to the LinkedIn-specific search configuration.

## Follow-up research

Modity is relevant to integration and applied AI, but this pass did not verify an employer-owned career/listing URL. Its corporate site returned no readable text, and search results mainly led to recruiter-specific ads. No guessed Modity career path was added. AcouSort, SenzaGen and Immunovia also remain research candidates; a verified stable recruitment entry point is still needed.

## Local validation

Parse the catalogue with `python3 -m json.tool job_listing_pages_lund.json`. Before committing updates, check unique employer names and primary URLs, ISO dates, HTTPS URLs, verification values, and nonempty focus/notes. This data-only addition does not require executing LinkedIn scraping, OpenAI evaluation or notifications.
