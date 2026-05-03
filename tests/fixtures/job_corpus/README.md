# Job Corpus Fixtures

These fixtures are offline regression inputs for parser and matching behavior.
They are not used by the production scraper or matcher.

Each JSON file contains a small anonymized LinkedIn-detail-style job ad and
expected deterministic outputs:

- `requirements_text`
- `requirements_extraction_method`
- local prefilter pass/reject metadata
- mock matcher route

The corpus should cover job-ad shapes that are easy to break when tuning
heading patterns, stop headings, or prefilter keywords. Keep fixtures short,
anonymized, and free from live LinkedIn IDs or URLs.
