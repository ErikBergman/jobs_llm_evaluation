# Cross-source job deduplication

Employer ads are now compared with recent LinkedIn ads before candidate-profile evaluation. The existing shared result files are the database: no database service or new package dependency is required. The scheduled workflow imports both LinkedIn and company career pages, then runs the shared waiting-room check once before classification on evaluation/report runs. Standalone employer imports also check new ads immediately by default; the workflow disables that import-time check to avoid a second comparison budget on scrape-only runs.

The check works in either arrival order. It keeps the older collected ad and archives a newer copy only after an explicit, valid LLM duplicate verdict. It never decides duplication from employer/title similarity alone. Separate requisitions, different roles and uncertain evidence are retained. The model receives public ad excerpts, not the private candidate profile.

## Decision path

1. Load full saved ads from `waiting_room`, `hits` and `discard`, consolidating repeated copies by job ID and the earliest known collection time. Ignore diagnostics and previously discarded duplicates.
2. Normalise employer names, accents and legal suffixes. Use explicit aliases for variations such as Axis / Axis Communications; avoid automatic substring or subsidiary matching.
3. Find ads from the opposite source collected in the seven days preceding the new ad's first collection. Use title similarity to rank plausible candidates and retain at most three. Clearly unrelated titles make no LLM request. Translated titles with little lexical overlap can be missed; this is a deliberate local shortlist/cost limitation.
4. Check the persistent decision cache. Otherwise make one small-model request containing the new ad and the shortlist together.
5. Accept only `duplicate`, `different` or `uncertain`. A duplicate must identify exactly one of the supplied older candidate IDs. Missing output, refusal, incomplete/invalid JSON, unknown IDs, API errors, missing credentials or a depleted budget retain the ad.
6. Save full confirmed duplicates before removing them from the waiting room. Keep the original ad untouched. Archive IDs remain visible to the existing seen-ID loader so the same copy will not be repeatedly scraped.

An API failure stops further network comparisons for that invocation. Cached decisions can still be used. Failures are recorded without logging API credentials, raw request/error bodies or candidate-profile text.

## Collection dates

`first_seen_at` is persisted when the duplicate stage first processes an ad. Existing employer `indexed_at` fields and older collection fields are supported. Legacy records use their original `YYYYMMDD_HHMMSS[_microseconds]` run directory as a UTC collection timestamp, matching GitHub's runner. The oldest copy wins across raw, queued and classified files. Posting dates and filesystem modification times are never used for recency.

Historical ads without a trustworthy collection date, or with a future collection date, are excluded from comparison and listed in `history_issues`. A newly processed queued ad gets a collection timestamp; this does not make undated historical ads eligible. Legacy local folders created in another timezone can differ by that timezone's offset; explicit timestamps are preferred.

## Cost controls and configuration

[job_deduplication.json](job_deduplication.json) contains the defaults:

| Setting | Default | Effect |
| --- | --- | --- |
| `lookback_days` | 7 | Bound the history window relative to the new ad's first collection. |
| `max_candidates` | 3 | Compare only the strongest title candidates from the same employer and opposite source. |
| `max_calls` | 20 | Maximum attempted comparisons per invocation; excess ads are retained. |
| `excerpt_chars` | 1600 | Bound each ad's description excerpt, including a requirements/end excerpt for long descriptions. Title, employer, location, URL and requisition fields are also bounded. |
| `max_output_tokens` | 512 | Hard request output limit, including reasoning. No automatic retries or larger-budget retries. |
| `model` | `gpt-6-luna` | Uses the app's existing small model family, independently configurable. |
| `employer_aliases` | Selected verified name variations | Conservative matching without assuming all similarly named employers are identical. |

No recent employer candidate means no API call. All shortlisted older ads are compared in one request rather than one call per pair. Cached positive, negative and uncertain decisions make no new calls; model, prompt and compared excerpt changes invalidate the cache. Cache entries expire after the lookback window plus a day and are capped at 10,000 entries.

Automatic shared-queue processing removes IDs already present in completed match results before considering any further LLM request. Each attempted duplicate-model call logs a `[dedup-request]` record ID, candidate IDs, and comparison fingerprint. The same fingerprint is saved in the per-ad audit for both fresh and cached decisions, allowing repeated identical requests to be distinguished from changed prompts without logging ad bodies or secrets.

With defaults, one comparison carries at most four 1,600-character ad excerpts plus bounded metadata and instructions. Actual billed input/output token counts are saved from API responses, including incomplete responses. The output ceiling is 10,240 tokens for 20 calls; input tokens depend on the text and API schema. This is a bounded workload, not a fixed monetary quote. `max_calls: 0` uses only cached decisions; `--mode off` disables the stage entirely.

The model uses [OpenAI Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs) with a strict JSON schema, no tools and `store: false`. There are no live API calls in the test suite; provider-specific duplicate judgement accuracy still needs observation on real paired ads.

## Running it

Set `OPENAI_API_KEY` through the existing secret/environment mechanism. No API key or candidate profile is stored in configuration.

Employer imports check duplicates automatically:

```bash
python3 employer_jobs.py --results-root runtime_results
```

Check the shared waiting room before classification, including LinkedIn copies collected after employer ads:

```bash
python3 deduplicate_jobs.py --results-root runtime_results
python3 match_jobs.py --waiting-room --output-root runtime_results --matcher openai --job-profile runtime_results/job_profile.rtf
```

Inspect candidate counts without API calls, cache updates or queue changes:

```bash
python3 deduplicate_jobs.py --results-root runtime_results --dry-run
```

Disable duplicate checking:

```bash
python3 employer_jobs.py --results-root runtime_results --deduplication-mode off
python3 deduplicate_jobs.py --results-root runtime_results --mode off
```

Override configuration with `--deduplication-config` on the employer importer or `--config` on the shared command. Model overrides are `--deduplication-model` and `--model`, respectively. In GitHub Actions, `JOB_DEDUPLICATION_MODE` and `JOB_DEDUPLICATION_MODEL` repository variables control the shared stage; it uses the existing OpenAI secret and result-memory sync. It runs on evaluation/report invocations, immediately before profile classification. Workflow changes are local to this branch until it is merged/deployed.

## Persistence and audits

- `duplicates/<timestamp>/duplicate_jobs.json`: full newer ads with `deduplication.duplicate_of` pointing to the retained original and a brief reason. They can be inspected/recovered and are never sent to the profile matcher.
- `deduplication/decisions.json`: atomic, bounded decision cache containing prompt fingerprints and decisions. It does not contain ad bodies.
- `deduplication/<timestamp>/deduplication_metadata.json`: per-run decisions, candidate IDs, call/cache counts, input/output usage, retained-on-error/budget counts and historical-date issues.
- Employer imports include the same audit in `employer_jobs_metadata.json`, along with before/after source counts.

Diagnostics and cached decisions deliberately use `record_id` rather than `job_id`: failed comparisons cannot accidentally enter the existing recursive seen-ID memory. Only saved ads, including full confirmed duplicate archives, contribute seen IDs. Queue files and cache writes use atomic replacement. If a queue rewrite is interrupted, its saved duplicate archive is authoritative on retry, so a confirmed discarded copy cannot accidentally be kept after a partial failure.

The original seven-day, title-shortlist and budget limits can leave duplicates unresolved. The importer retains these ads for normal evaluation. It does not modify already classified/previously notified older records or reevaluate edited ads under the app's existing seen-once policy.

## Verification

```bash
python3 -m unittest tests.test_job_deduplication tests.test_deduplication_llm tests.test_deduplication_integration
python3 test_all.py
```

The tests cover date boundaries, legal suffixes and aliases, both arrival orders, same-batch copies, distinct/uncertain results, malformed/refused/incomplete outputs, API failures, missing credentials, cache reuse and invalidation, budgets, immutable first-seen timestamps, poisoned metadata prevention, duplicate archive recovery after interrupted writes, employer CLI integration, and a one-copy handoff to the existing matcher. All LLM responses are simulated; no paid model calls or notifications are needed for validation.
