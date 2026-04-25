# jobs_llm_evaluation

Scrape public LinkedIn job ads, store each scrape under `results/discard/<timestamp>/`, then run a matcher that separates likely matches into `results/hits/<timestamp>/`.

## Scrape jobs

Configure the LinkedIn search in `linkedin_search_input.json`, or copy from `linkedin_search_input.template.jsonc` for documented filter syntax.

```bash
python3 linkedin_guest_jobs.py --limit 2
```

The scraper writes a timestamped JSON file under `results/discard/`.

## Classify latest scrape

The current matcher is a mock AI model. It marks a job as a hit only when the standalone word `engineer` appears in the job description, case-insensitively.

```bash
python3 match_jobs.py --latest
```

Inspect matching jobs in:

```text
results/hits/<timestamp>/
```

Rejected jobs are written back under:

```text
results/discard/<timestamp>/
```

## OpenAI profile matcher

The matcher also has an OpenAI mode. It is intentionally not the default, because every API call costs money.

OpenAI mode asks the model whether the job is a rare "unicorn" match for the candidate profile. It should only put a small fraction of unusually strong matches in `hits`; generic keyword overlap should stay in `discard`. It also asks the model for a short reason. This is a model-provided rationale, not hidden chain-of-thought.

The OpenAI matcher runs in two stages:

1. One batch triage call reads the profile once and scans all newly scraped ads.
2. Only ads selected for the temporary candidate list get a second, stateless confirmation call.
3. Confirmed jobs go to `results/hits/<timestamp>/`; everything else remains in `results/discard/<timestamp>/`.

Local usage:

```bash
OPENAI_API_KEY=... python3 match_jobs.py --latest --matcher openai --job-profile job_profile.txt
```

The profile file may be plain text or RTF. Local profile files named `job_profile.txt` or `job_profile.rtf` are ignored by git.

GitHub Actions usage:

- add repository secret `OPENAI_API_KEY`
- add repository variable `JOB_MATCHER_MODE=openai`
- optionally add repository variable `OPENAI_MODEL=gpt-5.4-mini`
- keep `job_profile.rtf` in the configured Koofr memory folder

If `JOB_MATCHER_MODE` is unset, the workflow keeps using the free mock matcher.

Each matching run writes an audit file next to the discard output:

```text
results/discard/<timestamp>/linkedin_jobs_sample_match_metadata.json
```

The workflow log also prints one `[decision]` line per job with the job ID, hit value, title, and reason.
