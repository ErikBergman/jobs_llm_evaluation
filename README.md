# jobs_llm_evaluation

Scrape public LinkedIn job ads, store each scrape under `results/discard/<timestamp>/`, then run a mock matcher that separates likely matches into `results/hits/<timestamp>/`.

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

## OpenAI matcher PoC

The matcher also has an OpenAI PoC mode using `gpt-5-nano`. It is intentionally not the default, because every API call costs money.

For this first PoC, the prompt only checks whether the job title starts with a vowel. If yes, the job is a hit.

Local usage:

```bash
OPENAI_API_KEY=... python3 match_jobs.py --latest --matcher openai
```

GitHub Actions usage:

- add repository secret `OPENAI_API_KEY`
- add repository variable `JOB_MATCHER_MODE=openai`
- optionally add repository variable `OPENAI_MODEL=gpt-5-nano`

If `JOB_MATCHER_MODE` is unset, the workflow keeps using the free mock matcher.
