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
