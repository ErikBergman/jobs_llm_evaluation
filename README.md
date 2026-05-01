# jobs_llm_evaluation

Scrape public LinkedIn job ads, store each scrape under `results/discard/<timestamp>/`, then run a matcher that separates likely matches into `results/hits/<timestamp>/`.

## Scrape jobs

Configure the LinkedIn search in `linkedin_search_input.json`, or copy from `linkedin_search_input.template.jsonc` for documented filter syntax. The production config starts with a geo-only Lund search and keeps keyword searches as source labels; broad discovery should come from LinkedIn geography and recency, not LinkedIn keyword filtering.

```bash
python3 linkedin_guest_jobs.py --card-limit 100 --detail-limit 100 --max-pages 10
```

The scraper writes a timestamped JSON file under `results/discard/` by default, or under `results/waiting_room/` in the scheduled workflow. Each job includes local prefilter metadata (`prefilter_pass`, reason, positive matches, and negative matches). OpenAI evaluation skips jobs rejected by this local prefilter and writes them as discards without an API call.

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

The evaluation flow uses three user-facing stages:

1. Eval 1: Keywords removes obvious non-matches with a permissive local keyword screen.
2. Eval 2: LLM triage reads the profile once and scans all Eval 1-passing ads.
3. Eval 3: LLM confirmation performs a second, stateless review of jobs selected by triage.
4. Confirmed jobs go to `results/hits/<timestamp>/`; everything else remains in `results/discard/<timestamp>/`.

Local usage:

```bash
OPENAI_API_KEY=... python3 match_jobs.py --latest --matcher openai --job-profile job_profile.txt
```

The profile file may be plain text or RTF. Local profile files named `job_profile.txt` or `job_profile.rtf` are ignored by git.

Waiting-room evaluation can cap OpenAI spend with `--llm-limit N`. Prefilter-passing jobs above that cap are requeued in `results/waiting_room/<timestamp>/deferred_jobs.json` for a later evaluation instead of being discarded.

GitHub Actions usage:

- add repository secret `OPENAI_API_KEY`
- add repository secrets `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` for scheduled summaries
- add repository variable `JOB_MATCHER_MODE=openai`
- optionally add repository variable `OPENAI_MODEL=gpt-5.4-mini`
- keep `job_profile.rtf` in the configured Koofr memory folder

If `JOB_MATCHER_MODE` is unset, the workflow keeps using the free mock matcher.

## Cheat mode

Set repository variable `CHEAT_MODE=true` to inject `cheat_mode_job_ad.rtf` as one scraped job result. In GitHub Actions, keep that file in the same Koofr memory folder as `job_profile.rtf`; locally, place it under the selected `--results-root` or pass `--cheat-ad /path/to/cheat_mode_job_ad.rtf`.

When cheat mode is enabled, classification must use `JOB_MATCHER_MODE=openai`. The workflow prints a `[cheat-mode]` log line with the language-model decision for `cheat-mode-perfect-job` and fails the run if the model does not classify it as a hit.

## Telegram summaries

Each workflow run that passes the schedule gate sends a Telegram message with the number of ads considered in the last 12 hours, the number of unique job ads in result memory, and any new matches from the current run. For each match, the message includes the scraped ad URL and the OpenAI decision reason.

## GitHub Actions scheduling

Scheduling is split across two workflows:

- `.github/workflows/job-search-scheduler.yml` runs hourly and dispatches the execution workflow.
- `.github/workflows/job-search.yml` performs Koofr sync, scraping, optional evaluation, Telegram notification, and upload.

GitHub cron schedules are always interpreted in UTC. The scheduler converts the scheduled UTC time to `Europe/Stockholm` before deciding whether to dispatch an evaluation run. Scraping, Koofr download, and Koofr upload run every scheduled hour; matching and Telegram reporting are gated to scheduled local hours `07` and `19`.

To customize the schedule:

1. Change the cron expressions under `on.schedule` in `.github/workflows/job-search-scheduler.yml`.
2. Change the report hours in the scheduler dispatch step if Telegram summaries should be sent at different local hours.
3. Change each `TZ=Europe/Stockholm` value in the scheduler if the reporting gate should use a different local timezone.

Examples:

```yaml
# Run every two hours at minute 17, still UTC.
- cron: "17 */2 * * *"

# Run at 06:17 and 18:17 UTC.
- cron: "17 6,18 * * *"
```

Manual runs of `job-search.yml` enable Telegram reporting by default. Manual runs of the scheduler use the current Stockholm hour to decide the dispatched `report_job` value.

Each matching run writes an audit file next to the discard output:

```text
results/discard/<timestamp>/linkedin_jobs_sample_match_metadata.json
```

The workflow log also prints one `[decision]` line per job with the job ID, evaluation stage, hit value, title, and reason.

## Open-Source TODO

Before making this repository public, remove or generalize the currently personalized code and data:

- [ ] Remove tracked scraped job data from the repo and git history: [`results/discard/20260424_202129/linkedin_jobs_sample.json`](results/discard/20260424_202129/linkedin_jobs_sample.json#L1). It contains real LinkedIn job IDs, URLs, tracking parameters, locations, and full ad text.
- [ ] Replace or remove the current personal search config: [`linkedin_search_input.json`](linkedin_search_input.json#L1). It currently contains a Lund-specific LinkedIn geo ID at [`linkedin_search_input.json:3`](linkedin_search_input.json#L3).
- [ ] Make the template generic: [`linkedin_search_input.template.jsonc:4`](linkedin_search_input.template.jsonc#L4), [`linkedin_search_input.template.jsonc:11`](linkedin_search_input.template.jsonc#L11), [`linkedin_search_input.template.jsonc:12`](linkedin_search_input.template.jsonc#L12), and [`linkedin_search_input.template.jsonc:41`](linkedin_search_input.template.jsonc#L41) currently use Lund examples and `geoId=105734258`.
- [ ] Keep private profile files out of the public repo. Local ignored files such as `job_profile.txt` and `job_profile.rtf` contain personal career data; `.gitignore` covers them at [`.gitignore:165`](.gitignore#L165) and [`.gitignore:166`](.gitignore#L166).
- [ ] Generalize the GitHub Actions workflow: [`.github/workflows/job-search.yml:7`](.github/workflows/job-search.yml#L7), [`.github/workflows/job-search.yml:29`](.github/workflows/job-search.yml#L29), and [`.github/workflows/job-search.yml:37`](.github/workflows/job-search.yml#L37) hardcode `Europe/Stockholm` and 07:00/19:00 behavior; [`.github/workflows/job-search.yml:57`](.github/workflows/job-search.yml#L57) through [`.github/workflows/job-search.yml:85`](.github/workflows/job-search.yml#L85) are Koofr-specific.
- [ ] Decide whether cheat mode should be renamed to a neutral sentinel test feature. Current implementation references are [`linkedin_guest_jobs.py:29`](linkedin_guest_jobs.py#L29), [`linkedin_guest_jobs.py:30`](linkedin_guest_jobs.py#L30), [`match_jobs.py:24`](match_jobs.py#L24), and [`match_jobs.py:748`](match_jobs.py#L748).
- [ ] Make tests less profile-shaped: [`tests/test_linkedin_guest_jobs.py:28`](tests/test_linkedin_guest_jobs.py#L28), [`tests/test_linkedin_guest_jobs.py:174`](tests/test_linkedin_guest_jobs.py#L174), [`tests/test_match_jobs.py:102`](tests/test_match_jobs.py#L102), and [`tests/test_telegram_notify.py:99`](tests/test_telegram_notify.py#L99) contain Lund, cheat-job, or strongly profile-specific examples.
- [ ] Clean git history before publishing. Historical commits include LinkedIn result data and old hardcoded search URLs; use `git filter-repo` or create a fresh public repository from a cleaned working tree.
