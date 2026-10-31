# Investigation: steady evaluation counts after employer imports

Checked GitHub execution runs and completed evaluation logs on 2026-10-31.

The last 100 job-search executions contain 99 scheduler dispatches and the one scrape-only verification run. Every execution has `run_attempt=1`. Scheduler and job-search entries are separate parts of one dispatch chain. The examined report slots are distinct and occur twice per day; no duplicate report slot was found.

Across eight completed evaluation runs, 296 distinct ad IDs were evaluated with no repeated IDs. Six runs after employer scraping was enabled account for 261 distinct evaluated ads, of which 240 reached the AI stage and 21 were rejected locally. The fixed 40-ad batches are the configured LLM-eligible batch cap draining a backlog, not evidence of duplicated profile requests.

| Evaluation run | Evaluated ads | Sent to AI | Deferred | Reported rolling 12-hour total |
| --- | ---: | ---: | ---: | ---: |
| [Oct 28 evening](https://github.com/ErikBergman/jobs_llm_evaluation/actions/runs/40780669223) | 61 | 40 | 243 | 76 |
| [Oct 29 morning](https://github.com/ErikBergman/jobs_llm_evaluation/actions/runs/40879811267) | 40 | 40 | 196 | 40 |
| [Oct 29 evening](https://github.com/ErikBergman/jobs_llm_evaluation/actions/runs/40967932627) | 40 | 40 | 153 | 80 |
| [Oct 30 morning](https://github.com/ErikBergman/jobs_llm_evaluation/actions/runs/41060300865) | 40 | 40 | 136 | 40 |
| [Oct 30 evening](https://github.com/ErikBergman/jobs_llm_evaluation/actions/runs/41136919491) | 40 | 40 | 109 | 80 |
| [Oct 31 morning](https://github.com/ErikBergman/jobs_llm_evaluation/actions/runs/41202300219) | 40 | 40 | 71 | 40 |

Morning/evening refer to the intended 07:17/19:17 Europe/Stockholm slots. Actual dispatches were several hours late. For example, the Oct 31 07:17 slot began at 13:42 local time. Consecutive reports sometimes ran less than 12 hours apart, so their rolling processing totals legitimately included two batches (80).

The Oct 31 evaluation collected no new ads during that particular scrape but processed 40 queued ads, leaving 71. Its duplicate-check audit made 20 bounded calls; this is a distinct stage and does not establish repeated comparison of identical prompts. Existing comparison fingerprints and the persistent cache control duplicate-check reuse. These logs do not expose every individual duplicate-check fingerprint, so no claim is made that all historical paid comparison attempts were unique.

Changes made:

- Show current-batch, AI-reviewed, and pending-backlog counts separately in Telegram.
- Count distinct recorded decision IDs in the rolling window and reject future timestamps; keep a legacy count fallback for old metadata without decisions.
- Exclude already recorded evaluations from automatic queue processing before either LLM stage. Remove stale completed copies during shared queue cleanup; preserve unprocessed and deferred ads.
- Record queue/cap/deferral metrics in match metadata and the Actions audit.
- Log duplicate-request fingerprints and preserve them in cached/fresh audit decisions so future paid comparisons can be checked for identical repeated inputs.

The batch limit and twice-daily evaluation schedule are unchanged. Verification uses simulated model responses; no additional paid evaluation or Telegram notification is triggered for this investigation.
