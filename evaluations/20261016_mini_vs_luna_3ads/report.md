# Three-ad comparison: gpt-5.4-mini and gpt-6-luna

**Status: complete.** The comparison ran using OPENAI_JOBS_EVAL_READWRITE injected by Keyway from the development vault. Earlier connection/authentication failures are preserved in the attempt ledger.

Run: 2026-10-17T12:57:58.348193+02:00 (Europe/Stockholm).

Uses the existing `good_match` triage and confirmation prompts in `match_jobs.py`, the full `job_profile.txt`, low reasoning effort, structured JSON, and existing output budgets/retry behavior. Each model gets the same batch of three ads. Confirmation runs only for that model's selected candidates. These older ads have no `requirements_text`, so the existing triage fallback sends full descriptions.

Selection spans two historical hits (QA and PCB design) and one historical discard (automation). These are purposively chosen contrasting roles, not a random sample; historical buckets are not verified labels. This run measures agreement and operational cost, not accuracy or general model quality.

## Decisions

| Ad | Historical bucket | gpt-5.4-mini | gpt-6-luna |
|---|---|---|---|
| Software QA Engineer, Lund (4405304862) | historical_hit | DISCARD (confirmation) | HIT (confirmation) |
| Electronic Engineer (PCB Design) (4406180171) | historical_hit | DISCARD (triage) | DISCARD (triage) |
| Senior Automation Engineer - Denmark (4405706422) | historical_discard | DISCARD (triage) | DISCARD (triage) |

Final decision agreement: 2/3. Both models selected the QA ad at triage and rejected the other two. At confirmation, mini rejected the QA role as insufficiently aligned with the candidate's core career direction; Luna accepted its validation, reliability, Python, and automation overlap while acknowledging gaps. There is no human ground-truth label here to establish which decision is better.

### gpt-5.4-mini

Triage selected: {"candidates":[{"candidate_id":"4405304862","reason":"His production validation mindset, test automation experience, Python/Git background, and cross-team communication make this a plausible QA/automation-adjacent match worth deeper review."}]}

- **Software QA Engineer, Lund**: DISCARD. The role aligns with his QA/automation mindset and Python/validation experience in Lund, but it is primarily a software testing position and doesn’t strongly match his core backend integration, process, or applied-LLM direction.
  Triage rationale: His production validation mindset, test automation experience, Python/Git background, and cross-team communication make this a plausible QA/automation-adjacent match worth deeper review.
- **Electronic Engineer (PCB Design)**: DISCARD. Rejected by Eval 2: LLM triage.
- **Senior Automation Engineer - Denmark**: DISCARD. Rejected by Eval 2: LLM triage.

### gpt-6-luna

Triage selected: {"candidates":[{"candidate_id":"4405304862","reason":"His production-system testing, validation, Python automation, engineering background, and clear cross-team communication make this QA role worth reviewing despite its C# and hardware-testing gaps."}]}

- **Software QA Engineer, Lund**: HIT. Lund-based engineering role aligns with his Python, pytest, validation, production-reliability, and process-automation experience, though C# and a QA-focused career are gaps.
  Triage rationale: His production-system testing, validation, Python automation, engineering background, and clear cross-team communication make this QA role worth reviewing despite its C# and hardware-testing gaps.
- **Electronic Engineer (PCB Design)**: DISCARD. Rejected by Eval 2: LLM triage.
- **Senior Automation Engineer - Denmark**: DISCARD. Rejected by Eval 2: LLM triage.

## Actual usage and calculated cost

API-reported usage is summed once per HTTP response, including incomplete responses and retries. Shared batch-triage usage is counted once, rather than once per ad. Output includes reasoning tokens. No local token estimates are used.

| Model | Request attempts | Retries | Input | Cached input | Cache writes | Output | Reasoning (part of output) | Cost USD |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| gpt-5.4-mini | 4 | 2 | 20,491 | 0 | 0 | 414 | 288 | $0.01723125 |
| gpt-6-luna | 4 | 2 | 20,491 | 0 | 20,485 | 239 | 123 | $0.002680725 |

The ledger includes the earlier four attempts: one sandbox connection failure and one HTTP 401 per model. They returned no usage and remain explicitly unmetered; their token use/cost is not inferred to be zero. The totals sum actual usage from every subsequent returned response, including incomplete responses and repeated prompts on output-budget retries. Earlier failures do not overwrite or replace any later usage.

Retry counts include resumed triage requests after the earlier connection/authentication failures. Output-budget retries are counted separately below; each uses the existing matcher's doubled output allowance.

- gpt-5.4-mini: 2 metered responses; 0 output-budget retries; 2 unmetered failed attempts.
- gpt-6-luna: 2 metered responses; 0 output-budget retries; 2 unmetered failed attempts.

| Attempt | Model | Stage / job | Budget | Status | Input | Output | Cost USD |
|---:|---|---|---:|---|---:|---:|---:|
| 1 | gpt-5.4-mini | triage / batch | 520 | URLError | unknown | unknown | unknown |
| 2 | gpt-6-luna | triage / batch | 520 | URLError | unknown | unknown | unknown |
| 3 | gpt-5.4-mini | triage / batch (retry) | 520 | HTTPError | unknown | unknown | unknown |
| 4 | gpt-6-luna | triage / batch (retry) | 520 | HTTPError | unknown | unknown | unknown |
| 5 | gpt-5.4-mini | triage / batch (retry) | 520 | completed | 10937 | 143 | 0.00884625 |
| 6 | gpt-5.4-mini | confirmation / 4405304862 | 512 | completed | 9554 | 271 | 0.008385 |
| 7 | gpt-6-luna | triage / batch (retry) | 520 | completed | 10937 | 186 | 0.00146005 |
| 8 | gpt-6-luna | confirmation / 4405304862 | 512 | completed | 9554 | 53 | 0.001220675 |

Cost reduction for Luna based on returned usage: 84.4%. This includes differences in the number of selected confirmations, retry behavior, output length, and cache usage, where present. Earlier unmetered failures are excluded from this arithmetic.

Rates (USD per million tokens), verified 2026-10-17: `gpt-5.4-mini` input 0.75, cached input 0.075, output 4.50; `gpt-6-luna` input 0.10, cached input 0.01, cache writes 0.125, output 0.50. [Official pricing](https://developers.openai.com/api/docs/pricing).

Cost = ((input − cached − writes) × input rate + cached × cached rate + writes × write rate + output × output rate) / 1,000,000. Cache writes replace the ordinary input rate for those tokens. [Official cache accounting](https://developers.openai.com/api/docs/guides/prompt-caching). These are calculated standard-rate costs from actual usage, before tax, credits, or account-specific discounts; not an invoice lookup.

## Isolation and reproducibility

Only this evaluation directory is written. The existing matcher is imported with bytecode writes disabled; its in-memory `post_json` injection records all attempts. The normal classify/queue workflow is not invoked. Model names are passed explicitly; no model default or environment file is edited. HTTP timeout is 120 seconds and `service_tier=default` is explicit; other model parameters, prompt text, profile, schemas, and retry policy come from the existing matcher.

`manifest.json` records settings, source hashes, profile/matcher hashes, pricing, and provenance. `request_*.json`, `response_*.json`, and `attempts.jsonl` retain per-attempt evidence without authentication headers. `attempt_usage.csv` and `cost_summary.json` contain derived usage/cost. `integrity.json` records the before/after protected-file check. The runner refuses to overwrite an existing attempt ledger.

Protected-file verification: unchanged across 34 files (size, modification time, and mode). ctime is captured but excluded because file-provider reads can change it. Changed paths: [].
