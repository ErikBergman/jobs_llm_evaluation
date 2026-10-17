#!/usr/bin/env python3
"""Rebuild cost and decision report from saved API usage; makes no API calls."""
import csv
import json
from decimal import Decimal
from pathlib import Path

OUT = Path(__file__).resolve().parent
manifest = json.loads((OUT / "manifest.json").read_text())
decisions = json.loads((OUT / "decisions.json").read_text())
ads = json.loads((OUT / "selected_ads.json").read_text())
attempts = [json.loads(line) for line in (OUT / "attempts.jsonl").read_text().splitlines()]


def calculate(attempt):
    u = attempt.get("usage")
    if not u:
        return None
    rates = {k: Decimal(str(v)) for k, v in manifest["rates_usd_per_million_tokens"][attempt["requested_model"]].items()}
    details = u.get("input_tokens_details") or {}
    cached, write = details.get("cached_tokens", 0), details.get("cache_write_tokens", 0)
    ordinary = u["input_tokens"] - cached - write
    assert ordinary >= 0
    assert write == 0 or rates["write"] > 0
    cost = (ordinary * rates["input"] + cached * rates["cached"] + write * rates["write"] + u["output_tokens"] * rates["output"]) / 1_000_000
    return {"input_tokens": u["input_tokens"], "ordinary_input_tokens": ordinary,
            "cached_input_tokens": cached, "cache_write_tokens": write,
            "output_tokens": u["output_tokens"],
            "reasoning_tokens": (u.get("output_tokens_details") or {}).get("reasoning_tokens", 0),
            "cost_usd": str(cost)}


totals, rows = {}, []
for model in manifest["models"]:
    selected = [a for a in attempts if a["requested_model"] == model]
    usage_rows = []
    for a in selected:
        c = calculate(a)
        if c:
            usage_rows.append(c)
        rows.append({"sequence": a["sequence"], "model": model, "stage": a["stage"],
                     "job_id": a["job_id"], "attempt": a["attempt_in_stage"],
                     "is_retry": a["is_retry"], "max_output_tokens": a["max_output_tokens"],
                     "status": a.get("status", a.get("error_type")), **(c or {})})
    t = {k: sum(c[k] for c in usage_rows) for k in ["input_tokens", "ordinary_input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "reasoning_tokens"]}
    t.update({"cost_usd": str(sum((Decimal(c["cost_usd"]) for c in usage_rows), Decimal(0))),
              "http_attempts": len(selected), "retry_attempts": sum(a["is_retry"] for a in selected),
              "responses_with_usage": len(usage_rows),
              "failed_connection_or_http_attempts": sum(bool(a.get("error_type")) for a in selected),
              "attempts_without_usage": sum(a.get("usage") is None for a in selected),
              "cost_complete": all(a.get("usage") is not None for a in selected),
              "pipeline_status": decisions[model]["status"]})
    t["output_budget_retries"] = sum(a["is_retry"] and a["max_output_tokens"] > (manifest["triage_max_output_tokens"] if a["stage"] == "triage" else manifest["confirmation_max_output_tokens"]) for a in selected)
    if not usage_rows:
        for field in ["input_tokens", "ordinary_input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "reasoning_tokens", "cost_usd"]:
            t[field] = None
    totals[model] = t
rows.sort(key=lambda r: r["sequence"])
(OUT / "cost_summary.json").write_text(json.dumps({"totals": totals, "attempts": rows}, indent=2) + "\n")
with (OUT / "attempt_usage.csv").open("w") as f:
    fields = list(dict.fromkeys(k for row in rows for k in row))
    writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)

complete = all(r["status"] == "complete" for r in decisions.values())
status_text = ("**Status: complete.** The comparison ran using OPENAI_JOBS_EVAL_READWRITE injected by Keyway from the development vault. Earlier connection/authentication failures are preserved in the attempt ledger." if complete else "**Status: incomplete.** See the per-model error and complete attempt ledger below; no missing decisions are inferred.")
report = ["# Three-ad comparison: gpt-5.4-mini and gpt-6-luna", "",
          status_text, "",
          f"Run: {manifest['started_at']} (Europe/Stockholm).", "",
          "Uses the existing `good_match` triage and confirmation prompts in `match_jobs.py`, the full `job_profile.txt`, low reasoning effort, structured JSON, and existing output budgets/retry behavior. Each model gets the same batch of three ads. Confirmation runs only for that model's selected candidates. These older ads have no `requirements_text`, so the existing triage fallback sends full descriptions.", "",
          "Selection spans two historical hits (QA and PCB design) and one historical discard (automation). These are purposively chosen contrasting roles, not a random sample; historical buckets are not verified labels. This run measures agreement and operational cost, not accuracy or general model quality.", "",
          "## Decisions", "",
          "| Ad | Historical bucket | gpt-5.4-mini | gpt-6-luna |", "|---|---|---|---|"]
agreement = 0
for ad, provenance in zip(ads["ads"], ads["provenance"]):
    cells = []
    for model in manifest["models"]:
        ds = decisions[model].get("decisions", [])
        d = next((d for d in ds if d["job_id"] == ad["job_id"]), None)
        cells.append(("HIT" if d["hit"] else "DISCARD") + " (" + d["stage"] + ")" if d else "Pipeline failed; see attempts")
    report.append(f"| {ad['title']} ({ad['job_id']}) | {provenance['historical_bucket']} | {' | '.join(cells)} |")
    if complete:
        ad_decisions = [next(d for d in decisions[model]["decisions"] if d["job_id"] == ad["job_id"])["hit"] for model in manifest["models"]]
        agreement += ad_decisions[0] == ad_decisions[1]
if complete:
    report.extend(["", f"Final decision agreement: {agreement}/{len(ads['ads'])}. Both models selected the QA ad at triage and rejected the other two. At confirmation, mini rejected the QA role as insufficiently aligned with the candidate's core career direction; Luna accepted its validation, reliability, Python, and automation overlap while acknowledging gaps. There is no human ground-truth label here to establish which decision is better."])
for model in manifest["models"]:
    report.extend(["", f"### {model}", ""])
    if decisions[model]["status"] != "complete":
        report.append("Pipeline error: " + decisions[model]["error"])
    for a in attempts:
        if a["requested_model"] == model and a["stage"] == "triage" and a.get("status") == "completed" and a.get("output_text"):
            report.append("Triage selected: " + a["output_text"])
            report.append("")
    for d in decisions[model].get("decisions", []):
        report.append(f"- **{d['title']}**: {'HIT' if d['hit'] else 'DISCARD'}. {d['reason']}")
        if d.get("triage_reason"):
            report.append(f"  Triage rationale: {d['triage_reason']}")
report.extend(["", "## Actual usage and calculated cost", "",
               "API-reported usage is summed once per HTTP response, including incomplete responses and retries. Shared batch-triage usage is counted once, rather than once per ad. Output includes reasoning tokens. No local token estimates are used.", "",
               "| Model | Request attempts | Retries | Input | Cached input | Cache writes | Output | Reasoning (part of output) | Cost USD |",
               "|---|---:|---:|---:|---:|---:|---:|---:|---:|"])
for model, t in totals.items():
    values = [f"{t[k]:,}" if t[k] is not None else "not returned" for k in ["input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "reasoning_tokens"]]
    cost = f"${t['cost_usd']}" if t["cost_usd"] is not None else "unavailable"
    report.append(f"| {model} | {t['http_attempts']} | {t['retry_attempts']} | {' | '.join(values)} | {cost} |")
report.extend(["", "The ledger includes the earlier four attempts: one sandbox connection failure and one HTTP 401 per model. They returned no usage and remain explicitly unmetered; their token use/cost is not inferred to be zero. The totals sum actual usage from every subsequent returned response, including incomplete responses and repeated prompts on output-budget retries. Earlier failures do not overwrite or replace any later usage.", "",
               "Retry counts include resumed triage requests after the earlier connection/authentication failures. Output-budget retries are counted separately below; each uses the existing matcher's doubled output allowance.", ""])
for model, t in totals.items():
    report.append(f"- {model}: {t['responses_with_usage']} metered responses; {t['output_budget_retries']} output-budget retries; {t['attempts_without_usage']} unmetered failed attempts.")
report.extend(["", "| Attempt | Model | Stage / job | Budget | Status | Input | Output | Cost USD |", "|---:|---|---|---:|---|---:|---:|---:|"])
for r in rows:
    report.append(f"| {r['sequence']} | {r['model']} | {r['stage']} / {r['job_id'] or 'batch'}{' (retry)' if r['is_retry'] else ''} | {r['max_output_tokens']} | {r['status']} | {r.get('input_tokens', 'unknown')} | {r.get('output_tokens', 'unknown')} | {r.get('cost_usd', 'unknown')} |")
mini, luna = [totals[m] for m in manifest["models"]]
if mini["cost_usd"] is not None and luna["cost_usd"] is not None and Decimal(mini["cost_usd"]) > 0 and complete:
    saving = (1 - Decimal(luna["cost_usd"]) / Decimal(mini["cost_usd"])) * 100
    report.extend(["", f"Cost reduction for Luna based on returned usage: {saving:.1f}%. This includes differences in the number of selected confirmations, retry behavior, output length, and cache usage, where present. Earlier unmetered failures are excluded from this arithmetic."])
report.extend(["", "Rates (USD per million tokens), verified 2026-10-17: `gpt-5.4-mini` input 0.75, cached input 0.075, output 4.50; `gpt-6-luna` input 0.10, cached input 0.01, cache writes 0.125, output 0.50. [Official pricing](https://developers.openai.com/api/docs/pricing).",
               "", "Cost = ((input − cached − writes) × input rate + cached × cached rate + writes × write rate + output × output rate) / 1,000,000. Cache writes replace the ordinary input rate for those tokens. [Official cache accounting](https://developers.openai.com/api/docs/guides/prompt-caching). These are calculated standard-rate costs from actual usage, before tax, credits, or account-specific discounts; not an invoice lookup.",
               "", "## Isolation and reproducibility", "",
               "Only this evaluation directory is written. The existing matcher is imported with bytecode writes disabled; its in-memory `post_json` injection records all attempts. The normal classify/queue workflow is not invoked. Model names are passed explicitly; no model default or environment file is edited. HTTP timeout is 120 seconds and `service_tier=default` is explicit; other model parameters, prompt text, profile, schemas, and retry policy come from the existing matcher.", "",
               "`manifest.json` records settings, source hashes, profile/matcher hashes, pricing, and provenance. `request_*.json`, `response_*.json`, and `attempts.jsonl` retain per-attempt evidence without authentication headers. `attempt_usage.csv` and `cost_summary.json` contain derived usage/cost. `integrity.json` records the before/after protected-file check. The runner refuses to overwrite an existing attempt ledger.", ""])
integrity = json.loads((OUT / "integrity.json").read_text())
report.append(f"Protected-file verification: {'unchanged' if integrity['unchanged'] else 'CHANGES DETECTED'} across {integrity['protected_file_count']} files (size, modification time, and mode). ctime is captured but excluded because file-provider reads can change it. Changed paths: {integrity['changed_paths']}.")
(OUT / "report.md").write_text("\n".join(report) + "\n")
print(json.dumps(totals, indent=2))
