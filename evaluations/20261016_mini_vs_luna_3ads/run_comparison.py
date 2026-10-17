#!/usr/bin/env python3
"""Isolated evaluation; records every HTTP attempt, including built-in retries."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.dont_write_bytecode = True
OUT = Path(__file__).resolve().parent
ROOT = OUT.parent.parent
sys.path.insert(0, str(ROOT))
import match_jobs as matcher

MODELS = ["gpt-5.4-mini", "gpt-6-luna"]
PRICES = {
    "gpt-5.4-mini": {"input": 0.75, "cached": 0.075, "write": 0, "output": 4.50},
    "gpt-6-luna": {"input": 0.10, "cached": 0.01, "write": 0.125, "output": 0.50},
}
SOURCES = [
    ("results/hits/20260425_095859/linkedin_jobs_sample_hits.json", "4405304862", "historical_hit"),
    ("results/hits/20260425_095859/linkedin_jobs_sample_hits.json", "4406180171", "historical_hit"),
    ("results/discard/20260425_095859/linkedin_jobs_sample_discard.json", "4405706422", "historical_discard"),
]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def protected_snapshot():
    paths = set((ROOT / "results").rglob("*"))
    paths.update(ROOT.glob("*.py"))
    paths.update(ROOT.glob("*.json*"))
    paths.update((ROOT / ".github").rglob("*"))
    paths.update([ROOT / "job_profile.txt", ROOT / "README.md"])
    # File metadata avoids opening unrelated cloud-backed files. Exact hashes of
    # the profile, matcher, and selected source payloads are saved separately.
    snapshot = {}
    for p in sorted(paths):
        if p.is_file():
            st = p.stat()
            snapshot[str(p.relative_to(ROOT))] = {
                "size": st.st_size, "mtime_ns": st.st_mtime_ns,
                "ctime_ns": st.st_ctime_ns, "mode": st.st_mode,
            }
    return snapshot


def main():
    key = os.environ.get("OPENAI_JOBS_EVAL_READWRITE", "")
    if not key:
        print("OPENAI_JOBS_EVAL_READWRITE unavailable in Python process; no requests made.")
        return 2
    ledger_path = OUT / "attempts.jsonl"
    resume = any(flag in sys.argv for flag in ["--resume-network-failures", "--resume-failed-requests"])
    previous_attempts = [json.loads(line) for line in ledger_path.read_text().splitlines()] if ledger_path.exists() else []
    if ledger_path.exists() and not (resume and previous_attempts and all((a.get("error_type") == "URLError" or a.get("http_status") == 401) and not a.get("usage") for a in previous_attempts)):
        print("Existing ledger found; refusing to rerun or overwrite billed attempts.")
        return 2

    def save(path, obj):
        serialized = json.dumps(obj, indent=2, ensure_ascii=False)
        path.write_text(serialized.replace(key, "[REDACTED]") + "\n")

    before = protected_snapshot()
    save(OUT / ("protected_before_resume.json" if resume else "protected_before.json"), before)
    print("Protected file metadata recorded; loading profile and three ads.", flush=True)
    profile = matcher.load_job_profile(ROOT / "job_profile.txt")
    jobs, provenance = [], []
    for source, job_id, historical in SOURCES:
        ad = next(j for j in json.loads((ROOT / source).read_text()) if j["job_id"] == job_id)
        jobs.append(ad)
        provenance.append({"job_id": job_id, "source": source, "historical_bucket": historical})
    save(OUT / "selected_ads.json", {"ads": jobs, "provenance": provenance})
    manifest = {
        "started_at": datetime.now(ZoneInfo("Europe/Stockholm")).isoformat(),
        "credential_variable": "OPENAI_JOBS_EVAL_READWRITE",
        "credential_delivery": "keyway run --env development; value never saved",
        "models": MODELS, "match_level": matcher.DEFAULT_OPENAI_MATCH_LEVEL,
        "reasoning_effort": matcher.OPENAI_STRUCTURED_JSON_REASONING_EFFORT,
        "triage_max_output_tokens": matcher.triage_max_output_tokens(3),
        "confirmation_max_output_tokens": matcher.DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
        "built_in_retry": "One retry at double output budget, only for empty output with max_output_tokens incomplete reason.",
        "selection": "Purposive sample: QA plausible match, PCB skill mismatch, automation adjacent but specialist PLC/SCADA requirements. Two historical hits and one historical discard; historical buckets are not ground-truth labels.",
        "execution": "Existing openai_two_stage_decisions; confirmation only for triage-selected ads. No keyword/location prefilter rerun. No queue/classify CLI invoked.",
        "transport": "urllib with 120-second timeout, no hidden transport retries; explicit service_tier=default. All other payload parameters and prompts come from existing matcher.",
        "profile_sha256": digest(ROOT / "job_profile.txt"),
        "matcher_sha256": digest(ROOT / "match_jobs.py"),
        "source_sha256": {source: digest(ROOT / source) for source, _, _ in SOURCES},
        "rates_usd_per_million_tokens": PRICES,
        "pricing_source": "https://developers.openai.com/api/docs/pricing",
        "cache_accounting_source": "https://developers.openai.com/api/docs/guides/prompt-caching",
        "pricing_verified_date": "2026-10-17",
        "cost_formula": "((input-cached-write)*input_rate + cached*cached_rate + write*write_rate + output*output_rate)/1000000; output includes reasoning. Published standard short-context rates, before taxes/credits/contract discounts.",
    }
    save(OUT / "manifest.json", manifest)
    print("Inputs loaded; starting recorded API requests.", flush=True)
    attempts, results = previous_attempts, {}
    for model in MODELS:
        counters = {}
        for previous in previous_attempts:
            if previous["requested_model"] == model:
                identity = (previous["stage"], previous["job_id"])
                counters[identity] = counters.get(identity, 0) + 1

        def logged_post(url, payload, headers):
            schema = payload["text"]["format"]["name"]
            stage = "triage" if "triage" in schema else "confirmation"
            job_id = None
            if stage == "confirmation":
                job_id = next(j["job_id"] for j in jobs if matcher.openai_match_confirmation_prompt(j, profile, matcher.DEFAULT_OPENAI_MATCH_LEVEL) == payload["input"])
            identity = (stage, job_id)
            counters[identity] = counters.get(identity, 0) + 1
            seq = len(attempts) + 1
            request_payload = dict(payload, service_tier="default")
            save(OUT / f"request_{seq:02d}.json", request_payload)
            record = {"sequence": seq, "requested_model": model, "stage": stage,
                      "started_at": datetime.now(ZoneInfo("Europe/Stockholm")).isoformat(),
                      "job_id": job_id, "attempt_in_stage": counters[identity],
                      "is_retry": counters[identity] > 1, "max_output_tokens": payload["max_output_tokens"],
                      "request_file": f"request_{seq:02d}.json"}
            started = time.monotonic()
            try:
                request = Request(url, data=json.dumps(request_payload).encode(), headers=headers, method="POST")
                with urlopen(request, timeout=120) as response:
                    raw = json.loads(response.read().decode())
                    record["http_status"] = response.status
                    record["request_id"] = response.headers.get("x-request-id")
                save(OUT / f"response_{seq:02d}.json", raw)
                record.update({"response_file": f"response_{seq:02d}.json", "response_id": raw.get("id"),
                               "resolved_model": raw.get("model"), "status": raw.get("status"),
                               "incomplete_details": raw.get("incomplete_details"),
                               "usage": raw.get("usage"), "output_text": matcher.extract_response_text(raw),
                               "service_tier": raw.get("service_tier")})
            except Exception as exc:
                record["error_type"] = type(exc).__name__
                if hasattr(exc, "reason"):
                    record["transport_reason"] = str(exc.reason).replace(key, "[REDACTED]")
                record["http_status"] = exc.code if isinstance(exc, HTTPError) else None
                # Never serialize exception objects, request objects, or headers.
                record["usage"] = None
                if isinstance(exc, HTTPError):
                    body = exc.read().decode("utf-8", "replace").replace(key, "[REDACTED]")
                    if exc.code == 401:
                        try:
                            error_info = json.loads(body).get("error", {})
                        except Exception:
                            error_info = {}
                        body = json.dumps({"error": {"type": error_info.get("type"), "code": error_info.get("code"), "param": error_info.get("param"), "message": "Authentication failed; credential text omitted."}})
                    record["error_body"] = body
                raise RuntimeError(f"Attempt {seq} failed ({record['error_type']}); see sanitized ledger.") from None
            finally:
                record["elapsed_seconds"] = round(time.monotonic() - started, 3)
                attempts.append(record)
                with ledger_path.open("a") as handle:
                    handle.write(json.dumps(record, ensure_ascii=False).replace(key, "[REDACTED]") + "\n")
                u = record.get("usage") or {}
                print(json.dumps({k: record.get(k) for k in ["sequence", "requested_model", "stage", "job_id", "attempt_in_stage", "status", "error_type"]} | {"input_tokens": u.get("input_tokens"), "output_tokens": u.get("output_tokens")}), flush=True)
            return raw

        try:
            decisions = matcher.openai_two_stage_decisions(jobs, key, profile, model=model, post_json=logged_post)
            results[model] = {"status": "complete", "decisions": decisions}
        except Exception as exc:
            results[model] = {"status": "failed", "error": str(exc).replace(key, "[REDACTED]")}
        save(OUT / "decisions.json", results)
    after = protected_snapshot()
    fields = ["size", "mtime_ns", "mode"]
    changed = sorted(p for p in set(before) | set(after) if any(before.get(p, {}).get(f) != after.get(p, {}).get(f) for f in fields))
    integrity = {"unchanged": not changed,
                 "verification": "File size, mtime_ns, and mode across results, root Python/config files, workflows, profile, and README. ctime is captured but excluded because file-provider materialization changes it on reads.",
                 "changed_paths": changed,
                 "protected_file_count": len(before)}
    save(OUT / "integrity.json", integrity)
    save(OUT / "protected_after.json", after)
    manifest["finished_at"] = datetime.now(ZoneInfo("Europe/Stockholm")).isoformat()
    save(OUT / "manifest.json", manifest)
    print(json.dumps({"result_directory": str(OUT), "integrity": integrity, "statuses": {m:r["status"] for m,r in results.items()}}), flush=True)
    return 0 if all(r["status"] == "complete" for r in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
