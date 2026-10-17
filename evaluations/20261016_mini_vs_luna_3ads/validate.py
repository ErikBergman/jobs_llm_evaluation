"""Read-only artifact validation; optionally checks injected key stays unsaved."""
import ast
import hashlib
import json
import os
from pathlib import Path

p = Path(__file__).resolve().parent
r = p.parent.parent
m = json.loads((p / 'manifest.json').read_text())
checks = {}
checks['sources_match_hashes'] = all(hashlib.sha256((r / f).read_bytes()).hexdigest() == h for f, h in m['source_sha256'].items())
checks['profile_unchanged'] = hashlib.sha256((r / 'job_profile.txt').read_bytes()).hexdigest() == m['profile_sha256']
checks['matcher_unchanged'] = hashlib.sha256((r / 'match_jobs.py').read_bytes()).hexdigest() == m['matcher_sha256']
a = json.loads((p / 'protected_before_resume.json').read_text())
b = json.loads((p / 'protected_after.json').read_text())
checks['queues_and_config_metadata_unchanged'] = all(all(a.get(f, {}).get(k) == b.get(f, {}).get(k) for k in ['size', 'mtime_ns', 'mode']) for f in set(a) | set(b))
key = os.environ.get('OPENAI_JOBS_EVAL_READWRITE', '').encode()
checks['credential_available_for_validation'] = bool(key)
checks['no_api_key_in_saved_artifacts'] = bool(key) and all(key not in f.read_bytes() for f in p.iterdir() if f.is_file())
checks['python_syntax_valid'] = all(ast.parse(f.read_text()) is not None for f in p.glob('*.py'))
attempts = [json.loads(line) for line in (p / 'attempts.jsonl').read_text().splitlines()]
metered = [a for a in attempts if a.get('usage')]
checks['all_metered_responses_saved'] = all(json.loads((p / a['response_file']).read_text())['usage'] == a['usage'] for a in metered)
checks['all_decisions_complete'] = all(v['status'] == 'complete' and len(v['decisions']) == 3 for v in json.loads((p / 'decisions.json').read_text()).values())
totals = json.loads((p / 'cost_summary.json').read_text())['totals']
checks['token_totals_match_every_returned_response'] = all(all(sum(a['usage'][field] for a in metered if a['requested_model'] == model) == totals[model][field] for field in ['input_tokens', 'output_tokens']) for model in m['models'])
(p / 'validation.json').write_text(json.dumps(checks, indent=2) + '\n')
print(json.dumps(checks, indent=2))
raise SystemExit(0 if all(checks.values()) else 1)
