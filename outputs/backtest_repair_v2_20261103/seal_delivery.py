from pathlib import Path
import json,hashlib
out=Path('outputs/backtest_repair_v2_20261103')
selected=['baseline_manifest.json','implementation_report.md','boundary_before_after.csv','before_review_checks.json',
'after_review_checks.json','failing_oracles.log','full_tests_release.log','final_affected_tests.log','lint_final_verified.log',
'import_order_verified.log','protected_inputs_check.json','identity_resolution_recheck.json',
'primary_event_date_direction_checks.json','reviewed_lifecycle_decisions.json','accounting_diagnostic.log']
paths=[out/p for p in selected]
for directory in ['accounting_diagnostic','bounded_candidate_v3','diagnostic_source_used','final_source_snapshot','parsed_monthly']:
 paths.extend(p for p in (out/directory).rglob('*') if p.is_file() and '__pycache__' not in p.parts)
paths.extend(p for p in (out/'evidence').iterdir() if p.is_file())
checks=json.loads((out/'protected_inputs_check.json').read_text(encoding='utf-8-sig'))
assert all(x['unchanged'] for x in checks)
assert '451 passed' in (out/'full_tests_release.log').read_text()
assert not (out/'lint_final_verified.log').read_text().strip()
assert not (out/'import_order_verified.log').read_text().strip()
manifest=dict(baseline_commit='ef6139f',status='engine/publication boundaries repaired; bounded data candidate unaccepted',
    complete_suite='451 passed',independent_reconciliation='576 sessions per book; 4511 fills; maximum NAV error <3e-14',
    identity_progress=dict(events=376,names=291,reusable_decisions=295,anchor_securities=501),
    remaining=dict(lifecycle_transitions=24,monthly_discrepancies=38,missing_local_adjusted_histories=144),
    canonical_candidate='bounded_candidate_v3',strategy_matrix='not run; data scope unsupported',
    active_shared_sources_changed=False,live_checkpoints_advanced=False,pushed=False,committed=True,
    sha256={p.relative_to(out).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(paths))})
with (out/'delivery_manifest.json').open('x',encoding='utf-8') as f:json.dump(manifest,f,indent=2)
print('Sealed',len(paths),'final evidence/results/source artifacts; protected original inputs unchanged.')
