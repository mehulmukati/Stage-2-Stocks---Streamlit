from pathlib import Path
import json,hashlib,shutil,platform,sys
import pandas as pd
OUT=Path(__file__).resolve().parent;ROOT=OUT.parents[1]
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()

if __name__=='__main__':
    checks=[]
    for r in json.loads((OUT/'protected_baseline.json').read_text()):
        p=ROOT/r['path'];actual=sha(p)
        checks.append(dict(path=r['path'],baseline_sha256=r['round3_start_sha256'],sha256=actual,unchanged=actual==r['round3_start_sha256']))
    assert len(checks)==38 and all(r['unchanged'] for r in checks)
    (OUT/'protected_inputs_check.json').write_text(json.dumps(checks,indent=2))
    previous=ROOT/'outputs/backtest_repair_v2_20261103'
    old=json.loads((previous/'delivery_manifest.json').read_text())
    prior_checks={rel:sha(previous/rel)==expected for rel,expected in old['sha256'].items()}
    assert all(prior_checks.values())
    (OUT/'prior_delivery_hashes_check.json').write_text(json.dumps(dict(files=len(prior_checks),all_unchanged=True,checks=prior_checks),indent=2))
    candidate=json.loads((OUT/'candidate_v3/manifest.json').read_text())
    price=json.loads((OUT/'price_audit_v4/manifest.json').read_text())
    for manifest,fields in [(candidate,['code_hashes','primary_input_hashes','other_input_hashes']),(price,['source_hashes'])]:
        for field in fields:
            for rel,expected in manifest[field].items():assert sha(ROOT/rel)==expected,(field,rel)
    for view in ['chain','economic_membership_chain']:
        c=candidate[view]
        assert c['forward_anchor_matches'] and c['reverse_initial_matches']
        assert not c['forward_conflicts'] and not c['reverse_conflicts'] and c['monthly_mismatches']==0
    assert '463 passed' in (OUT/'full_tests_final.log').read_text()
    assert not (OUT/'lint_final.log').read_text().strip()
    assert not (OUT/'import_order_final.log').read_text().strip()
    assert not (OUT/'format_final.log').read_text().strip()
    independent=json.loads((OUT/'independent_membership_reconciliation_v2.json').read_text())
    assert independent['passed'] and independent['economic_forward_anchor_matches'] and independent['economic_reverse_initial_matches']
    assert json.loads((OUT/'independent_price_reconciliation.json').read_text())['passed']
    ledger=json.loads((OUT/'export_recheck.json').read_text())
    assert all(x['passed'] for x in ledger['reconciliation'].values())
    calendar=json.loads((OUT/'calendar_reconciliation.json').read_text())
    assert not calendar['missing_primary_sessions'] and not calendar['unexpected_primary_sessions']
    snapshots=OUT/'source_used';snapshots.mkdir(exist_ok=False)
    source_files=['historical_universe.py','historical_reconciliation.py','historical_price_evidence.py','scripts/build_bounded_nifty500.py','tests/test_historical_reconciliation.py']
    for rel in source_files:
        target=snapshots/rel;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/rel,target)
    environment=dict(platform=platform.platform(),python=sys.version,pandas=pd.__version__,scope='Retained evidence only; fresh provider fetches may be restated')
    (OUT/'environment.json').write_text(json.dumps(environment,indent=2))
    root_names=['.gitattributes','implementation_report.md','protected_baseline.json','protected_inputs_check.json','prior_delivery_hashes_check.json','environment.json','full_tests_final.log','lint_final.log','import_order_final.log','format_final.log','independent_membership_reconciliation_v2.json','independent_membership_audit_v2.log','independent_price_reconciliation.json','independent_price_audit.log','calendar_reconciliation.json','calendar_reconciliation.log','export_recheck.json','export_recheck.log','action_date_evidence.json','primary_review_transitions.csv','event_set_comparison.json','review_supersession_decisions.json','vendor_query_worklist.csv','raw_session_request_basis.json','build_candidate_v3.log','price_audit_v4.log']
    files=[OUT/n for n in root_names]
    files += list(OUT.glob('*.py'))+list(OUT.glob('*urls.json'))+list(OUT.glob('retrieval_*.json'))
    for directory in ['candidate_v3','price_audit_v4','evidence','source_used','extracted','visual_checks']:
        files += [p for p in (OUT/directory).rglob('*') if p.is_file() and '__pycache__' not in p.parts]
    result=dict(baseline_commit='6981092',canonical_candidate='candidate_v3',canonical_price_audit='price_audit_v4',status='Membership reconciled; adjusted-price/model candidate rejected for publication',tests='463 passed',identity_records=376,unique_transitions=374,event_dates=24,dated_lifecycle_decisions=24,applicable_lifecycle_decisions=23,monthly_comparisons=39,monthly_mismatches=0,raw_primary_sessions=783,identified_vendor_histories_acquired=72,vendor_histories_remaining=72,unsupported_adjusted_quote_rows=33754,ohlc_discrepancy_rows=246,active_sources_changed=False,live_checkpoints_advanced=False,acceptance_certificate_created=False,strategy_matrix_run=False,pushed=False,commit_policy='Local commit only; no push',sha256={p.relative_to(OUT).as_posix():sha(p) for p in sorted(set(files))})
    with (OUT/'delivery_manifest.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2)
    print('SEALED',len(result['sha256']),'artifacts;',len(checks),'protected inputs and',len(prior_checks),'prior-delivery artifacts unchanged')
