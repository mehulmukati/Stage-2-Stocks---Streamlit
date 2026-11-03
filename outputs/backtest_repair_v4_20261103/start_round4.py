from pathlib import Path
import pandas as pd,json,hashlib,shutil
OUT=Path(__file__).parent;PREV=OUT.parent/'backtest_repair_v3_20261103'
if __name__=='__main__':
    if (OUT/'protected_baseline.json').exists() or (OUT/'price_completion_ledger_v1.csv').exists():
        raise FileExistsError('Round 4 baseline already exists; never rebase the sealed comparison inputs')
    work=pd.read_csv(PREV/'price_audit_v4/remaining_daily_price_worklist.csv',parse_dates=['date'])
    missing=work[work.price_source.eq('unavailable')].copy()
    special=missing.date.isin(pd.to_datetime(['2019-02-13','2019-03-29']))
    queries=pd.read_csv(PREV/'price_audit_v4/remaining_adjusted_history_acquisition.csv')
    ordinary=missing[~special]
    extras=ordinary[~ordinary.SYMBOL.isin(queries.historical_symbol)]
    assert len(missing)==33754 and special.sum()==1002 and len(ordinary)==32752
    assert ordinary.SYMBOL.nunique()==84 and extras.SYMBOL.nunique()==12
    ledger=work[['date','SYMBOL','ISIN','source_row_id','raw_source_sha256','price_source','ohlc_discrepancy_gt_1pct']].copy()
    ledger['row_id']=ledger.ISIN+':'+ledger.date.dt.strftime('%Y-%m-%d')
    ledger['reason']=['missing_market_session' if missing and day in pd.to_datetime(['2019-02-13','2019-03-29']) else 'missing_adjusted_observation' if missing else 'opposing_ohlc_observations' for day,missing in zip(ledger.date,ledger.price_source.eq('unavailable'))]
    ledger['resolution_status']='unresolved'
    ledger['resolution_evidence']=''
    ledger.to_csv(OUT/'price_completion_ledger_v1.csv',index=False)
    summary=dict(missing_adjusted=len(missing),missing_market_session_rows=int(special.sum()),other_missing_rows=len(ordinary),other_missing_labels=ordinary.SYMBOL.nunique(),partial_labels_outside_failed_queries=extras.SYMBOL.nunique(),ahlucont_partial_gaps=int(extras.SYMBOL.eq('AHLUCONT').sum()),ohlc_flags=int(work.ohlc_discrepancy_gt_1pct.sum()))
    (OUT/'baseline_counts.json').write_text(json.dumps(summary,indent=2))
    inputs=[]
    for r in json.loads((PREV/'protected_inputs_check.json').read_text()):inputs.append(dict(path=r['path'],sha256=hashlib.sha256(Path(r['path']).read_bytes()).hexdigest()))
    (OUT/'protected_baseline.json').write_text(json.dumps(inputs,indent=2))
    print(json.dumps(summary,indent=2))
