"""Scoped raw-price and vendor worklist audit; never publishes observations."""
from pathlib import Path
from collections import defaultdict
import sys,json,hashlib,zipfile
import pandas as pd
import numpy as np

ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from historical_price_evidence import parse_vendor_chart
from historical_universe import read_bhavcopy, legal_name_key
import market_data as md
OUT=Path(__file__).parent;PREV=ROOT/'outputs/backtest_repair_v2_20261103';CAND=OUT/'candidate_v3'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()

if __name__=='__main__':
    target=OUT/sys.argv[1];target.mkdir(exist_ok=False)
    membership=pd.read_parquet(CAND/'provisional_membership.parquet')
    states={d:set(g.SECURITY_ID.str.removeprefix('ISIN:')) for d,g in membership.groupby('TIME_STAMP')}
    paths=sorted({p.name:p for root in [PREV,OUT] for p in (root/'evidence').glob('cm*.zip') if zipfile.is_zipfile(p)}.values())
    raw=[];missing=[];file_checks=[];universe_ids=set().union(*states.values());days=set()
    for p in paths:
        f=read_bhavcopy(p);day=f.date.iloc[0]
        if not pd.Timestamp('2017-07-01')<=day<=pd.Timestamp('2020-09-01'):continue
        days.add(day);state=states[max(d for d in states if d<=day)]
        # Retain every relevant security observation, including nonmembers on a
        # date, for price-identity comparisons; eligibility is a separate field.
        own=f[f.ISIN.isin(universe_ids)].copy()
        own['eligible']=own.ISIN.isin(state)
        own['raw_source_sha256']=sha(p)
        raw.append(own)
        positive=set(own.loc[own.SERIES.isin(['EQ','BE','BZ'])&own.CLOSE.gt(0)&own.TOTTRDQTY.gt(0),'ISIN'])
        for isin in sorted(state-positive):missing.append(dict(date=day,security_id='ISIN:'+isin,reason='no positive-volume allowed-series primary quote; never filled or removed'))
        file_checks.append(dict(path=str(p.relative_to(ROOT)),sha256=sha(p),date=str(day.date()),eligible_members=len(state),eligible_positive_quotes=len(state&positive)))
    raw=pd.concat(raw,ignore_index=True)
    raw.to_parquet(target/'raw_security_quotes.parquet',index=False)
    pd.DataFrame(missing).to_csv(target/'eligible_missing_raw_quotes.csv',index=False)
    (target/'raw_file_checks.json').write_text(json.dumps(file_checks,indent=2))
    work=pd.read_csv(OUT/'vendor_query_worklist.csv')
    histories=[];decisions=[];all_events={};links=[]
    observations=pd.read_parquet(PREV/'bounded_candidate_v3/primary_constituent_observations.parquet')
    for r in work.to_dict('records'):
        vendor=r['vendor_query_symbol'];path=OUT/'evidence'/(str(vendor)+'_yahoo_chart.json')
        record={**r,'vendor_payload_available':path.exists(),'identity_supported':'not certified' not in r['identity_basis'],'accepted_for_backtest':False}
        if not path.exists():
            record.update(status='vendor_history_unavailable',remaining='retired/security-specific adjusted acquisition required')
            decisions.append(record);continue
        try:
            payload=json.loads(path.read_text());bars,actions=parse_vendor_chart(payload,str(vendor)+'.NS')
        except (ValueError,KeyError) as exc:
            record.update(status='vendor_payload_rejected',remaining=str(exc));decisions.append(record);continue
        bars=bars[bars.date.between('2017-07-01','2020-09-01')]
        bars['source_sha256']=sha(path);bars['historical_label']=r['historical_symbol'];histories.append(bars)
        all_events[vendor]=actions
        ids=r['historical_isins'].split('|')
        primary=raw[raw.ISIN.isin(ids)&raw.SERIES.isin(['EQ','BE','BZ'])].drop_duplicates(['date','ISIN'])
        joined=bars[bars.status.eq('observed')].merge(primary[['date','ISIN','CLOSE','OPEN','HIGH','LOW','TOTTRDQTY','eligible']],on='date')
        meta=payload['chart']['result'][0]['meta']
        legal_names=set(observations.loc[observations.symbol.eq(r['historical_symbol']),'name'].map(legal_name_key))
        exact_name=bool(legal_names & {legal_name_key(meta.get('shortName','')),legal_name_key(meta.get('longName',''))})
        if not record['identity_supported'] and exact_name and vendor==r['historical_symbol'] and meta.get('exchangeName')=='NSI' and len(joined)>=10:
            record['identity_supported']=True
            record['identity_basis']='exact dated primary legal name/symbol plus retrospective vendor name/symbol and dated own-ISIN price rows'
        ratio=joined.CLOSE/joined.raw_vendor_close
        record.update(vendor_observed_sessions=int(bars.status.eq('observed').sum()),vendor_first=str(bars.date.min().date()),vendor_last=str(bars.date.max().date()),primary_comparison_rows=len(joined),eligible_primary_comparison_rows=int(joined.eligible.sum()),vendor_payload_sha256=sha(path),vendor_actions=len(actions.get('splits',{})),vendor_distributions=len(actions.get('dividends',{})))
        # Historical raw prices and Yahoo split-normalized close are kept apart.
        # Ratios document normalization; they do not authorize a splice.
        record['raw_to_vendor_close_ratio_min']=float(ratio.min()) if len(ratio) else None
        record['raw_to_vendor_close_ratio_max']=float(ratio.max()) if len(ratio) else None
        record['status']='identity_supported_history_acquired' if record['identity_supported'] and len(joined) else 'history_acquired_identity_unresolved'
        record['remaining']='Adjustment/volume/entitlement seams and eligible-period gaps require validation; no accepted-source change'
        if record['identity_supported']:
            linked=joined[['date','ISIN','vendor_symbol','historical_label','source_sha256','eligible']].copy()
            linked['identity_basis']=record['identity_basis'];linked['validity_basis']='dated price observations only; no unobserved exchange alias interval'
            links.append(linked)
        decisions.append(record)
    prices=pd.concat(histories,ignore_index=True)
    prices.to_parquet(target/'staged_vendor_adjusted_prices.parquet',index=False)
    pd.DataFrame(decisions).to_csv(target/'missing_label_decisions.csv',index=False)
    (target/'vendor_action_evidence.json').write_text(json.dumps(all_events,indent=2))
    pd.concat(links,ignore_index=True).to_parquet(target/'dated_vendor_security_links.parquet',index=False)
    # Audit every eligible primary row against original local or newly acquired
    # observations. A price's existence is distinct from validated adjustment,
    # entitlement, nominal tax, liquidity or execution conventions.
    local=md.load_snapshot().equity_prices()
    local=local[local.date.between('2017-07-01','2020-09-01')&local.price_status.isin(['observed','verified_mark'])&local.Close.gt(0)]
    primary=raw[raw.eligible&raw.SERIES.isin(['EQ','BE','BZ'])].copy()
    coverage=primary[['date','SYMBOL','ISIN','OPEN','HIGH','LOW','CLOSE','TOTTRDQTY','source_row_id','raw_source_sha256']].merge(local[['date','symbol','Open','High','Low','Close','Volume']],left_on=['date','SYMBOL'],right_on=['date','symbol'],how='left')
    coverage['price_source']=np.where(coverage.Close.notna(),'protected_local_same_dated_exchange_label','unavailable')
    valid_labels={r['historical_symbol'] for r in decisions if r['identity_supported']}
    fresh=prices[prices.historical_label.isin(valid_labels)&prices.status.eq('observed')]
    coverage=coverage.merge(fresh[['date','historical_label','vendor_symbol','adjusted_open','adjusted_high','adjusted_low','adjusted_close','reported_volume']],left_on=['date','SYMBOL'],right_on=['date','historical_label'],how='left')
    use=coverage.Close.isna()&coverage.adjusted_close.gt(0)
    for before,after in [('Open','adjusted_open'),('High','adjusted_high'),('Low','adjusted_low'),('Close','adjusted_close'),('Volume','reported_volume')]:coverage.loc[use,before]=coverage.loc[use,after]
    coverage.loc[use,'price_source']='staged_identity_supported_vendor_history'
    ratio=coverage.CLOSE/coverage.Close
    errors=[]
    for raw_key,price_key in [('OPEN','Open'),('HIGH','High'),('LOW','Low')]:
        errors.append(((coverage[raw_key]-coverage[price_key]*ratio).abs()/(coverage[raw_key].abs()+.05)).fillna(0))
    coverage['max_normalized_ohlc_error']=pd.concat(errors,axis=1).max(axis=1)
    coverage['ohlc_discrepancy_gt_1pct']=coverage.max_normalized_ohlc_error.gt(.01)&coverage.Close.notna()
    coverage['accepted_adjustment_model']=False
    coverage.to_parquet(target/'all_eligible_daily_price_coverage.parquet',index=False)
    coverage[coverage.price_source.eq('unavailable')|coverage.ohlc_discrepancy_gt_1pct].to_csv(target/'remaining_daily_price_worklist.csv',index=False)
    coverage.groupby(['SYMBOL','ISIN']).agg(eligible_primary_rows=('date','count'),first_eligible_quote=('date','min'),last_eligible_quote=('date','max'),unavailable_adjusted_rows=('price_source',lambda x:int(x.eq('unavailable').sum())),ohlc_discrepancy_rows=('ohlc_discrepancy_gt_1pct','sum')).to_csv(target/'all_security_price_coverage.csv')
    summary=dict(historical_missing_labels=len(work),vendor_payloads=sum(r['vendor_payload_available'] for r in decisions),identity_supported_acquired=sum(r['status']=='identity_supported_history_acquired' for r in decisions),acquired_identity_unresolved=sum(r['status']=='history_acquired_identity_unresolved' for r in decisions),unavailable=sum(r['status']=='vendor_history_unavailable' for r in decisions),rejected_payloads=sum(r['status']=='vendor_payload_rejected' for r in decisions),staged_vendor_rows=len(prices),primary_sessions=len(days),primary_security_quote_rows=len(raw),eligible_missing_positive_quote_rows=len(missing),eligible_daily_quote_rows=len(coverage),eligible_daily_source_counts=coverage.price_source.value_counts().to_dict(),ohlc_discrepancy_rows=int(coverage.ohlc_discrepancy_gt_1pct.sum()),accepted_adjusted_histories=0,accepted=False,raw_and_adjusted_separate=True,source_hashes={str(p.relative_to(ROOT)):sha(p) for p in [Path(__file__),ROOT/'historical_price_evidence.py',OUT/'vendor_query_worklist.csv',CAND/'provisional_membership.parquet']})
    (target/'manifest.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary,indent=2))
