"""Reproduce staged Round 3 decisions from immutable primary evidence."""
from pathlib import Path
import sys,json,hashlib,zipfile,re
import pandas as pd

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from historical_universe import read_bhavcopy, validate_constituent_cache
from historical_reconciliation import walk_membership, project_raw_identifier_observation
from review_catalogue import catalogue

OUT=Path(__file__).parent
PREV=ROOT/'outputs/backtest_repair_v2_20261103'
OLD=PREV/'bounded_candidate_v3'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()

if __name__=='__main__':
    target=OUT/sys.argv[1]
    target.mkdir(exist_ok=False)
    cache_checks=[]
    for p in sorted((PREV/'parsed_monthly').glob('indices*.parquet')):
        f=pd.read_parquet(p);archive=PREV/'evidence'/(p.stem+'.zip')
        validate_constituent_cache(f,archive)
        cache_checks.append(dict(cache=str(p.relative_to(ROOT)),cache_sha256=sha(p),archive=str(archive.relative_to(ROOT)),archive_sha256=sha(archive)))
    (target/'parsed_cache_checks.json').write_text(json.dumps(cache_checks,indent=2))
    paths=sorted({p.name:p for root in [PREV,OUT] for p in (root/'evidence').glob('cm*.zip') if zipfile.is_zipfile(p)}.values())
    bhav=pd.concat([read_bhavcopy(p) for p in paths],ignore_index=True)
    bhav.to_parquet(target/'raw_exchange_observations.parquet',index=False)
    work=pd.read_csv(OLD/'lifecycle_transition_worklist.csv')
    action_dates=json.loads((OUT/'action_date_evidence.json').read_text())
    ratios={'AVANTIFEED':3,'BRITANNIA':2,'CANFINHOME':5,'CHOLAFIN':5,'CONCOR':2,'EICHERMOT':10,'HDFCBANK':2,'HERITGFOOD':2,'IEX':10,'IGL':5,'IRCON':5,'NAVINFLUOR':5,'NBCC':2,'SUNTECK':2,'TRIDENT':10,'VINATIORGA':2,'YESBANK':5,'ZENSARTECH':5}
    actions=[]
    for r in work.to_dict('records'):
        if r['new_isin']=='INE528G01035':
            date='2020-03-16';source='INSP43833.pdf';ratio=1;kind='reconstruction_with_conditional_lock_in'
            purpose='75% lock-in for existing holdings >=100 shares under scheme; not a split; nominal execution unsupported'
        else:
            matching=[a for a in action_dates if a['symbol']==r['symbol']]
            assert len(matching)==1,r
            a=matching[0];date=a['ex_date'];source=a['source'];ratio=a.get('share_ratio',ratios.get(r['symbol']))
            assert ratio is not None,r
            kind='split_and_bonus' if r['symbol']=='AVANTIFEED' else 'face_value_split'
            purpose=a.get('purpose','Exchange split circular; ratio '+str(ratio))
        same=bhav[bhav.SYMBOL.eq(r['symbol'])&bhav.SERIES.eq('EQ')]
        new=same[same.ISIN.eq(r['new_isin'])&same.date.ge(date)].sort_values('date').iloc[0]
        old=same[same.ISIN.eq(r['old_isin'])&same.date.lt(new.date)].sort_values('date').iloc[-1]
        # First/last monthly observations are insufficient: both boundary rows
        # below come from newly acquired adjacent daily exchange observations.
        assert (new.date-old.date).days<=4,(r,old.date,new.date)
        fact=OUT/'evidence'/source
        actions.append(dict(action_id=r['symbol']+':'+date,symbol=r['symbol'],old_security_id='ISIN:'+r['old_isin'],new_security_id='ISIN:'+r['new_isin'],kind=kind,ex_date=date,identifier_effective_date=str(new.date.date()),last_daily_old_identifier=str(old.date.date()),share_ratio=ratio,purpose=purpose,membership_conversion_supported=True,portfolio_adjustment_authorized=False,price_policy='Evidence only; do not multiply already-adjusted units or credit dividends',applicable_to_scope=str(new.date.date())<='2020-09-01',affected_event_row_ids=r['affected_event_row_ids'] if pd.notna(r['affected_event_row_ids']) else '',source=str(fact.relative_to(ROOT)),source_sha256=sha(fact),boundary_evidence=[old.source_row_id,new.source_row_id],verified_at='2026-11-03'))
    for a in actions:
        a['identifier_date_semantics']='First supported daily raw exchange identifier observation; not a depository settlement date. Economic membership uses the separately evidenced ex_date.'
        a['research_unit_basis_policy']='No lot quantity, FIFO basis or cash adjustment in existing adjusted research units; no double split/bonus/distribution credit'
    (target/'reviewed_lifecycle_decisions.json').write_text(json.dumps(actions,indent=2))
    transitions=pd.read_csv(OUT/'primary_review_transitions.csv')
    reviewed=transitions[transitions.status.eq('applicable')]
    checks=json.loads((OUT/'event_set_comparison.json').read_text())
    assert all(c['exact_match'] for c in checks)
    date_contexts=[]
    for stem,(date,note) in catalogue.items():
        text=(OUT/'extracted'/(stem+'.txt')).read_text(encoding='utf-8')
        day=pd.Timestamp(date)
        pattern=day.strftime('%B')+r'\s+0?'+str(day.day)+r',?\s+'+str(day.year)
        found=[m for m in re.finditer(pattern,text,re.I)]
        contexts=[text[max(0,m.start()-280):m.end()+90] for m in found]
        assert any(re.search(r'effective|w\.e\.f|effect from',c,re.I) for c in contexts),(stem,date)
        dp=r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s*(20\d{2})'
        announcement=re.search(dp,text)
        preceding=re.search(r'close\s+of\s+'+dp,text,re.I)
        exact=lambda m: str(pd.to_datetime(' '.join(m.groups()),format='%B %d %Y').date()) if m else None
        date_contexts.append(dict(source=stem+'.pdf',sha256=sha(OUT/'evidence'/(stem+'.pdf')),announcement_date=exact(announcement),effective_session=date,preceding_close_date=exact(preceding),retrieved_at='2026-11-03',review_note=note,contexts=contexts))
    (target/'reviewed_notice_date_contexts.json').write_text(json.dumps(date_contexts,indent=2))
    events=pd.read_csv(OLD/'event_identity_decisions.csv')
    events=events.rename(columns={c:'LEGACY_'+c for c in ['CONFIDENCE','MATCH_SCORE','RECONSTRUCTION_ALLOWED'] if c in events})
    events['EVENT_TIMING_STATUS']='verified_complete_primary_notice_set'
    events['EVENT_CHAIN_STATUS']='pending_independent_reconciliation'
    events['DAILY_RECONSTRUCTION_ACCEPTED']=False
    refs=[]
    for e in events.to_dict('records'):
        matching=reviewed[reviewed.effective_date.eq(e['EVENT_DATE'])&reviewed.symbol.eq(e['SYMBOL'])&reviewed.direction.eq(e['EVENT_TYPE'])]
        assert not matching.empty,e
        refs.append('|'.join(matching.source+':500:line'+matching.line.astype(str)))
    events['EVENT_TIMING_EVIDENCE']=refs
    monthly=pd.read_csv(OLD/'monthly_security_observations.csv',parse_dates=['date'])
    initial=set('ISIN:'+s for s in monthly.loc[monthly.date.eq('2017-06-30'),'ISIN'])
    anchor=set(pd.read_csv(OLD/'anchor_security_decisions.csv').SECURITY_ID)
    states,chain=walk_membership(initial,anchor,events,actions)
    comparisons=[]
    for day,g in monthly.groupby('date'):
        if not pd.Timestamp('2017-06-30')<=day<=pd.Timestamp('2020-08-31'):continue
        sample=pd.Timestamp('2017-07-01') if day==pd.Timestamp('2017-06-30') else day
        state=states[max(d for d in states if d<=sample)]
        truth=set('ISIN:'+s for s in g.ISIN)
        comparisons.append(dict(date=str(day.date()),matches=state==truth,observed_count=len(truth),reconstructed_count=len(state),observed_only='|'.join(sorted(truth-state)),reconstructed_only='|'.join(sorted(state-truth))))
    pd.DataFrame(comparisons).to_csv(target/'monthly_reconciliation.csv',index=False)
    chain['monthly_comparisons']=len(comparisons)
    chain['monthly_mismatches']=sum(not c['matches'] for c in comparisons)
    (target/'chain_reconciliation.json').write_text(json.dumps(chain,indent=2))
    economic_states,economic_chain=walk_membership(initial,anchor,events,actions,action_date_field='ex_date')
    economic_comparisons=[]
    projections=[]
    for day,g in monthly.groupby('date'):
        if not pd.Timestamp('2017-06-30')<=day<=pd.Timestamp('2020-08-31'):continue
        sample=max(day,pd.Timestamp('2017-07-01'))
        original=set('ISIN:'+s for s in g.ISIN)
        projected=project_raw_identifier_observation(original,sample,actions)
        actual=economic_states[max(d for d in economic_states if d<=sample)]
        economic_comparisons.append(dict(date=str(day.date()),matches=actual==projected,raw_source_changed=False,raw_only=sorted(original-projected),projected_only=sorted(projected-original)))
        if original!=projected:projections.append(economic_comparisons[-1])
    economic_chain.update(monthly_comparisons=len(economic_comparisons),monthly_mismatches=sum(not c['matches'] for c in economic_comparisons),observation_policy='Original raw ISIN rows retained; only evidenced ex-date/raw-identifier lags explicitly projected',raw_observation_projections=projections)
    (target/'economic_membership_reconciliation.json').write_text(json.dumps(economic_chain,indent=2))
    pd.DataFrame([dict(INDEX_NAME='NIFTY 500',TIME_STAMP=d,SECURITY_ID=s,IDENTITY_STATUS='verified',MEMBERSHIP_ID_POLICY='economic_ex_date',ACCEPTED=False) for d,state in economic_states.items() for s in sorted(state)]).to_parquet(target/'economic_provisional_membership.parquet',index=False)
    events['EVENT_CHAIN_STATUS']='reconciled_observed_endpoints_and_months' if all(chain[k] for k in ['forward_anchor_matches','reverse_initial_matches']) and not any(chain[k] for k in ['monthly_mismatches','forward_conflicts','reverse_conflicts']) else 'rejected_unreconciled'
    events.to_csv(target/'event_identity_and_timing_decisions.csv',index=False)
    pd.DataFrame([dict(INDEX_NAME='NIFTY 500',TIME_STAMP=d,SECURITY_ID=s,IDENTITY_STATUS='verified',EVENT_CHAIN_STATUS=events.EVENT_CHAIN_STATUS.iloc[0],CONFIDENCE='unaccepted_pending_price_and_lifecycle_model') for d,state in states.items() for s in sorted(state)]).to_parquet(target/'provisional_membership.parquet',index=False)
    manifest=dict(baseline_commit='6981092',scope=['2017-07-01','2020-09-01'],events=len(events),unique_transitions=len(events.drop_duplicates(['EVENT_DATE','EVENT_TYPE','SECURITY_ID'])),resolved_names=events.SCRIP_NAME.nunique(),event_dates=len(checks),complete_event_sets=all(c['exact_match'] for c in checks),lifecycle_decisions=len(actions),applicable_lifecycle_decisions=sum(a['applicable_to_scope'] for a in actions),chain=chain,accepted=False,publication='not attempted; adjusted price/entitlement/execution validation pending',primary_input_hashes={str(p.relative_to(ROOT)):sha(p) for p in paths},other_input_hashes={str(p.relative_to(ROOT)):sha(p) for p in [OUT/'action_date_evidence.json',OUT/'primary_review_transitions.csv',OLD/'event_identity_decisions.csv',OLD/'monthly_security_observations.csv',OLD/'anchor_security_decisions.csv']},code_hashes={str(p.relative_to(ROOT)):sha(p) for p in [Path(__file__),ROOT/'historical_reconciliation.py',ROOT/'historical_universe.py']})
    manifest['economic_membership_chain']=economic_chain
    (target/'manifest.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps({k:v for k,v in manifest.items() if 'hashes' not in k},indent=2))
