"""Offline security-specific price construction and unresolved-action register."""
from pathlib import Path
import sys,json,hashlib,zipfile
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from historical_universe import read_bhavcopy
from historical_price_model import parse_primary_action,back_adjust_primary_quotes,MODEL_VERSION,read_primary_action_bulletin,corroborate_action_terms,select_primary_equity_actions
OUT=Path(__file__).parent;R3=ROOT/'outputs/backtest_repair_v3_20261103'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()

if __name__=='__main__':
    target=OUT/sys.argv[1];target.mkdir(exist_ok=False)
    base=pd.read_parquet(R3/'price_audit_v4/raw_security_quotes.parquet')
    seed_ids=set(base.ISIN);seed_symbols=set(base.SYMBOL)
    api=pd.DataFrame(json.loads((OUT/'evidence/nse_actions_2014_2020.json').read_text(encoding='utf-8')))
    api['date']=pd.to_datetime(api.exDate,format='%d-%b-%Y');api['source_row_id']=['nse_actions_2014_2020.json:'+str(i) for i in range(len(api))]
    api,excluded=select_primary_equity_actions(api)
    excluded.to_csv(target/'excluded_non_equity_action_records.csv',index=False)
    for source in [Path(__file__),ROOT/'historical_price_model.py']:
        (target/source.name).write_bytes(source.read_bytes())
    lookup_symbols=seed_symbols|set(api.loc[api['isin'].isin(seed_ids),'symbol'])
    possible_ids=seed_ids|set(api.loc[api.symbol.isin(lookup_symbols),'isin'])
    frames=[];inputs={};sessions=[]
    archives={p.name:p for folder in [ROOT/'outputs/backtest_repair_v2_20261103',R3,OUT] for p in (folder/'evidence').glob('cm*.zip')}
    for p in sorted(archives.values()):
        if not zipfile.is_zipfile(p):continue
        f=read_bhavcopy(p);day=f.date.iloc[0]
        if not pd.Timestamp('2014-01-01')<=day<=pd.Timestamp('2020-09-01'):continue
        if day<pd.Timestamp('2017-07-01'):sessions.append(str(day.date()))
        inputs[str(p.relative_to(ROOT))]=sha(p)
        f=f[f.SERIES.isin(['EQ','BE','BZ'])&(f.ISIN.isin(possible_ids)|f.SYMBOL.isin(lookup_symbols))].copy()
        f['raw_source_sha256']=sha(p);frames.append(f)
    raw=pd.concat(frames,ignore_index=True).sort_values(['date','SYMBOL','SERIES']).drop_duplicates(['date','SYMBOL','SERIES','ISIN'])
    raw.to_parquet(target/'raw_warmup_and_scope_quotes.parquet',index=False)
    # Build only individually evidenced split/bonus security-ID edges. Same
    # ticker and issuer prefixes alone never establish continuity.
    edges=json.loads((R3/'candidate_v3/reviewed_lifecycle_decisions.json').read_text())
    edges=[dict(old_isin=a['old_security_id'].removeprefix('ISIN:'),new_isin=a['new_security_id'].removeprefix('ISIN:'),date=a['ex_date'],kind=a['kind'],share_ratio=a['share_ratio'],evidence=a['boundary_evidence'],source=a['source']) for a in edges if a['applicable_to_scope']]
    parsed=[]
    for r in api.to_dict('records'):
        decision=parse_primary_action(r['subject']);r.update(decision);parsed.append(r)
    actions=pd.DataFrame(parsed)
    grouped={symbol:g for symbol,g in raw.groupby('SYMBOL')}
    by_id={isin:g for isin,g in raw.groupby('ISIN')}
    bulletins=[]
    for p in sorted((OUT/'evidence').glob('PR*.zip')):
        try:b=read_primary_action_bulletin(p)
        except ValueError:continue
        b['archive_sha256']=sha(p);bulletins.append(b)
        inputs[str(p.relative_to(ROOT))]=sha(p)
    bulletin_frame=pd.concat(bulletins,ignore_index=True) if bulletins else pd.DataFrame()
    for a in parsed:
        if a['kind'] not in ['split','bonus','split_and_bonus'] or not a['supported']:continue
        symbol=None;bulletin_refs=[]
        if a['isin'] in by_id:
            own=by_id[a['isin']];near_id=own[own.date.ge(a['date']-pd.Timedelta(days=3))&own.date.le(a['date']+pd.Timedelta(days=10))]
            symbols=set(near_id.SYMBOL)
            if len(symbols)==1:symbol=next(iter(symbols))
        if symbol is None and a['symbol'] in grouped:
            proposed={**a,'historical_symbol':a['symbol']}
            candidates=bulletin_frame[bulletin_frame.date.eq(a['date'])&bulletin_frame.SYMBOL.eq(a['symbol'])]
            bulletin_refs=corroborate_action_terms(proposed,candidates.to_dict('records'))
            if bulletin_refs:symbol=a['symbol']
        if symbol is None:continue
        near=grouped[symbol]
        following=near[near.date.ge(a['date']-pd.Timedelta(days=3))&near.date.le(a['date']+pd.Timedelta(days=10))]
        ids=set(following.ISIN)
        if len(ids)==2 and (a['isin'] in ids or bulletin_refs):
            ordered=following.groupby('ISIN').date.agg(['min','max']).sort_values('min')
            old,new=ordered.index
            if ordered.loc[old,'max']<ordered.loc[new,'min']:
                edges.append(dict(old_isin=old,new_isin=new,date=str(a['date'].date()),kind=a['kind'],share_ratio=a['share_ratio'],evidence=[a['source_row_id'],*bulletin_refs,*following.source_row_id],source='primary NSE action plus adjacent dated exchange identifiers; obsolete API ISIN requires original bulletin corroboration'))
    parent={s:s for s in set(raw.ISIN)}
    def find(x):
        while parent[x]!=x:parent[x]=parent[parent[x]];x=parent[x]
        return x
    for edge in edges:
        old,new=edge['old_isin'],edge['new_isin']
        if old in parent and new in parent:parent[find(old)]=find(new)
    family={s:find(s) for s in parent};valid_families={family[s] for s in seed_ids}
    raw['research_security_id']=raw.ISIN.map(family)
    raw=raw[raw.research_security_id.isin(valid_families)]
    actions['attribution_family']=actions['isin'].map(family)
    actions['attribution_status']='dated_identifier_or_individually_evidenced_lineage'
    identity_reviews=[]
    # Keep apparently relevant actions with conflicting/restated API identifiers
    # visible. Do not silently lose them, or force an issuer-prefix identity.
    for i,a in actions[~actions.attribution_family.isin(valid_families)&actions.symbol.isin(lookup_symbols)].iterrows():
        actual=raw[raw.SYMBOL.eq(a.symbol)&raw.date.eq(a.date)]
        families=set(actual.research_security_id)
        if len(families)==1:
            actions.loc[i,'attribution_family']=next(iter(families))
            actions.loc[i,'attribution_status']='API identifier conflict; dated symbol requires corroboration'
            if a.kind!='no_mandatory_holder_distribution':
                proposed={**a.to_dict(),'historical_symbol':a.symbol}
                candidates=bulletin_frame[bulletin_frame.date.eq(a.date)&bulletin_frame.SYMBOL.eq(a.symbol)] if not bulletin_frame.empty else pd.DataFrame()
                refs=corroborate_action_terms(proposed,candidates.to_dict('records')) if not candidates.empty else []
                if refs:
                    actions.loc[i,'attribution_status']='verified original dated bulletin and raw quote; conflicting API ISIN retained'
                    actions.loc[i,'attribution_evidence']='|'.join(refs)
                    identity_reviews.append(dict(api_row_id=a.source_row_id,api_isin=a['isin'],date=str(a.date.date()),historical_symbol=a.symbol,dated_isins=sorted(set(actual.ISIN)),bulletin_rows=refs,raw_quote_rows=list(actual.source_row_id),decision='verified action attribution; API identifier not asserted historical'))
                else:
                    actions.loc[i,'supported']=False;actions.loc[i,'reason']='Primary API identifier conflicts with dated quote; corporate-action identity requires corroboration'
    (target/'reviewed_action_identity_conflicts.json').write_text(json.dumps(identity_reviews,indent=2))
    actions[~actions.attribution_family.isin(valid_families)&actions.symbol.isin(lookup_symbols)].to_csv(target/'unattributed_primary_action_records.csv',index=False)
    action_rows=[];prices=[];conflicts=[]
    for family_id,g in raw.groupby('research_security_id'):
        g=g[g.SERIES.isin(['EQ','BE','BZ'])].sort_values(['date','SERIES'])
        if g.date.duplicated().any():
            conflicts.extend(g[g.date.duplicated(False)].to_dict('records'));continue
        g=g.set_index('date');own_ids=set(g.ISIN)
        own=actions[actions.attribution_family.eq(family_id)].to_dict('records')
        # Reuse Round 3's explicitly reviewed Avanti combined split/bonus ratio.
        for a in own:
            if a['date']==pd.Timestamp('2018-06-26') and a['isin']=='INE871C01020' and a['kind']=='combined_action_requires_review':
                a.update(kind='reviewed_split_and_bonus',supported=True,share_ratio=3.0,cash=0.0,reviewed_source='Round 3 AVANTIFEED primary MSEI decision')
            if a['date']==pd.Timestamp('2019-03-29') and a['isin']=='INE669E01016':
                proof=OUT/'evidence/IDEA_rights_issuer_20190320.pdf'
                assert proof.exists()
                a.update(kind='reviewed_fully_paid_rights',supported=True,share_ratio=1.0,cash=0.0,rights_ratio=87/38,subscription_price=12.5,reviewed_source=str(proof.relative_to(ROOT)),reviewed_source_sha256=sha(proof),entitlement_policy='Theoretical fully-paid ex-rights research normalization; no additional subscription cash or right-sale credit; not nominal execution')
            a['date']=str(a['date'].date())
        result,decisions=back_adjust_primary_quotes(g,own)
        result['research_security_id']=family_id
        prices.append(result.reset_index())
        for a in decisions:a['research_security_id']=family_id;action_rows.append(a)
    pd.concat(prices,ignore_index=True).to_parquet(target/'primary_adjusted_research_candidate.parquet',index=False)
    pd.DataFrame(action_rows).to_csv(target/'action_decisions.csv',index=False)
    unresolved=pd.DataFrame(action_rows);unresolved=unresolved[~unresolved.applied & unresolved.reason.ne('no mandatory holder adjustment')]
    unresolved.to_csv(target/'unresolved_action_decisions.csv',index=False)
    (target/'identity_edges.json').write_text(json.dumps(edges,indent=2,default=str))
    (target/'identity_families.json').write_text(json.dumps(family,indent=2))
    pd.DataFrame(conflicts).to_csv(target/'ambiguous_daily_security_quotes.csv',index=False)
    summary=dict(model=MODEL_VERSION,raw_rows=len(raw),warmup_sessions=len(set(sessions)),warmup_first=min(sessions),warmup_last=max(sessions),seed_security_ids=len(seed_ids),research_lineages=len(valid_families),lineage_edges=len(edges),action_decisions=len(action_rows),unresolved_actions=len(unresolved),ambiguous_quote_rows=len(conflicts),accepted=False,source_hashes={str(p.relative_to(ROOT)):sha(p) for p in [Path(__file__),ROOT/'historical_price_model.py',OUT/'evidence/nse_actions_2014_2020.json']},warmup_input_hashes=inputs)
    (target/'manifest.json').write_text(json.dumps(summary,indent=2));print(json.dumps({k:v for k,v in summary.items() if 'hashes' not in k},indent=2))
