from pathlib import Path
import json,pandas as pd
from security_identity import load_aliases,resolve_source_events
root=Path('outputs/backtest_repair_v2_20261103');p=root/'bounded_candidate_v3'
aliases=load_aliases(p/'reviewed_event_aliases.json')
source=pd.read_csv('outputs/backtest_repair_v1_20261102/candidate_universe_v5/source_events_with_decisions.csv')
source=source[source.INDEX_NAME.eq('NIFTY 500')&source.EVENT_DATE.between('2017-07-01','2020-09-01')]
resolved=resolve_source_events(source,aliases=aliases)
assert resolved.IDENTITY_STATUS.eq('verified').all()
actual=pd.read_csv(p/'event_identity_decisions.csv').set_index('SOURCE_ROW_ID')
for r in resolved.to_dict('records'):
 assert r['SECURITY_ID']==actual.loc[r['SOURCE_ROW_ID'],'SECURITY_ID']
 assert r['SYMBOL']==actual.loc[r['SOURCE_ROW_ID'],'SYMBOL']
(root/'identity_resolution_recheck.json').write_text(json.dumps(dict(passed=True,aliases=len(aliases),names=resolved.SCRIP_NAME.nunique(),events=len(resolved),date_policy='whitelisted verified effective dates only'),indent=2))
obs=pd.read_csv(p/'monthly_security_observations.csv');obs['date']=pd.to_datetime(obs.date)
rows=[]
for symbol,g in obs.groupby('symbol'):
 seq=g[['date','ISIN']].drop_duplicates().sort_values('date')
 for i in range(1,len(seq)):
  if seq.iloc[i].ISIN!=seq.iloc[i-1].ISIN:
   old,new=seq.iloc[i-1],seq.iloc[i]
   ev=actual[actual.SYMBOL.eq(symbol)]
   rows.append(dict(symbol=symbol,old_isin=old.ISIN,new_isin=new.ISIN,last_observed_old=str(old.date.date()),
       first_observed_new=str(new.date.date()),effective_date=None,share_ratio=None,decision='unresolved; dated action evidence required',
       affected_event_row_ids='|'.join(ev.index),affected_event_count=len(ev)))
pd.DataFrame(rows).to_csv(p/'lifecycle_transition_worklist.csv',index=False)
(root/'reviewed_lifecycle_decisions.json').write_text(json.dumps(dict(decisions=[dict(
 symbol='IRCON',issuer_id='CIN:L45203DL1976GOI008171',event_type='split',old_isin='INE962Y01013',new_isin='INE962Y01021',
 share_ratio=5.,announcement_date='2020-03-31',effective_date=None,effective_date_status='not established by this filing',
 source='https://www.ircon.org/sites/default/files/2025-05/64_0.pdf',artifact='evidence/ircon_20200331_new_isin.pdf',
 scope='identity lineage and ratio verified; no dated portfolio conversion or reconstructed-state change authorized')]),indent=2))
print('Identity recheck passed:',len(aliases),'aliases;',len(rows),'lifecycle transitions remain date-scoped work items')
