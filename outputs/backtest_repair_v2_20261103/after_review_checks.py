"""Independent post-implementation probes; production files are read only."""
from pathlib import Path
import sys,json,copy,tempfile,hashlib
from unittest.mock import patch
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2];OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT));sys.path.append(str(ROOT/'.codex-runtime'))
import pyarrow as pa
pa._hotfix_installed=True
import backtest_engine as e
import market_data as md
from backtest_accounting import QuantityLedger
from scripts.publish_reconstructed_membership import publish
from backtest_provenance import sha256
from security_identity import REGISTRY
reports={}
days=pd.bdate_range('2024-07-01',periods=30)
def frame(close):
    c=pd.Series(close,index=days,dtype=float)
    return pd.DataFrame(dict(Open=c,High=c,Low=c,Close=c,Volume=1e6),index=days)
def simulate(frames,events,rank,m=1,n=3,**kw):
    cfg=e.BacktestConfig(m=m,n=n,rebalance_freq='weekly',sort_method='3 months',start_date=str(days[0].date()),end_date=str(days[-1].date()),market_calendar=days,min_history_days=1,apply_volume_filter=False,transaction_cost_pct=kw.pop('cost',0),corporate_actions=[],**kw)
    with patch.object(e,'get_rebalance_dates',return_value=[days[i] for i in events]),patch.object(e,'rank_universe_at_date',side_effect=lambda _,day,*a,**k:(rank(days.get_loc(day)),{})):
        r=e.run_backtest(frames,{},cfg)
    assert 'error' not in r,r.get('error')
    return r

# Independent repeats of original behavioral oracles through the public engine.
r=simulate({'A':frame([100]*3+[np.nan]+[200]*26)},[3],lambda _:['A'])
reports['original_missing_buy_fixed']={'ending_nav':r['nav'].iloc[-1,0],'passed':abs(r['nav'].iloc[-1,0]-100)<1e-8,'trades':len(r['execution_ledgers']['Full Rebalance'])}
reports['average_holdings_after_failed_buy']={'reported':float(r['stats'].loc['Full Rebalance','Avg Holdings']),'actual_held_every_day':0,'passed':float(r['stats'].loc['Full Rebalance','Avg Holdings'])==0}
r=simulate({'A':frame([100]*10+[np.nan]+[200]*19)},[3,10],lambda i:['A'] if i<9 else [])
reports['original_missing_sell_fixed']={'ending_nav':r['nav'].iloc[-1,0],'passed':abs(r['nav'].iloc[-1,0]-200)<1e-8,'held':r['holdings_log'][-1]['holdings']}
r=simulate({'A':frame([100]*4+[np.nan,200,400]+[400]*23),'B':frame([100]*4+[200]*26)},[3],lambda _:['A','B'],m=2)
reports['original_gap_fixed']={'ending_nav':r['nav'].iloc[-1,0],'passed':abs(r['nav'].iloc[-1,0]-300)<1e-8}
r=simulate({'A':frame([100]*4+[200]*26)},[3,10],lambda i:['A'] if i<9 else [],cost=.001)
reports['original_exit_cost_and_quantity_fixed']={'ending_nav':r['nav'].iloc[-1,0],'passed':abs(r['nav'].iloc[-1,0]-199.6002)<1e-8,'remaining':r['daily_ledgers']['Full Rebalance'][-1]['quantities']}
r=simulate({'A':frame([100]*4+[200]*26),'B':frame([100]*4+[200]*26)},[3],lambda _:['A','B'],m=2,max_position_pct=20)
reports['original_infeasible_cap_fixed']={'ending_nav':r['nav'].iloc[-1,0],'cash':r['daily_ledgers']['Full Rebalance'][-1]['cash'],'passed':abs(r['nav'].iloc[-1,0]-140)<1e-8}

# Missing exits must also occupy actual displacement slots.
r=simulate({'A':frame([100]*10+[np.nan]+[100]*19),'B':frame([100]*30),'C':frame([100]*30),'D':frame([100]*30)},[3,10],lambda i:['A','B'] if i<9 else ['C','D'],m=2,band_rule='displacement')
event=r['holdings_log'][-1]
reports['blocked_exit_position_count']={'requested_M':2,'holdings':event['holdings_by_variant'],'passed':all(len(v)<=2 for v in event['holdings_by_variant'].values()),'fill_failures':event['fill_failures']}

# A feasible cap after an ordinary taxable trim, with every quote executable.
ledger=QuantityLedger(cash=0,lots={'A':[dict(date=days[0],price=100.0,shares=1.0)]},marks={'A':200.0},mark_dates={'A':days[10]})
ledger.rebalance(days[10],{'A':.5,'B':.5},{'A':200.0,'B':100.0},.001,0,e._compute_fy_tax,.2,.125)
reports['ordinary_taxable_cap']={'requested_cap':.5,'actual_weights':ledger.weights(),'cash':ledger.cash,'tax_accrued':ledger.accrued,'passed':max(ledger.weights().values())<=.5+1e-8,'failures':ledger.failures}
r=simulate({'A':frame([100]*4+[200]*26),'B':frame([100]*30)},[3,10],lambda i:['A'] if i<9 else ['A','B'],m=2,max_position_pct=50,stcg_rate=.2,ltcg_rate=.125,cost=.001)
event=r['holdings_log'][-1]
reports['engine_ordinary_taxable_cap']={'requested_cap_pct':50,'actual_weights':event['full_weights'],'cap_violations':event['cap_violations'],'passed':max(event['full_weights'].values())<=50+1e-6,'failures':event['fill_failures']}

# Independent portfolio states must not be merged for displacement selection.
def initialized(symbol):
    return QuantityLedger(cash=0,lots={symbol:[dict(date=days[0],price=100.0,shares=1.0)]},marks={symbol:100.0},mark_dates={symbol:days[0]})
states=[initialized('A'),initialized('B'),initialized('A')]
with patch.object(e,'QuantityLedger',side_effect=states):
    r=simulate({'A':frame([100]*30),'B':frame([100]*30)},[3],lambda _:['A','B'],m=1,band_rule='displacement')
event=r['holdings_log'][-1]
reports['independent_variant_states']={'initial_full':['A'],'initial_marginal':['B'],'ranking':['A','B'],'M':1,'N':3,'result':event['holdings_by_variant'],'expected_full':['A'],'expected_marginal':['B'],'passed':event['holdings_by_variant']['Full Rebalance']==['A'] and event['holdings_by_variant']['Marginal Rebalance']==['B']}

# Publication must not change membership outside its approved effective interval.
with tempfile.TemporaryDirectory(dir=OUT,prefix='publication_probe_') as directory:
    tmp=Path(directory);price=tmp/'data/screener_ohlcv.parquet';member=tmp/'data/constituents.parquet'
    with patch.object(md,'PRICE_PATH',price),patch.object(md,'MEMBERSHIP_PATH',member):
        md.write_source(pd.DataFrame(dict(symbol=['A'],date=pd.to_datetime(['2025-01-02']),Open=[100.],High=[100.],Low=[100.],Close=[100.],Volume=[1000.],series_type=['equity'])),price)
        history=pd.DataFrame(dict(INDEX_NAME=['X','X'],TIME_STAMP=pd.to_datetime(['2025-01-01','2025-01-20']),SYMBOL=['OLD','AFTER']))
        md.write_source(history,member)
        cand=pd.DataFrame(dict(INDEX_NAME=['X','X'],TIME_STAMP=pd.to_datetime(['2025-01-05','2025-01-10']),SYMBOL=['NEW','NEW'],SECURITY_ID=['TEST:NEW']*2,CONFIDENCE=['verified']*2))
        cp=tmp/'candidate.parquet';op=tmp/'observed.parquet';certp=tmp/'certificate.json'
        cand.to_parquet(cp,index=False);cand.to_parquet(op,index=False)
        rev=md.source_revisions()
        cert=dict(accepted=True,reviewer='isolated reassessment fixture',candidate_sha256=sha256(cp),identity_registry_sha256=sha256(REGISTRY),price_revision=rev[0],membership_revision=rev[1],validated_intervals=[dict(index='X',start='2025-01-05',end='2025-01-10')],observed_snapshots_path=str(op),observed_snapshots_sha256=sha256(op))
        certp.write_text(json.dumps(cert))
        before=md.constituents_at(history,pd.Timestamp('2025-01-13'),['X'])
        publish(cp,certp,tmp/'backup')
        after=md.constituents_at(md.load_snapshot().membership,pd.Timestamp('2025-01-13'),['X'])
        reports['publication_interval_isolation']={'reviewed_interval':['2025-01-05','2025-01-10'],'outside_date':'2025-01-13','before':before,'after':after,'passed':before==after}

# Source/delivery checks independently of narrative reports.
base=ROOT/'outputs/backtest_repair_v1_20261102'
manifest=json.loads((base/'baseline_manifest.json').read_text())
reports['baseline_manifest_top_keys']=list(manifest)
aliases=json.loads(REGISTRY.read_text())
decisions=pd.read_csv(base/'candidate_universe_v5/source_events_with_decisions.csv')
reports['identity_work_completed']={'alias_records':len(aliases['aliases']),'verified_event_rows':int(decisions.IDENTITY_STATUS.eq('verified').sum()),'event_rows':len(decisions),'unresolved_event_rows':int(decisions.IDENTITY_STATUS.eq('unresolved').sum())}
events=json.loads((base/'accounting_diagnostic_v2/holdings_log.json').read_text())
reports['existing_diagnostic_variant_divergence']={'events':len(events),'different_holdings_events':sum(len({tuple(v) for v in ev['holdings_by_variant'].values()})>1 for ev in events),'cap_violations':sum(any(ev.get('cap_violations',{}).values()) for ev in events)}
def encode(value):
    return value.item() if isinstance(value,np.generic) else str(value)
(OUT/'review_checks.json').write_text(json.dumps(reports,indent=2,default=encode))
print(json.dumps(reports,indent=2,default=encode),flush=True)
