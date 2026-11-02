"""One frozen legacy configuration; not a repaired-universe strategy comparison."""
from pathlib import Path
import dataclasses
import json
import sys

ROOT=Path(__file__).resolve().parents[2]
OUT=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parent/'accounting_diagnostic'
sys.path.insert(0,str(ROOT))
import market_data as md
import run_batch_v8 as batch
from backtest_engine import BacktestConfig,run_backtest
from backtest_provenance import write_manifest_exclusive
from equity_data import session_frame
from scripts.reconcile_backtest_ledger import reconcile

OUT.mkdir(exist_ok=False)
snapshot=md.load_snapshot('2026-10-27')
membership=snapshot.membership
keys={md.index_key(i) for i in batch.ALL_5_INDICES}
symbols=set(membership.loc[membership.INDEX_NAME.isin(keys),'SYMBOL'])
frames=snapshot.ohlcv(symbols=symbols,as_of='2026-10-27')
bench=snapshot.series('index_price',['Nifty 50','Nifty 100','Nifty 500'])
cfg=BacktestConfig(m=15,n=30,rebalance_freq='weekly',sort_method='Average of 3/6/9/12 months',
    start_date='2017-07-01',end_date='2026-10-27',compositions_df=membership,index_names=batch.ALL_5_INDICES,
    min_history_days=252,apply_volume_filter=True,transaction_cost_pct=.001,stcg_rate=.2,ltcg_rate=.125,
    initial_capital=1_000_000,band_rule='classic',stage2_drop_exit=False,max_position_pct=None,
    min_annual_return=7.,pct_from_52w_high=25.,max_circuits=18,close_above_200dma=True,
    pos_days_3m_min=45.,pos_days_6m_min=45.,pos_days_12m_min=45.)
print('RUN accounting diagnostic',snapshot.revisions,flush=True)
result=run_backtest(frames,bench,cfg)
assert 'error' not in result,result.get('error')
write_manifest_exclusive(OUT/'manifest.json',result['run_manifest'])
result['nav'].to_csv(OUT/'nav.csv')
result['stats'].to_csv(OUT/'stats.csv')
for key in ['execution_ledgers','daily_ledgers','closed_lots','fill_failures','tax_events','holdings_log','coverage_report','accounting_totals']:
    with (OUT/f'{key}.json').open('x',encoding='utf-8') as f:
        json.dump(result[key],f,default=str)
print('RECONCILE original accepted quotes and independently rebuilt FIFO/cash/tax',flush=True)
calendar=result['trading_days']
accepted={s:session_frame(f,calendar) for s,f in frames.items()}
checks=reconcile(result,accepted,cfg)
write_manifest_exclusive(OUT/'independent_reconciliation.json',checks)
print(json.dumps(checks,indent=2),flush=True)
print(result['stats'].to_string(),flush=True)
