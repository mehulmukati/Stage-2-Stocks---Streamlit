from pathlib import Path
import sys,json,hashlib
import pandas as pd
ROOT=Path(__file__).resolve().parents[2];OUT=ROOT/'outputs/backtest_repair_v4_20261103'
sys.path.insert(0,str(ROOT));sys.path.append(str(ROOT/'.codex-runtime'))
import pyarrow as pa
pa._hotfix_installed=True
import market_data as md
from equity_data import session_frame
from backtest_engine import BacktestConfig
from scripts.reconcile_backtest_ledger import reconcile
base=ROOT/'outputs/backtest_repair_v2_20261103';folder=base/'accounting_diagnostic'
manifest=json.loads((folder/'manifest.json').read_text());cfg=BacktestConfig(**manifest['config'])
r={k:json.loads((folder/f'{k}.json').read_text()) for k in ['execution_ledgers','daily_ledgers','closed_lots','tax_events']}
symbols={t['symbol'] for book in r['execution_ledgers'].values() for t in book}
cal=pd.DatetimeIndex(pd.to_datetime([d['date'] for d in r['daily_ledgers']['Full Rebalance']]))
print('Check actual accepted source quotes',len(symbols),'symbols',len(cal),'sessions',flush=True)
snapshot=md.load_snapshot('2026-10-27');frames=snapshot.ohlcv(symbols=symbols,as_of='2026-10-27')
accepted={s:session_frame(f,cal) for s,f in frames.items()}
checks=reconcile(r,accepted,cfg)
baseline=json.loads((base/'baseline_manifest.json').read_text())
protected={}
for item in baseline['files']:
    name=item['path'].replace('\\','/')
    if name in ['data/screener_ohlcv.parquet','data/constituents.parquet','data/scrip_symbol_map.csv','data/compositions.parquet','batch_v10.xlsx']:
        protected[name]=hashlib.sha256((ROOT/name).read_bytes()).hexdigest().lower()==item['sha256'].lower()
report={'source_revisions':snapshot.revisions,'reconciliation':checks,'protected_baseline_inputs':protected,'date_range':[str(cal[0].date()),str(cal[-1].date())]}
assert all(c['passed'] for c in checks.values());assert len(protected)==5 and all(protected.values())
destination=Path(sys.argv[1]) if len(sys.argv)>1 else OUT/'export_recheck.json'
with destination.open('x') as f:json.dump(report,f,indent=2,default=str)
print(json.dumps(report,indent=2,default=str),flush=True)
