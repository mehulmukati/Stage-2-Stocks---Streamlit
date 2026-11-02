from pathlib import Path
import sys,json,hashlib,time
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2];ORIGINAL_OUT=Path(__file__).resolve().parent
OUT=Path(sys.argv[1]) if len(sys.argv)>1 else ORIGINAL_OUT/'baseline_reproduction_rerun'
OUT.mkdir(parents=True,exist_ok=False)
sys.path.insert(0,str(ROOT));sys.path.append(str(ROOT/'.codex-runtime'));sys.path.append(str(ROOT/'outputs/momo_review_20261102/reader_dependencies'))
import pyarrow as pa
pa._hotfix_installed=True
import importlib.util
spec=importlib.util.spec_from_file_location('backtest_engine',ORIGINAL_OUT/'backtest_engine_baseline.py')
engine=importlib.util.module_from_spec(spec)
sys.modules['backtest_engine']=engine
spec.loader.exec_module(engine)
import market_data,run_batch_v8 as batch
print('LOAD APP SNAPSHOT',flush=True)
snap=market_data.load_snapshot();membership=snap.membership
keys=[engine._canonical_index_name(i) for i in batch.ALL_5_INDICES]
symbols=set(membership.loc[membership.INDEX_NAME.map(engine._canonical_index_name).isin(keys),'SYMBOL'])
data=snap.ohlcv(symbols=symbols,as_of='2026-10-27')
bench=snap.series('index_price',['Nifty 50','Nifty 100','Nifty 500'])
batch.END_DATE='2026-10-27'
print('RUN APP',len(data),snap.revisions,flush=True)
r=batch.run_one(data,membership,bench,'Average of 3/6/9/12 months',0,15,30,'classic',0)
assert 'error' not in r,r.get('error')
r['nav'].to_csv(OUT/'app_batch_N15_N30_nav.csv')
r['stats'].to_csv(OUT/'app_batch_N15_N30_stats.csv')
print('RESULT',r['stats'].to_string(),flush=True)
x=pd.read_excel(ROOT/'batch_v10.xlsx')
sel=x[(x.M==15)&(x.N==30)&(x.Band=='classic')&(x.Stage2DropThreshold==0)&(x.MaxPositionPct==0)&(x.SortMethod=='Average of 3/6/9/12 months')]
assert len(sel)==3, 'Require three actual workbook matches before verification'
print('SAVED',sel.to_string(index=False),flush=True)
for row in sel.to_dict('records'):
    print('CAGR delta',row['Variant'],r['stats'].loc[row['Variant'],'CAGR (%)']-row['CAGR (%)'])
    print('NAV delta',row['Variant'],r['stats'].loc[row['Variant'],'Final NAV']-row['Final NAV'])
print('BATCH REPRODUCED',flush=True)
meta={'source_revisions':snap.revisions,'engine_sha256':hashlib.sha256((ORIGINAL_OUT/'backtest_engine_baseline.py').read_bytes()).hexdigest(),'return_keys':list(r)}
(OUT/'app_reproduction.json').write_text(json.dumps(meta,indent=2))
for key,value in r.items():
    if isinstance(value,pd.DataFrame) and key!='nav' and key!='stats':value.to_csv(OUT/f'app_{key}.csv',index=False)
    if isinstance(value,list) and value and isinstance(value[0],dict):
        (OUT/f'app_{key}.json').write_text(json.dumps(value,default=str))
