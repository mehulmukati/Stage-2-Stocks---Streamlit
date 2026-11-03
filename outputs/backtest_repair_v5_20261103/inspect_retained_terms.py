"""Extract the exact original bulletin terms for only the finite worklist."""
import hashlib
from pathlib import Path
import sys

import pandas as pd

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from historical_price_model import read_primary_action_bulletin

OUT=Path(__file__).parent
R4=ROOT/'outputs/backtest_repair_v4_20261103'
rows=pd.read_csv(OUT/'finite_entitlement_worklist.csv')
results=[]
for r in rows.to_dict('records'):
    day=pd.Timestamp(r['date'])
    p=R4/'evidence'/('PR'+day.strftime('%d%m%y')+'.zip')
    if not p.exists():continue
    frame=read_primary_action_bulletin(p)
    own=frame[frame.date.eq(day)&frame.SYMBOL.eq(r['dated_symbol'])]
    for b in own.to_dict('records'):
        results.append(dict(decision_id=r['decision_id'],symbol=r['dated_symbol'],date=r['date'],api_terms=r['subject'],bulletin_terms=b['PURPOSE'],company=b['SECURITY'],bulletin_row_id=b['source_row_id'],path=str(p.relative_to(ROOT)),sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
pd.DataFrame(results).to_csv(OUT/'retained_bulletin_terms.csv',index=False)
for r in results[::2]:print(r['symbol'],r['date'],r['bulletin_terms'])
