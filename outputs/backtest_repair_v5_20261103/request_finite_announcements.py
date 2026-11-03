"""Exact unresolved action windows; no new market-price collection."""
import json
from pathlib import Path
from urllib.parse import quote

import pandas as pd

OUT=Path(__file__).parent
work=pd.read_csv(OUT/'finite_entitlement_worklist.csv')
selected=work[work.kind.isin(['unsupported_entitlement','combined_action_requires_review'])|work.symbol.eq('SRF')]
items=[];basis=[]
for r in selected.to_dict('records'):
    day=pd.Timestamp(r['date']);start=day-pd.Timedelta(days=90);end=day+pd.Timedelta(days=45)
    symbol=r['symbol'];name=symbol+'_'+day.strftime('%Y%m%d')+'_announcements.json'
    url='https://www.nseindia.com/api/corporate-announcements?index=equities&symbol='+quote(symbol,safe='')+'&from_date='+start.strftime('%d-%m-%Y')+'&to_date='+end.strftime('%d-%m-%Y')
    items.append([name,url]);basis.append(dict(decision_id=r['decision_id'],dated_symbol=r['dated_symbol'],query_symbol=symbol,period_start=str(start.date()),period_end=str(end.date()),file=name,identity_policy='Query symbol is not a historical identity decision; each retained attachment must bind the dated source security'))
for symbol in ['TECHNO','TECHNOE']:
    items.append([symbol+'_ownership_announcements.json','https://www.nseindia.com/api/corporate-announcements?index=equities&symbol='+symbol+'&from_date=01-06-2018&to_date=31-12-2018'])
(OUT/'finite_announcements_urls.json').write_text(json.dumps(items,indent=2))
pd.DataFrame(basis).to_csv(OUT/'finite_announcement_request_basis.csv',index=False)
print('Targeted finite windows:',len(items))
