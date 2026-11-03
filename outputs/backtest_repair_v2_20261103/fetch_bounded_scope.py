from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import json,hashlib,zipfile
from curl_cffi import requests
import pandas as pd
out=Path('outputs/backtest_repair_v2_20261103/evidence')
src=pd.read_csv('outputs/backtest_repair_v1_20261102/candidate_universe_v5/source_events_with_decisions.csv')
events=src[src.INDEX_NAME.eq('NIFTY 500')&src.EVENT_DATE.between('2017-07-01','2020-09-01')]
months=pd.period_range('2017-06','2020-09',freq='M')
items=[]
for m in months:
 name='indices_data'+m.start_time.strftime('%b%Y')+'.zip'
 items.append((name,'https://www.niftyindices.com/Indices_-_Market_Capitalisation_and_Weightage/'+name))
for d in sorted(events.EVENT_DATE.unique()):
 t=pd.Timestamp(d);name='cm'+t.strftime('%d%b%Y').upper()+'bhav.csv.zip'
 items.append((name,'https://nsearchives.nseindia.com/content/historical/EQUITIES/'+t.strftime('%Y/%b').upper()+'/'+name))
# Match the June anchor with its own primary daily security identifiers.
items.append(('cm30JUN2017bhav.csv.zip','https://nsearchives.nseindia.com/content/historical/EQUITIES/2017/JUN/cm30JUN2017bhav.csv.zip'))
def fetch(item):
 name,url=item;p=out/name
 result=dict(name=name,url=url,retrieved_at=datetime.now(timezone.utc).isoformat())
 try:
  if p.exists(): result.update(cached=True,status=200,sha256=hashlib.sha256(p.read_bytes()).hexdigest());return result
  r=requests.get(url,impersonate='chrome',timeout=25);result.update(status=r.status_code,bytes=len(r.content))
  if r.status_code==200 and r.content[:2]==b'PK':
   with p.open('xb') as f:f.write(r.content)
   result['sha256']=hashlib.sha256(r.content).hexdigest()
  else:result['rejected']=r.text[:80]
 except Exception as e:result['error']=str(e)
 print(json.dumps(result),flush=True);return result
with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(fetch,items))
(out/'retrieval_attempt_2.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
