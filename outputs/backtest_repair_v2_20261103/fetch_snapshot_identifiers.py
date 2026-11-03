from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import json,hashlib
import pandas as pd
from curl_cffi import requests
out=Path('outputs/backtest_repair_v2_20261103/evidence')
items=[]
# NSE report observation dates are independently read from each dated primary
# PDF; fetch its exact observation day, not an inferred calendar month end.
from historical_universe import read_nifty500_archive
observations=[]
cache=out.parent/'parsed_monthly';cache.mkdir(exist_ok=True)
for f in sorted(out.glob('indices*.zip')):
 p=cache/(f.stem+'.parquet')
 if p.exists():d=pd.read_parquet(p)
 else:d=read_nifty500_archive(f);d.to_parquet(p,index=False)
 observations.append(d)
 for day in d.date.unique():
  t=pd.Timestamp(day);name='cm'+t.strftime('%d%b%Y').upper()+'bhav.csv.zip'
  items.append((name,'https://nsearchives.nseindia.com/content/historical/EQUITIES/'+t.strftime('%Y/%b').upper()+'/'+name))
pd.concat(observations,ignore_index=True).to_parquet(cache/'all_observations.parquet',index=False)
def fetch(item):
 name,url=item;p=out/name;rpt=dict(name=name,url=url,retrieved_at=datetime.now(timezone.utc).isoformat())
 try:
  if p.exists():rpt.update(cached=True,status=200,sha256=hashlib.sha256(p.read_bytes()).hexdigest());return rpt
  r=requests.get(url,impersonate='chrome',timeout=25);rpt.update(status=r.status_code,bytes=len(r.content))
  if r.status_code==200 and r.content[:2]==b'PK':p.write_bytes(r.content);rpt['sha256']=hashlib.sha256(r.content).hexdigest()
  else:rpt['rejected']=r.text[:80]
 except Exception as e:rpt['error']=str(e)
 print(json.dumps(rpt),flush=True);return rpt
with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(fetch,items))
(out/'retrieval_attempt_3.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
