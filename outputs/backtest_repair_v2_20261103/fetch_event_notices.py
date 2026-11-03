from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,hashlib
from curl_cffi import requests
out=Path('outputs/backtest_repair_v2_20261103/evidence')
names=['ind_prs29082017.pdf','ind_prs17092019.pdf','ind_prs10062020.pdf']
def fetch(name):
 u='https://www.niftyindices.com/Press_Release/'+name
 try:
  r=requests.get(u,impersonate='chrome',timeout=25);d=dict(name=name,url=u,status=r.status_code,retrieved_at='2026-11-03')
  if r.status_code==200 and r.content.startswith(b'%PDF'):(out/name).write_bytes(r.content);d['sha256']=hashlib.sha256(r.content).hexdigest()
  return d
 except Exception as e:return dict(name=name,url=u,error=str(e))
with ThreadPoolExecutor(max_workers=3) as pool:results=list(pool.map(fetch,names))
(out/'retrieval_event_notices.json').write_text(json.dumps(results,indent=2));print(json.dumps(results,indent=2))
