from pathlib import Path
import json,hashlib
from curl_cffi import requests
out=Path('outputs/backtest_repair_v2_20261103/evidence')
items={'nsdl_issuer_code_instructions.pdf':'https://nsdl.co.in/downloadables/pdf/19%20Circular%20Instructions%20for%20preparation%20of%20Upload%20File.pdf','ircon_20200331_new_isin.pdf':'https://www.ircon.org/sites/default/files/2025-05/64_0.pdf'}
results=[]
for name,url in items.items():
 try:
  r=requests.get(url,impersonate='chrome',timeout=25);d=dict(name=name,url=url,status=r.status_code,bytes=len(r.content),retrieved_at='2026-11-03')
  if r.status_code==200 and r.content.startswith(b'%PDF'):
   (out/name).write_bytes(r.content);d['sha256']=hashlib.sha256(r.content).hexdigest()
  results.append(d)
 except Exception as e:results.append(dict(name=name,url=url,error=str(e)))
(out/'retrieval_lifecycle.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results,indent=2))
