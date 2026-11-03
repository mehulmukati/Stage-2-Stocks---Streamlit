from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,hashlib,datetime
from curl_cffi import requests
out=Path('outputs/backtest_repair_v2_20261103/evidence');out.mkdir(exist_ok=True)
urls={
'indices_dataSep2020.zip':'https://www.niftyindices.com/Indices_-_Market_Capitalisation_and_Weightage/indices_dataSep2020.zip',
'indices_dataJun2017.zip':'https://www.niftyindices.com/Indices_-_Market_Capitalisation_and_Weightage/indices_dataJun2017.zip',
'indices_dataAug2020.zip':'https://www.niftyindices.com/Indices_-_Market_Capitalisation_and_Weightage/indices_dataAug2020.zip',
'cm29SEP2017bhav.csv.zip':'https://nsearchives.nseindia.com/content/historical/EQUITIES/2017/SEP/cm29SEP2017bhav.csv.zip',
'cm31AUG2020bhav.csv.zip':'https://nsearchives.nseindia.com/content/historical/EQUITIES/2020/AUG/cm31AUG2020bhav.csv.zip',
}
def fetch(item):
    name,url=item
    try:
        r=requests.get(url,impersonate='chrome',timeout=25)
        result=dict(name=name,url=url,status=r.status_code,bytes=len(r.content),retrieved_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
        if r.status_code==200 and r.content[:2]==b'PK':
            (out/name).write_bytes(r.content);result['sha256']=hashlib.sha256(r.content).hexdigest()
        else: result['rejected_content_prefix']=r.text[:100]
    except Exception as e: result=dict(name=name,url=url,error=str(e))
    print(json.dumps(result),flush=True);return result
with ThreadPoolExecutor(max_workers=3) as pool: results=list(pool.map(fetch,urls.items()))
(out/'retrieval_attempt_1.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
