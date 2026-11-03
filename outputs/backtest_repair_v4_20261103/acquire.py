"""Explicit read-only evidence acquisition; no accepted-data writes."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json,hashlib,sys
from curl_cffi import requests

OUT=Path(__file__).parent/'evidence';OUT.mkdir(parents=True,exist_ok=True)

def fetch(item):
    name,url=item;p=OUT/name
    if p.exists():return dict(name=name,url=url,cached=True,sha256=hashlib.sha256(p.read_bytes()).hexdigest())
    record=dict(name=name,url=url,retrieved_at=datetime.now(timezone.utc).isoformat())
    try:
        r=requests.get(url,impersonate='chrome',timeout=35)
        record.update(status=r.status_code,bytes=len(r.content),content_type=r.headers.get('content-type'))
        valid=r.status_code==200 and len(r.content)>20
        if name.endswith('.zip'):valid=valid and r.content.startswith(b'PK')
        if name.endswith('.pdf'):valid=valid and r.content.startswith(b'%PDF')
        if name.endswith('.json'):valid=valid and r.content.lstrip()[:1] in (b'{',b'[')
        if valid:p.write_bytes(r.content);record['sha256']=hashlib.sha256(r.content).hexdigest()
        else:
            failed=OUT/'failed_responses';failed.mkdir(exist_ok=True)
            (failed/(name+'.response')).write_bytes(r.content)
            record['response_sha256']=hashlib.sha256(r.content).hexdigest()
    except Exception as e:record['error']=str(e)
    return record

if __name__=='__main__':
    source=Path(sys.argv[1]);items=json.loads(source.read_text())
    with ThreadPoolExecutor(max_workers=4) as pool:records=list(pool.map(fetch,items))
    output=OUT.parent/('retrieval_'+source.stem+'.json')
    with output.open('x') as f:json.dump(records,f,indent=2)
    print(json.dumps(records,indent=2))
