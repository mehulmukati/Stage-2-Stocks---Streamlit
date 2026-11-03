"""Targeted fallback using Windows certificate validation; never disables TLS."""
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

OUT=Path(__file__).parent
ITEMS=[
    ('SRF_Q1_native_issuer.pdf','https://www.srf.com/wp-content/uploads/2020/11/Unaudited-Financial-Results-alongwith-Limited-Review-Report-for-the-Quarter-Ended-30-6-2017.pdf'),
    ('TECHNO_buyback_native_SEBI.pdf','https://www.sebi.gov.in/sebi_data/attachdocs/dec-2018/1545993505904.pdf'),
    ('ICICIBANK_prebonus_issuer.html','https://www.icici.bank.in/about-us/article/news-performance-review-quarter-ended-september-30-2017-20172710175602015'),
]


def fetch(item):
    name,url=item;path=OUT/'evidence'/name
    result=dict(name=name,url=url,retrieved_at=datetime.now(timezone.utc).isoformat(),transport='Windows curl/Schannel; certificate verification enabled')
    if path.exists():return {**result,'cached':True,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    process=subprocess.run(['curl.exe','--location','--fail','--max-time','150','--output',str(path),url],capture_output=True)
    result['return_code']=process.returncode
    if process.returncode==0 and (not name.endswith('.pdf') or path.read_bytes().startswith(b'%PDF')):
        result['sha256']=hashlib.sha256(path.read_bytes()).hexdigest();result['bytes']=path.stat().st_size
    else:
        result['error']=process.stderr.decode(errors='replace')
        if path.exists():
            failed=OUT/'evidence/failed_responses';failed.mkdir(exist_ok=True)
            target=failed/(name+'.partial');path.replace(target)
            result['partial_response_sha256']=hashlib.sha256(target.read_bytes()).hexdigest()
    return result


if __name__=='__main__':
    with ThreadPoolExecutor(max_workers=3) as pool:results=list(pool.map(fetch,ITEMS))
    with (OUT/'retrieval_native_documents.json').open('x') as f:json.dump(results,f,indent=2)
    print(json.dumps(results,indent=2))
