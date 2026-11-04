"""Targeted fallback using Windows certificate validation; never disables TLS."""
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

OUT=Path(__file__).parent
ITEMS=[
    ('CANBK_issuer.pdf','https://canarabank.bank.in/UploadedFiles/Pdf/canara-bank-rights-issue.pdf'),
    ('KARURVYSYA_offer.pdf','https://www.kvb.co.in/docs/rights-issue-17-letter-of-offer-rights-issue-2017.pdf'),
    ('KARURVYSYA_record.pdf','https://www.kvb.co.in/docs/rights-issue-17-fixation-of-record-date.pdf'),
    ('CMTR42403.pdf','https://nsearchives.nseindia.com/content/circulars/CMTR42403.pdf'),
    ('MSE8259.pdf','https://www.msei.in/SX-Content/Circulars/2019/October/Circular-8259.pdf'),
    ('FAOP42404.pdf','https://www.steelcitynettrade.com/Circulars/Muhurat%20Trading%20session%20on%20account%20of%20Diwali%202019_1.pdf'),
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

