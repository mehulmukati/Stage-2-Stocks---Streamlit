"""Targeted fallback using Windows certificate validation; never disables TLS."""
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

OUT=Path(__file__).parent
ITEMS=[
    ('PVR_offer.pdf','https://www.sebi.gov.in/sebi_data/attachdocs/jul-2020/1594192456627.pdf'),
    ('CANBK_BSE_offer.pdf','https://www.bseindia.com/downloads/ipo/finalletterofoffercanarabankclean_160220171440.pdf'),
    ('ARVINDFASN_final_terms.pdf','https://www.arvindfashions.com/wp-content/uploads/2020/06/FAQ.pdf'),
    ('RELIANCE_calls.pdf','https://www.ril.com/sites/default/files/2023-01/UpdateonRI17052020.pdf'),
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
    with (OUT/'retrieval_additional.json').open('x') as f:json.dump(results,f,indent=2)
    print(json.dumps(results,indent=2))


