"""Read-only primary evidence acquisition; never writes accepted market data."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import sys
from urllib.parse import urljoin
from curl_cffi import requests
from bs4 import BeautifulSoup

OUT = Path(__file__).parent / 'evidence'
OUT.mkdir(parents=True, exist_ok=True)

def fetch(item):
    name, url = item
    path = OUT / name
    if path.exists():
        return dict(name=name, url=url, cached=True, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    record = dict(name=name, url=url, retrieved_at=datetime.now(timezone.utc).isoformat())
    try:
        r = requests.get(url, impersonate='chrome', timeout=40)
        record.update(status=r.status_code, bytes=len(r.content))
        if r.status_code == 200 and len(r.content) > 100:
            path.write_bytes(r.content)
            record['sha256'] = hashlib.sha256(r.content).hexdigest()
    except Exception as exc:
        record['error'] = str(exc)
    return record

if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'initial':
        import csv
        symbols = {r['symbol'] for r in csv.DictReader(open('outputs/backtest_repair_v2_20261103/bounded_candidate_v3/lifecycle_transition_worklist.csv'))}
        items = [('press_release.html', 'https://www.niftyindices.com/press-release')]
        items += [(s+'_msei.html', 'https://www.msei.in/corporates/corporate-securities-information/corporate-update/default?symbol='+s+'&type=4&vmode=vm') for s in sorted(symbols)]
        items += [('SIS_CMPT43159.pdf', 'https://nsearchives.nseindia.com/content/circulars/CMPT43159.pdf')]
    elif mode == 'reviews':
        soup = BeautifulSoup((OUT/'press_release.html').read_bytes(), 'html.parser')
        items = []
        for a in soup.find_all('a', href=True):
            text = a.get_text(' ', strip=True)
            href = a['href']
            if any(str(y) in text for y in range(2017,2021)) and any(s in text.lower() for s in ['change in indices','replacement','replacements','postpon','reschedul']):
                url = urljoin('https://www.niftyindices.com', href)
                if '.pdf' in url.lower():
                    name=url.rsplit('/',1)[-1]
                    items.append((name,url))
        items = sorted(set(items))
        (OUT.parent/'review_link_worklist.json').write_text(json.dumps(items,indent=2))
    else:
        items=json.loads(Path(mode).read_text())
    with ThreadPoolExecutor(max_workers=4) as pool:
        results=list(pool.map(fetch,items))
    log=OUT.parent/('retrieval_'+Path(mode).stem+'.json')
    log.write_text(json.dumps(results,indent=2))
    print(json.dumps(results,indent=2))
