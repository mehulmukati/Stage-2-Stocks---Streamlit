from pathlib import Path
import sys,json,hashlib
import pandas as pd
from pypdf import PdfReader
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from historical_universe import parse_primary_holidays
import market_data as md
OUT=Path(__file__).parent
if __name__=='__main__':
    closed=set();special=set();evidence=[]
    for year,name in [(2017,'CMTR33746'),(2018,'CMTR36475'),(2019,'CMTR39612'),(2020,'CMTR42877')]:
        p=OUT/'evidence'/(name+'.pdf');text='\n'.join(page.extract_text(extraction_mode='layout') for page in PdfReader(p).pages)
        c,s=parse_primary_holidays(text,year);closed|=c;special|=s
        evidence.append(dict(path=str(p.relative_to(ROOT)),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),closed=sorted(str(d.date()) for d in c),muhurat=sorted(str(d.date()) for d in s)))
    for name,date in [('CMTR40536','2019-04-29'),('MSD42319','2019-10-21')]:
        p=OUT/'evidence'/(name+'.pdf');text='\n'.join(page.extract_text() for page in PdfReader(p).pages)
        assert pd.Timestamp(date).strftime('%B %d, %Y') in ' '.join(text.split()),name
        assert 'holiday' in text.lower()
        closed.add(pd.Timestamp(date));evidence.append(dict(path=str(p.relative_to(ROOT)),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),additional_closed=date))
    expected=(set(pd.bdate_range('2017-07-01','2020-09-01'))-closed)|{d for d in special if pd.Timestamp('2017-07-01')<=d<=pd.Timestamp('2020-09-01')}
    observed={pd.Timestamp(r['date']) for r in json.loads((OUT/sys.argv[1]/'raw_file_checks.json').read_text())}
    local=md.load_snapshot().equity_prices();local_dates=set(local.loc[local.date.between('2017-07-01','2020-09-01'),'date'])
    report=dict(expected_sessions=len(expected),primary_observed_sessions=len(observed),missing_primary_sessions=sorted(str(d.date()) for d in expected-observed),unexpected_primary_sessions=sorted(str(d.date()) for d in observed-expected),local_missing_sessions=sorted(str(d.date()) for d in expected-local_dates),evidence=evidence,active_calendar_changed=False)
    assert not report['missing_primary_sessions'] and not report['unexpected_primary_sessions'],report
    (OUT/'calendar_reconciliation.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
