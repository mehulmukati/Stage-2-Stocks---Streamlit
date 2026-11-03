"""Rank only decision-specific announcement terms for human review."""
import hashlib
import json
from pathlib import Path

import pandas as pd

OUT=Path(__file__).parent
work=pd.read_csv(OUT/'finite_entitlement_worklist.csv')
rows=[]
for r in work.to_dict('records'):
    p=OUT/'evidence'/(r['symbol']+'_'+pd.Timestamp(r['date']).strftime('%Y%m%d')+'_announcements.json')
    if not p.exists():continue
    records=json.loads(p.read_text())
    if not isinstance(records,list):continue
    for i,a in enumerate(records):
        text=str(a.get('attchmntText',''));desc=str(a.get('desc',''));low=(desc+' '+text).lower()
        relevant=any(w in low for w in ['rights','demerg','scheme','dividend','bonus','record date','amalgam'])
        if not relevant:continue
        score=5*('rights' in desc.lower())+4*('scheme' in desc.lower())+4*('demer' in desc.lower())+2*('rights' in low)+2*('scheme' in low)+('price' in low)+('fully paid' in low)+('ratio' in low)
        rows.append(dict(decision_id=r['decision_id'],symbol=r['symbol'],date=r['date'],source=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),source_record=i,announcement_date=a.get('an_dt'),description=desc,text=text,attachment=a.get('attchmntFile'),score=score))
frame=pd.DataFrame(rows).sort_values(['decision_id','score'],ascending=[True,False])
frame.to_csv(OUT/'finite_announcement_terms.csv',index=False)
rights=work[work.subject.str.contains('rights',case=False)]
for r in rights.to_dict('records'):
    print('\n',r['symbol'],r['date'])
    for a in frame[frame.decision_id.eq(r['decision_id'])].head(3).to_dict('records'):
        print(a['source_record'],a['announcement_date'],a['description'],a['text'][:1500])
