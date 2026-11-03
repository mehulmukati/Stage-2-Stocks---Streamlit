"""Select a bounded set of original filings for the remaining finite decisions."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

OUT=Path(__file__).parent
work=pd.read_csv(OUT/'finite_entitlement_worklist.csv')
terms=pd.read_csv(OUT/'finite_announcement_terms.csv')
requests={};basis=[]
for r in work[work.kind.eq('unsupported_entitlement')].to_dict('records'):
    own=terms[terms.decision_id.eq(r['decision_id'])].copy()
    rights='rights' in r['subject'].lower()
    if rights:own=own[own.text.str.contains('rights',case=False,na=False)]
    else:own=own[own.text.str.contains('scheme|demerg|amalgam',case=False,regex=True,na=False)]
    own=own[own.attachment.fillna('').str.startswith('https://nsearchives.nseindia.com/')]
    own['review_score']=own.score+own.text.str.len()/1000+10*own.text.str.contains('letter of offer|newspaper|full amount|partly paid',case=False,regex=True).astype(int)
    for a in own.sort_values('review_score',ascending=False).head(2 if rights else 1).to_dict('records'):
        url=a['attachment'];suffix=Path(urlparse(url).path).suffix.lower()
        if suffix not in ['.zip','.pdf']:continue
        name='nse_filing_'+hashlib.sha256(url.encode()).hexdigest()[:16]+suffix
        requests[name]=url
        basis.append(dict(decision_id=r['decision_id'],name=name,url=url,source=a['source'],announcement_record=a['source_record'],announcement_date=a['announcement_date'],text=a['text'],role='candidate primary terms; does not imply accepted economics'))
extra=[
 ['CANBK_final_offer_SEBI.pdf','https://www.sebi.gov.in/sebi_data/attachdocs/1487579909741.pdf'],
 ['KARURVYSYA_final_offer_SEBI.pdf','https://www.sebi.gov.in/sebi_data/attachdocs/nov-2017/1510050330405.pdf'],
 ['UNOMINDA_bonus_dividend_notice_NSE.zip','https://nsearchives.nseindia.com/corporate/3163_001_sw_21062018114112_484.zip'],
 ['SRF_dividend_NSE.zip','https://nsearchives.nseindia.com/corporate/2017_08_08_Ist_Interim_Dividend_17_18_SRF_08082017181446_471.zip'],
 ['TECHNO_ratio_record_NSE.zip','https://nsearchives.nseindia.com/corporate/RD_30072018202546_30072018202620_30072018203207.zip']]
requests.update(dict(extra))
(OUT/'finite_attachment_urls.json').write_text(json.dumps(list(requests.items()),indent=2))
pd.DataFrame(basis).to_csv(OUT/'finite_attachment_request_basis.csv',index=False)
print('Decision-specific filings:',len(requests))
