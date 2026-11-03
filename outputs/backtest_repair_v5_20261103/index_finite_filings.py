"""Index retained original attachments without unsafe archive extraction."""
import hashlib
import json
import re
import zipfile
from pathlib import Path

import pandas as pd
import pypdfium2 as pdfium

OUT=Path(__file__).parent
DEST=OUT/'filing_text';DEST.mkdir(exist_ok=True)
records=[];terms=[]
names=[p for pattern in ['nse_filing_*','*final_offer_SEBI.pdf','UNOMINDA_bonus_dividend_notice_NSE.zip','SRF_dividend_NSE.zip','TECHNO_ratio_record_NSE.zip'] for p in (OUT/'evidence').glob(pattern)]
for path in sorted(set(names)):
    if path.suffix.lower()=='.zip':
        with zipfile.ZipFile(path) as z:
            payloads=[(m,z.read(m)) for m in z.namelist() if m.lower().endswith('.pdf') and z.getinfo(m).file_size<100_000_000]
    else:payloads=[(path.name,path.read_bytes())]
    for ordinal,(member,raw) in enumerate(payloads):
        dest=DEST/(path.stem+'_'+str(ordinal)+'.json')
        if dest.exists():pages=json.loads(dest.read_text(encoding='utf-8'))
        else:
            with pdfium.PdfDocument(raw) as pdf:
                pages=[dict(page=i+1,text=pdf[i].get_textpage().get_text_range()) for i in range(len(pdf))]
            dest.write_text(json.dumps(pages,indent=2),encoding='utf-8')
        records.append(dict(archive=path.name,member=member,member_sha256=hashlib.sha256(raw).hexdigest(),text_path=str(dest),pages=len(pages),text_characters=sum(len(p['text']) for p in pages)))
        for p in pages:
            text=' '.join(p['text'].split())
            for match in re.finditer(r'(?i)(?:terms of payment|full amount.{0,35}application|100% of.{0,30}issue|partly.paid|payable.{0,40}application|share exchange ratio|ratio of [0-9].{0,25}for every|not.{0,20}dividend)',text):
                terms.append(dict(archive=path.name,member=member,page=p['page'],context=text[max(0,match.start()-100):match.end()+350]))
pd.DataFrame(records).to_csv(OUT/'filing_text_manifest.csv',index=False)
pd.DataFrame(terms).drop_duplicates().to_csv(OUT/'filing_payment_ownership_terms.csv',index=False)
print('Retained PDF members indexed:',len(records),'term contexts:',len(terms))
