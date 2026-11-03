"""Fast source-page indexing of retained issuer documents for finite decisions."""
import json
import re
from pathlib import Path

import pypdfium2 as pdfium

OUT=Path(__file__).parent
target=OUT/'extracted';target.mkdir(exist_ok=True)
for path in sorted((OUT/'evidence').glob('*.pdf')):
    destination=target/(path.stem+'.json')
    if destination.exists():continue
    with pdfium.PdfDocument(path) as document:
        pages=[dict(page=i+1,text=document[i].get_textpage().get_text_range()) for i in range(len(document))]
    destination.write_text(json.dumps(pages,indent=2),encoding='utf-8')
    print(path.name,len(pages))
    shown=0
    for page in pages:
        text=' '.join(page['text'].split())
        for match in re.finditer(r'(?i)(?:pre.bonus|post.bonus|rank pari|eligible for dividend|13\.5|14\.50|dividend.{0,30}29|dividend.{0,30}1\.10|listed.{0,50}2018|exchange ratio)',text):
            print(page['page'],text[max(0,match.start()-120):match.end()+250]);shown+=1
            if shown>=12:break
        if shown>=12:break
