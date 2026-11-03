from pathlib import Path
import re,json,pandas as pd
from pypdf import PdfReader
root=Path('outputs/backtest_repair_v2_20261103');d=pd.read_csv(root/'bounded_candidate_v3/event_identity_decisions.csv')
checks=[]
for name,day in [('ind_prs29082017.pdf','2017-09-05'),('ind_prs17092019.pdf','2019-09-27'),('ind_prs10062020.pdf','2020-06-26')]:
 text='\n'.join(p.extract_text() for p in PdfReader(root/'evidence'/name).pages)
 start=re.search(r'NIFTY\s+500(?:\s+Index)?',text,re.I).end()
 after=text[start:];nxt=re.search(r'\n\s*\d+\)\s*NIFTY',after,re.I)
 section=after[:nxt.start()] if nxt else after
 include=re.search(r'following\s+compan(?:y|ies)\s+(?:is|are)\s+being\s+included',section,re.I)
 exclude=re.search(r'following\s+compan(?:y|ies)\s+(?:is|are)\s+being\s+excluded',section,re.I)
 assert include and exclude,(name,section[:500])
 parts={'EXCL':section[exclude.end():include.start()],'INCL':section[include.end():]}
 covered=[];uncovered=[]
 for row in d[d.EVENT_DATE.eq(day)].to_dict('records'):
  symbol=row['SYMBOL']
  found=re.search(r'(?<![A-Z0-9&-])'+re.escape(symbol)+r'(?![A-Z0-9&-])',parts[row['EVENT_TYPE']]) is not None
  (covered if found else uncovered).append(dict(source_row_id=row['SOURCE_ROW_ID'],symbol=symbol,direction=row['EVENT_TYPE']))
 checks.append(dict(notice=name,effective_date=day,effective_basis='effective on stated date; close of previous session',
                    covered_source_records=covered,source_records_not_covered_by_this_specific_notice=uncovered,
                    completeness='not asserted; notice can amend an earlier main review'))
(root/'primary_event_date_direction_checks.json').write_text(json.dumps(checks,indent=2))
print([(c['notice'],len(c['covered_source_records']),len(c['source_records_not_covered_by_this_specific_notice'])) for c in checks])
