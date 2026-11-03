from pathlib import Path
import json,re,sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd

OUT=Path(__file__).parent
# Effective dates are reviewed against retained notice text; not inferred from
# file/retrieval dates. Main reviews and amendments stay separate.
catalogue={
 'ind_prs29082017':('2017-09-05','reschedules RELCAPITAL/MFSL from main review'),
 'ind_prs28082017':('2017-09-29','main review; RELCAPITAL/MFSL superseded'),
 'ind_prs16102017':('2017-10-30','ad hoc'),
 'ind_prs03112017':('2017-11-13','ad hoc'),
 'ind_prs10112017':('2017-11-20','ad hoc'),
 'ind_prs08012018':('2018-02-05','ad hoc'),
 'ind_prs06032018':('2018-03-13','accelerates VIDEOIND/ERIS from main review'),
 'ind_prs21022018':('2018-04-02','main review; VIDEOIND/ERIS superseded'),
 'ind_prs24052018':('2018-06-29','main quarterly review'),
 'ind_prs20062018':('2018-06-29','additional HTMEDIA/ASTERDM'),
 'ind_prs01082018':('2018-08-08','ad hoc'),
 'ind_prs28082018':('2018-09-28','main review'),
 'ind_prs31082018':('2018-09-28','additional KPIT/SHILPAMED'),
 'ind_prs14122018':('2018-12-28','quarterly'),
 'ind_prs21012019':('2019-01-28','ad hoc'),
 'ind_prs25022019':('2019-03-29','main review'),
 'ind_prs08042019':('2019-04-15','ad hoc'),
 'ind_prs13062019':('2019-06-28','quarterly'),
 'ind_prs17092019_1':('2019-09-24','ad hoc JETAIRWAYS/VAIBHAVGBL'),
 'ind_prs28082019':('2019-09-27','main review'),
 'ind_prs17092019':('2019-09-27','additional THOMASCOOK/IIFL'),
 'ind_prs18122019':('2019-12-26','ad hoc suspension removals'),
 'ind_prs16122019':('2019-12-27','quarterly review'),
 'ind_prs19122019':('2019-12-27','additional DEN/HATHWAY'),
 'ind_prs09012020':('2020-01-16','ad hoc'),
 'ind_prs16032020':('2020-03-19','accelerated YESBANK/SWSOLAR'),
 'ind_prs10062020':('2020-06-26','revised review after March deferral'),
 'ind_prs02072020_1':('2020-07-31','ad hoc VEDL/SBICARD'),
}

from historical_reconciliation import parse_review_transition_section as parse_section

if __name__=='__main__':
    events=pd.read_csv('outputs/backtest_repair_v2_20261103/bounded_candidate_v3/event_identity_decisions.csv')
    rows=[]
    for stem,(day,note) in catalogue.items():
        section=OUT/'extracted'/(stem+'_500.txt')
        for r in parse_section(section.read_text(encoding='utf-8')):
            r.update(effective_date=day,source=stem+'.pdf',review_note=note)
            r['status']='superseded' if (stem=='ind_prs28082017' and r['symbol'] in ['RELCAPITAL','MFSL']) or (stem=='ind_prs21022018' and r['symbol'] in ['VIDEOIND','ERIS']) else 'applicable'
            rows.append(r)
    frame=pd.DataFrame(rows);frame.to_csv(OUT/'primary_review_transitions.csv',index=False)
    checks=[]
    for day,g in events.groupby('EVENT_DATE'):
        raw=set(zip(g.EVENT_TYPE,g.SYMBOL));p=frame[frame.effective_date.eq(day)&frame.status.eq('applicable')]
        official=set(zip(p.direction,p.symbol))
        checks.append(dict(date=day,source_records=len(g),source_transitions=len(raw),official_transitions=len(official),source_only=sorted(raw-official),official_only=sorted(official-raw),sources=sorted(set(p.source)),exact_match=raw==official))
    (OUT/'event_set_comparison.json').write_text(json.dumps(checks,indent=2))
    for c in checks:print(c['date'],c['source_transitions'],c['official_transitions'],'MATCH' if c['exact_match'] else {k:c[k] for k in ['source_only','official_only']})
