"""Independent primary-PDF extraction and endpoint/monthly membership audit.

Uses pdfplumber instead of pypdf and no production walk/parser function.
"""
from pathlib import Path
import sys,json,re,zipfile,hashlib
import pandas as pd
import pdfplumber
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from review_catalogue import catalogue
OUT=Path(__file__).parent;PREV=ROOT/'outputs/backtest_repair_v2_20261103';CAND=OUT/'candidate_v3'

if __name__=='__main__':
    original=pd.read_csv(CAND/'event_identity_and_timing_decisions.csv')
    notice_rows=[];tables=[]
    for stem,(date,note) in catalogue.items():
        with pdfplumber.open(OUT/'evidence'/(stem+'.pdf')) as pdf:
            text='\n'.join(page.extract_text(x_tolerance=1,y_tolerance=3) for page in pdf.pages)
        lines=text.splitlines();inside=False;direction=None;got=[]
        for line in lines:
            heading=re.match(r'^\s*\d+[).]\s+(NIFTY.*)$',line,re.I)
            if heading:
                if inside:break
                inside=bool(re.match(r'NIFTY\s+500\b',heading[1],re.I))
            if not inside:continue
            if 'following compan' in line.lower() and 'exclud' in line.lower():direction='EXCL'
            if 'following compan' in line.lower() and 'includ' in line.lower():direction='INCL'
            parts=line.split()
            if direction and parts and parts[0].isdigit() and len(parts)>2 and re.fullmatch(r'[A-Z0-9&_.-]+',parts[-1]):
                got.append((direction,parts[-1]))
        assert got,(stem,text[:500])
        # Explicit review amendment decisions; original source PDFs unchanged.
        superseded={'ind_prs28082017':{'RELCAPITAL','MFSL'},'ind_prs21022018':{'VIDEOIND','ERIS'}}.get(stem,set())
        notice_rows.extend((date,kind,symbol) for kind,symbol in got if symbol not in superseded)
        tables.append(dict(source=stem+'.pdf',independent_rows=len(got),applicable_rows=sum(s not in superseded for _,s in got)))
        print('Independent table',stem,len(got),flush=True)
    official=set(notice_rows);source=set(zip(original.EVENT_DATE,original.EVENT_TYPE,original.SYMBOL))
    assert official==source,dict(source_only=sorted(source-official),official_only=sorted(official-source))
    monthly=pd.read_csv(PREV/'bounded_candidate_v3/monthly_security_observations.csv',parse_dates=['date'])
    # Re-read dated exchange files to establish each monthly ISIN truth set.
    truth={}
    for day,g in monthly.groupby('date'):
        if not pd.Timestamp('2017-06-30')<=day<=pd.Timestamp('2020-08-31'):continue
        name='cm'+day.strftime('%d%b%Y').upper()+'bhav.csv.zip'
        path=next(p for p in [PREV/'evidence'/name,OUT/'evidence'/name] if p.exists())
        with zipfile.ZipFile(path) as z:
            member=next(n for n in z.namelist() if n.lower().endswith('.csv'));raw=pd.read_csv(z.open(member))
        matched=raw[raw.SYMBOL.isin(set(g.symbol))&raw.SERIES.isin(['EQ','BE','BZ'])]
        ids=set('ISIN:'+s for s in matched.ISIN)
        assert ids==set('ISIN:'+s for s in g.ISIN),str(day)
        truth[day]=ids
    actions=json.loads((CAND/'reviewed_lifecycle_decisions.json').read_text())
    valid_actions=[a for a in actions if a['applicable_to_scope']]
    # Recheck both daily boundary references, not just monthly ID bounds.
    boundary_checks=[]
    for a in actions:
        p=ROOT/a['source'];assert hashlib.sha256(p.read_bytes()).hexdigest()==a['source_sha256']
        for reference,expected in zip(a['boundary_evidence'],[a['old_security_id'],a['new_security_id']]):
            archive,member,line=reference.split(':');p=next(p for p in [OUT/'evidence'/archive,PREV/'evidence'/archive] if p.exists())
            with zipfile.ZipFile(p) as z:r=pd.read_csv(z.open(member)).iloc[int(line)-2]
            assert 'ISIN:'+r.ISIN==expected and r.SYMBOL==a['symbol'],a
        boundary_checks.append(a['action_id'])
    e=original.drop_duplicates(['EVENT_DATE','EVENT_TYPE','SECURITY_ID'])
    events={day:[(r.EVENT_TYPE,r.SECURITY_ID) for r in g.itertuples()] for day,g in e.groupby('EVENT_DATE')}
    calendar=sorted(set(events)|{a['identifier_effective_date'] for a in valid_actions})
    observed_initial=truth[pd.Timestamp('2017-06-30')]
    state=set(observed_initial);daily={pd.Timestamp('2017-07-01'):set(state)}
    for day in calendar:
        for a in valid_actions:
            if a['identifier_effective_date']==day and a['old_security_id'] in state:
                assert a['new_security_id'] not in state;state.remove(a['old_security_id']);state.add(a['new_security_id'])
        changes=events.get(day,[])
        for kind,sid in changes:
            if kind=='EXCL':assert sid in state;state.remove(sid)
        for kind,sid in changes:
            if kind=='INCL':assert sid not in state;state.add(sid)
        daily[pd.Timestamp(day)]=set(state)
    anchor=set(truth[pd.Timestamp('2020-08-31')]);assert state==anchor
    comparisons=[]
    for day,ids in truth.items():
        sample=max(day,pd.Timestamp('2017-07-01'));actual=daily[max(k for k in daily if k<=sample)]
        assert actual==ids,str(day);comparisons.append(dict(date=str(day.date()),securities=len(ids),matches=True))
    state=set(anchor)
    for day in reversed(calendar):
        for kind,sid in events.get(day,[]):
            if kind=='INCL':assert sid in state;state.remove(sid)
        for kind,sid in events.get(day,[]):
            if kind=='EXCL':assert sid not in state;state.add(sid)
        for a in reversed(valid_actions):
            if a['identifier_effective_date']==day and a['new_security_id'] in state:
                assert a['old_security_id'] not in state;state.remove(a['new_security_id']);state.add(a['old_security_id'])
    assert state==observed_initial
    # Independently repeat economic-ID membership at evidenced ex dates. Raw
    # source rows are not rewritten; only the known identifier-lag observations
    # are projected for comparison, with every difference retained.
    economic_calendar=sorted(set(events)|{a['ex_date'] for a in valid_actions})
    state=set(observed_initial);economic_daily={pd.Timestamp('2017-07-01'):set(state)}
    for day in economic_calendar:
        for a in valid_actions:
            if a['ex_date']==day and a['old_security_id'] in state:
                assert a['new_security_id'] not in state;state.remove(a['old_security_id']);state.add(a['new_security_id'])
        for kind,sid in events.get(day,[]):
            if kind=='EXCL':assert sid in state;state.remove(sid)
        for kind,sid in events.get(day,[]):
            if kind=='INCL':assert sid not in state;state.add(sid)
        economic_daily[pd.Timestamp(day)]=set(state)
    assert state==anchor
    economic_comparisons=[]
    for day,ids in truth.items():
        projected=set(ids);projection=[]
        for a in valid_actions:
            if a['ex_date']<=str(day.date())<a['identifier_effective_date'] and a['old_security_id'] in projected:
                projected.remove(a['old_security_id']);projected.add(a['new_security_id']);projection.append(a['action_id'])
        sample=max(day,pd.Timestamp('2017-07-01'));actual=economic_daily[max(k for k in economic_daily if k<=sample)]
        assert actual==projected,str(day)
        economic_comparisons.append(dict(date=str(day.date()),matches=True,explicit_identifier_projections=projection))
    state=set(anchor)
    for day in reversed(economic_calendar):
        for kind,sid in events.get(day,[]):
            if kind=='INCL':assert sid in state;state.remove(sid)
        for kind,sid in events.get(day,[]):
            if kind=='EXCL':assert sid not in state;state.add(sid)
        for a in reversed(valid_actions):
            if a['ex_date']==day and a['new_security_id'] in state:
                assert a['old_security_id'] not in state;state.remove(a['new_security_id']);state.add(a['old_security_id'])
    assert state==observed_initial
    economic_saved=pd.read_parquet(CAND/'economic_provisional_membership.parquet')
    for day,g in economic_saved.groupby('TIME_STAMP'):assert set(g.SECURITY_ID)==economic_daily[day]
    report=dict(passed=True,raw_notice_tables=tables,independent_unique_transitions=len(official),preserved_source_records=len(original),verified_action_boundaries=len(boundary_checks),applicable_actions=len(valid_actions),forward_anchor_matches=True,reverse_initial_matches=True,monthly_comparisons=comparisons,economic_forward_anchor_matches=True,economic_reverse_initial_matches=True,economic_monthly_comparisons=economic_comparisons,production_walker_imported=False,accepted=False)
    (OUT/'independent_membership_reconciliation_v2.json').write_text(json.dumps(report,indent=2))
    print('PASSED independent primary notice sets, daily lineage references, both observed endpoints and',len(comparisons),'monthly observations',flush=True)
