"""Explicit finite primary-term reviews; never infer a factor from price changes."""
import hashlib
import json
import re
import sys
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from research_entitlements import reviewed_cash_share_action

OUT=Path(__file__).parent
R3=ROOT/'outputs/backtest_repair_v3_20261103'
R4=ROOT/'outputs/backtest_repair_v4_20261103'


def evidence(path,locator):
    path=Path(path)
    return dict(path=str(path.relative_to(ROOT)),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),locator=locator)


def main(name):
    work=pd.read_csv(OUT/'finite_entitlement_worklist.csv').fillna('')
    bulletins=pd.read_csv(OUT/'retained_bulletin_terms.csv').fillna('')
    announcements=pd.read_csv(OUT/'finite_announcement_terms.csv').fillna('')
    api=R4/'evidence/nse_actions_2014_2020.json'
    cash={('CRISIL','2020-08-04'):[6],('ASIANPAINT','2017-06-15'):[5.65,2],
          ('RELAXO','2018-09-18'):[1.5],('SUPREMEIND','2017-06-21'):[7,5],
          ('BRITANNIA','2020-08-26'):[83],('SYMPHONY','2017-08-23'):[1,1],
          ('SYMPHONY','2020-02-17'):[2,18],('AUROPHARMA','2020-08-21'):[1.25],
          ('CCL','2020-03-06'):[1.5,1.5],('ENGINERSIN','2018-09-11'):[1.5],
          ('RALLIS','2017-06-09'):[2.5,1.25],('PAGEIND','2018-11-20'):[41,110],
          ('M&MFIN','2019-07-15'):[4,2.5],('BALKRISIND','2020-08-20'):[3],
          ('HINDZINC','2018-03-23'):[6]}
    combined={
        'ICICIBANK':(1.1,2.5,'ICICIBANK_prebonus_issuer.html','Issuer Q2 FY2018 notes 1-2 explicitly pre-bonus dividend'),
        'NIACL':(2,5,'NIACL_20180627_announcements.json','NSE records16,30,49,50: June28 dividend/bonus record date; bonus allotted June29, so Rs5 is per old recorded share'),
        'UNOMINDA':(3,1.6,'UNOMINDA_20180711_announcements.json','NSE records18,26,34,36: July12 common record date and July13 bonus allotment; Rs1.60 on old recorded shares'),
        'HINDPETRO':(1.5,1.1,'HINDPETRO_2018_annual_issuer.pdf','PDF pages 120,134/Note45: net final dividend Rs111.75crore at Rs1.10 on pre-bonus 1,015,881,750 shares'),
        'EMAMILTD':(2,7,'EMAMILTD_2019_annual_issuer.pdf','PDF page230 Note3.46: FY18 Rs7 and net Rs15,887.73lakh, with explicit bonus-comparability footnote'),
        'GICRE':(2,13.5,'GICRE_2019_annual_issuer.pdf','PDF pages43,119,140: old877,200,000/new1,754,400,000 shares and Rs11,842,200thousand final dividend; cash denominator old shares'),
        'TCS':(2,29,'TCS_bonus_record_ad_issuer.pdf','Visually checked PDF page2: bonus shares participate in dividends recommended and declared after allotment; FY18 Rs29 recommended April19 before June3 allotment')}
    decisions=[]
    for r in work.to_dict('records'):
        own=bulletins[bulletins.decision_id.eq(r['decision_id'])]
        refs=[evidence(api,r['source_row_id'])]
        refs += [evidence(ROOT/x.path,x.bulletin_row_id) for x in own.itertuples()]
        d=dict(decision_id=r['decision_id'],source_row_id=r['source_row_id'],date=r['date'],
               original_subject=r['subject'],query_symbol=r['symbol'],dated_symbol=r['dated_symbol'],
               dated_isin=r['dated_isin'],dated_identity_source_row=r['dated_identity_source_row'],
               research_security_id=r['research_security_id'],record_date=r['recDate'],
               conservative_consumption_start=r['consumption_start'],consumption_end=r['consumption_end'],
               status='unsupported',evidence=refs,normalization=None,
               ownership_treatment='No invented distribution, share conversion or subscription cash',
               needed='Final primary entitlement terms and independently justified ex-date research normalization/ownership marks')
        key=(r['symbol'],r['date'])
        if key in cash:
            amounts=cash[key]
            numbers=[float(n) for text in own.bulletin_terms for n in re.findall(r'\d+(?:\.\d+)?',text)]
            assert all(any(abs(value-n)<1e-9 for n in numbers) for value in amounts),(key,numbers)
            d.update(status='resolved',cash_components=amounts,
                     normalization=dict(kind='reviewed_cash_dividend',supported=True,share_ratio=1.,cash=sum(amounts)),
                     attribution='Original same-date bulletin terms and actual quoted security bind the restated API identifier',
                     needed='',ownership_treatment='Cash embedded once in adjusted units; no portfolio cash credit')
        elif r['symbol']=='BRITANNIA' and r['date']=='2018-11-29':
            lifecycle=json.loads((R3/'candidate_v3/reviewed_lifecycle_decisions.json').read_text())
            action=next(a for a in lifecycle if a['symbol']=='BRITANNIA' and a['ex_date']==r['date'])
            assert action['share_ratio']==2 and r['dated_isin'] in [action['old_security_id'].removeprefix('ISIN:'),action['new_security_id'].removeprefix('ISIN:')]
            d['evidence'].append(evidence(ROOT/action['source'],'Reviewed original Britannia MSEI 2018-11-29 split row'))
            d.update(status='resolved',normalization=dict(kind='reviewed_split',supported=True,share_ratio=2.,cash=0.),needed='',attribution='Explicit price-action binding to Round3 reviewed split and dated quote; API obsolete ISIN retained',ownership_treatment='Split normalized once; no extra research quantity credit')
        elif r['symbol'] in combined and r['kind']=='combined_action_requires_review':
            ratio,amount,filename,locator=combined[r['symbol']]
            source=OUT/'evidence'/filename
            assert source.exists(),filename
            if r['symbol']=='ICICIBANK':
                text=BeautifulSoup(source.read_bytes(),'html.parser').get_text(' ',strip=True)
                assert 'pre-bonus issue' in text and '2.50' in text
            if r['symbol']=='NIACL':
                records=json.loads(source.read_text())
                assert 'Jun 29, 2018' in records[16]['attchmntText']
                assert 'June 28, 2018' in records[30]['attchmntText']
                assert 'Dividend of 5 per equity share' in records[50]['attchmntText']
            if r['symbol']=='UNOMINDA':
                records=json.loads(source.read_text())
                assert 'July 13, 2018' in records[18]['attchmntText']
                assert 'July 12, 2018' in records[26]['attchmntText']
                assert 'Rs. 1.60 per equity share' in records[36]['attchmntText']
            d['evidence'].append(evidence(source,locator))
            if r['symbol']=='TCS':
                d['evidence'].append(evidence(OUT/'evidence/TCS_2018_annual_issuer.pdf','PDF pages37,165: final29 proposed April19,2018'))
                d['evidence'].append(evidence(OUT/'evidence/TCS_announcements_201804_201806.json','records24,59: June3 allotment / April19 dividend recommendation'))
            d.update(status='resolved',normalization=reviewed_cash_share_action(ratio,amount,'pre_action_share'),needed='',
                     ownership_treatment='Pre-action cash deducted, then share ratio applied once; no additional portfolio credit')
        elif r['symbol']=='COLPAL':
            d['evidence'].append(evidence(OUT/'evidence/COLPAL_2019_annual_issuer.pdf','PDF pages32,48,115: second interim Rs7, paid April22; API record date April8'))
            d.update(status='resolved',normalization=dict(kind='reviewed_cash_dividend',supported=True,share_ratio=1.,cash=7.),needed='',ownership_treatment='Single embedded dividend; no extra cash')
        elif r['symbol']=='SRF':
            p=OUT/'evidence/SRF_20170816_announcements.json';records=json.loads(p.read_text())
            assert 'Rs. 6 per share' in records[8]['attchmntText'] and 'August 18, 2017' in records[10]['attchmntText']
            d['evidence'].append(evidence(p,'records8,10: Aug8 board approval Rs6 and Aug18 record date'))
            d.update(status='resolved',record_date='2017-08-18',normalization=dict(kind='reviewed_cash_dividend',supported=True,share_ratio=1.,cash=6.),needed='',ownership_treatment='Single embedded dividend; no extra cash')
        else:
            selected=announcements[announcements.decision_id.eq(r['decision_id'])].head(4)
            d['primary_announcement_records']=selected[['announcement_date','description','text','source_record','attachment']].to_dict('records')
            for x in selected.itertuples():
                d['evidence'].append(evidence(Path(x.source),'NSE announcement record '+str(x.source_record)))
            if 'rights' in r['subject'].lower():
                d['needed']='Final rights offer: new-security paid-up status, total issue price (not premium), cash/call dates and treatment of partly-paid claims; no automatic fully-paid TERP'
                text=' '.join(selected.text).lower()
                d['partly_paid_primary_notice']='partly paid' in text or 'partly-paid' in text
            elif 'debenture' in r['subject'].lower():
                d['needed']='Bonus-debenture face value, coupon, redemption and entitlement timing plus an independent ex-date value/mark; not an equity split'
            elif r['kind']=='combined_action_requires_review':
                d['needed']='Primary cash denomination relative to bonus entitlement; do not choose denominator from observed price changes'
            else:
                d['needed']='Final scheme: exact child/successor security and ratio, effective/record/credit/trading dates; independent entitlement value before listing or a verifiable adjusted research series'
        # Individually evidenced fully-paid theoretical rights, same convention
        # as the already reviewed IDEA example. No actual subscription credit.
        if r['symbol'] in ['INDHOTEL','LAKSHVILAS','M&MFIN'] and 'rights' in r['subject'].lower():
            if r['symbol']=='INDHOTEL':
                ratio,price=1/5,75.;p=OUT/'evidence/INDHOTEL_offer_SEBI.pdf';locator='PDF pages28,47: 100% of Rs75 payable on application; newly allotted shares fully paid; ratio1:5'
            elif r['symbol']=='LAKSHVILAS':
                ratio,price=1/3,122.;p=OUT/'evidence/LAKSHVILAS_20171205_announcements.json';locator='record5: full Rs122 amount payable on application; ratio1:3 in dated action'
                assert 'full amount is to be paid on application' in json.loads(p.read_text())[5]['attchmntText']
            else:
                ratio,price=1.,50.;p=OUT/'evidence/M&MFIN_20200722_announcements.json';locator='records7,23: new fully-paid equity issue for Rs50, ratio1:1'
                assert 'fully paid-up Equity' in json.loads(p.read_text())[7]['attchmntText']
            d['evidence'].append(evidence(p,locator))
            d.update(status='resolved',normalization=dict(kind='reviewed_fully_paid_rights',supported=True,
                        share_ratio=1.,cash=0.,rights_ratio=ratio,subscription_price=price,reviewed_source=str(p.relative_to(ROOT))),
                     needed='',ownership_treatment='Theoretical full-paid ex-rights research normalization only; no extra subscription cash, shares or sale proceeds')
        verified_offers = {
            'nse_actions_2014_2020.json_9929': (49/100, 10., 'nse_filing_c0a3172f40dbe6ae.pdf',
                'Final HCC offer p305: 49 for 100; full Rs10 payable on application'),
            'nse_actions_2014_2020.json_12742': (3/26, 570., 'nse_filing_ff51bb1b94b88bb0.pdf',
                'Shriram Transport issuer filing p1: full Rs570 payable on application; 3 for 26; July10 record'),
        }
        if d['decision_id'] in verified_offers:
            ratio,price,filename,locator=verified_offers[d['decision_id']]
            p=OUT/'evidence'/filename
            d['evidence'].append(evidence(p,locator))
            d.update(status='resolved',normalization=dict(kind='reviewed_fully_paid_rights',supported=True,
                        share_ratio=1.,cash=0.,rights_ratio=ratio,subscription_price=price,reviewed_source=str(p.relative_to(ROOT))),
                     needed='',ownership_treatment='Theoretical full-paid ex-rights normalization; no extra subscription cash, shares or proceeds')
        precise = {
            'RELIANCE':'Partly paid 1:15, total Rs1257. Need independently evidenced value of the entitlement including staged calls, or contemporaneous priced right with correct timing.',
            'DHANI':'Historical IBVENTURES 3:16, total Rs240, Rs60 initial. Need value of the partly paid entitlement including the remaining calls at the ex-date.',
            'HATSUN':'Partly paid 1:16 at Rs555 plus separate Rs3 interim dividend. Need application/call schedule and independently supported rights value without counting the dividend twice.',
            'ABFRL':'Partly paid 9:77 at Rs110. Need the entitlement valuation reflecting actual instalment terms or an independently documented adjusted series.',
            'TATASTEEL':'Two tranches, 4:25 fully paid and 2:25 partly paid. Need independent valuation and treatment of both tranches without treating the second as fully paid.',
            'ARVINDFASN':'March 2020 offer was deferred. Need final record/eligibility disposition and issue terms, or exchange confirmation that the original ex-date generated no entitlement.',
            'GRASIM':'Primary scheme provides 7 ABCAPITAL shares per 5 GRASIM and July20 record date. Need independently supported ex-date child entitlement value before listing.',
        }
        if d['status']=='unsupported' and r['symbol'] in precise:
            d['needed']=precise[r['symbol']]
        decisions.append(d)
    assert len(decisions)==67
    with (OUT/(name+'.json')).open('x') as f:json.dump(decisions,f,indent=2)
    pd.DataFrame([{k:v for k,v in d.items() if k not in ['evidence','primary_announcement_records','normalization']}|dict(normalization=json.dumps(d['normalization']),evidence=json.dumps(d['evidence'])) for d in decisions]).to_csv(OUT/(name+'.csv'),index=False)
    print(pd.Series([d['status'] for d in decisions]).value_counts().to_string())


if __name__=='__main__':main(sys.argv[1])

