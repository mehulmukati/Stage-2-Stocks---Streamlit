"""Freeze the finite Round 5 decision set and read-only acquisition requests."""
import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
R4 = ROOT / 'outputs/backtest_repair_v4_20261103'
R3 = ROOT / 'outputs/backtest_repair_v3_20261103'

if __name__ == '__main__':
    if (OUT/'baseline.json').exists():
        raise FileExistsError('Closure baseline already frozen')
    rows = pd.read_csv(R4/'consumption_audit_v7/scoped_unresolved_actions.csv')
    rows = rows[rows.potentially_consumed].copy()
    assert len(rows) == 67 and rows.research_security_id.nunique() == 59
    prices = pd.read_parquet(R4/'primary_model_v8/primary_adjusted_research_candidate.parquet')
    lookup = prices.set_index(['research_security_id','date'])
    queries = []
    for i, r in rows.iterrows():
        key = (r.research_security_id, pd.Timestamp(r.date))
        if key in lookup.index:
            own = lookup.loc[key]
            rows.loc[i,'dated_symbol'] = own.SYMBOL
            rows.loc[i,'dated_isin'] = own.ISIN
            rows.loc[i,'dated_identity_source_row'] = own.source_row_id
        else:
            rows.loc[i,'dated_symbol'] = r.symbol
        rows.loc[i,'decision_id'] = r.source_row_id.replace(':','_')
    rows.to_csv(OUT/'finite_entitlement_worklist.csv',index=False)
    for symbol in sorted(set(rows.dated_symbol)):
        name = symbol+'_msei.html'
        if not (R3/'evidence'/name).exists():
            queries.append([name,'https://www.msei.in/corporates/corporate-securities-information/corporate-update/default?symbol='+symbol+'&type=4&vmode=vm'])
    queries += [
        ['CAPF_merger_FAQ.html','https://www.idfcfirst.bank.in/idfc-bank-and-capital-first-merger'],
        ['CAPF_effective_issuer.html','https://www.idfcfirst.bank.in/about-us/news-and-media/press-releases/idfc-bank-capital-first-merged-on-Dec-18'],
        ['CAPF_allotment_issuer.pdf','https://www.idfcfirst.bank.in/content/dam/idfcfirstbank/pdf/announcements/IDFC-Bank-Letter-to-Stock-Exchanges-Allotment-of-Shares-pursuant-to-Merger.pdf'],
        ['CAPF_listing_issuer.html','https://www.idfcfirst.bank.in/about-us/news-and-media/press-releases/idfc-first-bank-listing'],
        ['TECHNO_NCLT_order.pdf','https://archive.nclt.gov.in/sites/default/files/final-orders-pdf/TECHNO%20ELECTRIC%20AND%20SIMRAN%20WIND%20PROJECT%2018%20ALD%202018.pdf'],
        ['TECHNO_scheme_SEBI.pdf','https://www.sebi.gov.in/sebi_data/attachdocs/mar-2018/1520223842957.pdf'],
    ]
    (OUT/'finite_evidence_urls.json').write_text(json.dumps(queries,indent=2))
    protected = json.loads((R4/'protected_baseline.json').read_text())
    source_names = ['backtest_engine.py','backtest_accounting.py','momentum_engine.py','app.py','app_backtest.py','data.py','backtest_provenance.py','historical_price_model.py']
    baseline = dict(commit='93483c1',protected=protected,source_sha256={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in source_names},prior_delivery_manifest_sha256={str((r/'delivery_manifest.json').relative_to(ROOT)):hashlib.sha256((r/'delivery_manifest.json').read_bytes()).hexdigest() for r in [R3,R4]},finite_records=len(rows),lineages=59)
    (OUT/'baseline.json').write_text(json.dumps(baseline,indent=2))
    shutil.copyfile(R4/'acquire.py',OUT/'acquire.py')
    print('Frozen 67 decisions / 59 lineages; targeted new evidence queries:',len(queries))
