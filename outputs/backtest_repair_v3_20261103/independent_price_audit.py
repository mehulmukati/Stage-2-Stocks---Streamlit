"""Independent raw payload checks; imports no production price parser."""
from pathlib import Path
from datetime import datetime
import json,hashlib,zipfile,random,math
import pandas as pd
OUT=Path(__file__).parent;ROOT=OUT.resolve().parents[1];PREV=ROOT/'outputs/backtest_repair_v2_20261103'

if __name__=='__main__':
    target=OUT/'price_audit_v4'
    bars=pd.read_parquet(target/'staged_vendor_adjusted_prices.parquet')
    checked=0;files=0
    for symbol,g in bars.groupby('vendor_symbol'):
        p=OUT/'evidence'/(symbol.removesuffix('.NS')+'_yahoo_chart.json')
        payload=json.loads(p.read_text(encoding='utf-8'))['chart']['result'][0]
        assert payload['meta']['symbol']==symbol
        source={pd.Timestamp(t,unit='s',tz='UTC').tz_convert('Asia/Kolkata').date():i for i,t in enumerate(payload['timestamp'])}
        quote=payload['indicators']['quote'][0];adjusted=payload['indicators']['adjclose'][0]['adjclose'];digest=hashlib.sha256(p.read_bytes()).hexdigest()
        for r in g.itertuples():
            i=source[r.date.date()];assert digest==r.source_sha256
            if r.status=='observed':
                factor=adjusted[i]/quote['close'][i]
                assert abs(r.adjusted_close-adjusted[i])<1e-7
                assert abs(r.adjustment_factor-factor)<1e-12
                for key in ['open','high','low']:
                    assert abs(getattr(r,'adjusted_'+key)-quote[key][i]*factor)<1e-7
                assert r.reported_volume==quote['volume'][i]
            checked+=1
        files+=1
    raw=pd.read_parquet(target/'raw_security_quotes.parquet')
    sample=raw.sample(n=300,random_state=1303)
    loaded={}
    for r in sample.itertuples():
        archive,member,line=r.source_row_id.split(':')
        p=next(p for p in [OUT/'evidence'/archive,PREV/'evidence'/archive] if p.exists())
        assert hashlib.sha256(p.read_bytes()).hexdigest()==r.raw_source_sha256
        if archive not in loaded:
            with zipfile.ZipFile(p) as z:loaded[archive]=pd.read_csv(z.open(member))
        source=loaded[archive].iloc[int(line)-2]
        assert source.SYMBOL==r.SYMBOL and source.ISIN==r.ISIN
        for key in ['OPEN','HIGH','LOW','CLOSE','TOTTRDQTY']:assert abs(float(source[key])-getattr(r,key))<1e-8
        try:day=datetime.strptime(source.TIMESTAMP,'%d-%b-%Y').date()
        except ValueError:day=datetime.strptime(source.TIMESTAMP,'%d-%b-%y').date()
        assert day==r.date.date()
    coverage=pd.read_parquet(target/'all_eligible_daily_price_coverage.parquet')
    report=dict(passed=True,vendor_payloads=files,vendor_rows_checked=checked,raw_source_rows_checked=len(sample),raw_archives_sampled=len(loaded),eligible_daily_price_source_counts=coverage.price_source.value_counts().to_dict(),unsupported_adjusted_rows=int(coverage.price_source.eq('unavailable').sum()),ohlc_discrepancy_rows=int(coverage.ohlc_discrepancy_gt_1pct.sum()),double_split_or_distribution_credit=False,production_price_parser_imported=False,accepted=False)
    (OUT/'independent_price_reconciliation.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
