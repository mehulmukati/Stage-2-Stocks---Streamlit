"""Agreed price-only momentum backtest. Source databases opened read-only.
Run from the project root: python backtest/available_observations/run_backtest.py
"""
from pathlib import Path
from dataclasses import dataclass, field
import sqlite3, json, re, hashlib, math, argparse
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent.parent
OUT=HERE/'results'
CFG=dict(coverage_fraction=None,execution_mode='hourly',midday_only=False,regime_filter=True,rebalance_frequency='weekly',capital=10_000_000.,start='2016-01-01',holdings=20,wrh=60,
 missing_execution_policy='exit_at_last_known_price',
 rebalance_weekday='THU',monthly_occurrence='last',
 sharpe_sessions=[63,126],lastmsr_calendar_days=30,lastmsr_rank=650,
 high_sessions=252,max_below_high=.25,max_nominal_price=20000.,stop=.20,
 regime_index='NIFTY500MOMENTM50',regime_ma=50,normal_weight=.05,weak_weight=.025,
 fees_per_side=.00125,slippage_per_side=.001,ltcg_days=365,stcg_rate=.20,ltcg_rate=.125,
 execution_candle_labels=['11:15','12:15','13:15'],cash_interest=0.,liquidity_filter=False,
 tax_loss_offsets='within category and calendar year only',tax_method='FIFO; net gains after fees',
 dividends='no additional adjustment and no cash credit; embedded vendor adjustments retained',
 unit_model='fractional split-adjusted units; no new dividend fixes',
 price_cap='nominal-price proxy undoing locally documented subsequent splits/bonuses through data end',
 allocation='proportional available-cash scaling across replacement/top-up orders; no leverage or forced trims',
 tax_funding='reserve estimated current-year realised-gain tax in cash; deduct at year end / final partial-year date',
 signals='prior completed session; full lookback age retained; no completeness threshold; >=2 valid returns; no return filling; execution-day availability required',
 stop_timing='selected execution candle close; same-close fill approximation',
 same_day_reentry=False,topup_policy='weak-regime entries only, retried on good-regime rebalances until funded')
BENCH=['NIFTY 50','NIFTY 500','NIFTY500MOMENTM50']

@dataclass
class Lot:
    date: pd.Timestamp
    units: float
    fill: float
    fee_unit: float
    id: int

@dataclass
class Position:
    lots: list=field(default_factory=list)
    recovery_pending: bool=False
    @property
    def units(self): return sum(x.units for x in self.lots)
    @property
    def average_fill(self): return sum(x.units*x.fill for x in self.lots)/self.units

def tax_amount(st,lt):
    return CFG['stcg_rate']*max(st,0.)+CFG['ltcg_rate']*max(lt,0.)

def sale_lots(position,date,price):
    """Entire-position sale, enumerated FIFO; each top-up retains its own holding period."""
    fill=price*(1-CFG['slippage_per_side']);fee=fill*CFG['fees_per_side']
    result=[]
    for l in sorted(position.lots,key=lambda x:(x.date,x.id)):
        days=(date-l.date).days
        result.append(dict(lot_id=l.id,entry_date=l.date,exit_date=date,holding_days=days,
          units=l.units,entry_fill=l.fill,exit_fill=fill,buy_fee=l.units*l.fee_unit,
          sell_fee=l.units*fee,pnl=l.units*(fill-fee-l.fill-l.fee_unit),
          category='LTCG' if days>=CFG['ltcg_days'] else 'STCG'))
    return result

def proportional_budgets(desired,available):
    total=sum(desired.values())
    scale=min(1.,max(0.,available)/total) if total else 0.
    return {k:v*scale for k,v in desired.items()}

def rebalance_dates(cal,frequency='weekly',weekday='THU',monthly_occurrence='last'):
    available=set(cal); result={}
    if frequency=='monthly':
        # Calendar anchors are chosen before holiday fallback, including for a
        # truncated final month. A later anchor must not move to an earlier week.
        anchors=pd.date_range(cal.min().to_period('M').start_time,
          cal.max()+pd.offsets.MonthEnd(0),freq='W-'+weekday)
        groups={}
        for day in anchors:groups.setdefault(day.to_period('M'),[]).append(day)
        nth={'first':0,'second':1,'third':2,'last':-1}[monthly_occurrence]
        anchors=[days[nth] for days in groups.values() if days[nth]<=cal.max()]
    else:
        anchors=pd.date_range(cal.min(),cal.max(),freq='W-'+weekday)
    for th in anchors:
        # Generalize Thursday/Wednesday/Tuesday to the anchor and two prior
        # weekdays; Monday falls back to Friday, then Thursday.
        for d in reversed(pd.bdate_range(end=th,periods=3)):
            if d in available:
                result[d]=th;break
    return result

def split_events(path,end):
    ca=pd.read_csv(path).drop_duplicates(['symbol','exDate','subject'])
    events=[];unparsed=[]
    money=r'r[se]\.?\s*([\d]+(?:\.\d+)?)'
    for r in ca.itertuples():
        date=pd.to_datetime(r.exDate,format='%d-%b-%Y',errors='coerce')
        if pd.isna(date) or date>end or r.series not in ('EQ','BE','BZ'):continue
        subject=re.sub(r'\s+',' ',str(r.subject).lower());factor=1.
        split=bool(re.search(r'split|splt|sub-?division',subject));bonus='bonus' in subject and 'debenture' not in subject and 'preference' not in subject
        if split:
            m=re.search(money+r'\D{0,40}?to\s*'+money,subject)
            if m:factor*=float(m.group(2))/float(m.group(1))
            else:unparsed.append(dict(symbol=r.symbol,date=date,subject=subject))
        if bonus and 'ncrps' not in subject:
            m=re.search(r'bonus\s*(?:issue\s*)?(?:shares?\s*)?-?\s*(\d+)\s*:\s*(\d+)',subject)
            if m:factor*=float(m.group(2))/(float(m.group(1))+float(m.group(2)))
            else:unparsed.append(dict(symbol=r.symbol,date=date,subject=subject))
        if factor!=1:events.append(dict(symbol=r.symbol,date=date,factor=factor,subject=subject))
    return pd.DataFrame(events),pd.DataFrame(unparsed)

def available_signals(C,H):
    r=C.pct_change(fill_method=None)
    def sh(n):
        minimum=2
        # Full calendar lookback required; tolerance only controls observations inside it.
        full_history=C.notna().cummax().rolling(n+1,min_periods=n+1).sum().eq(n+1)
        return (r.rolling(n,min_periods=minimum).mean()/r.rolling(n,min_periods=minimum).std(ddof=1)*np.sqrt(252)).where(full_history).replace([np.inf,-np.inf],np.nan)
    components=[sh(n) for n in CFG['sharpe_sessions']]
    score=sum(components)/len(components)
    win=r.rolling('30D',min_periods=2)
    last=(win.mean()/win.std(ddof=1)*np.sqrt(252)).replace([np.inf,-np.inf],np.nan)
    full_year=C.notna().cummax().rolling(252,min_periods=252).sum().eq(252)
    high=H.rolling(252,min_periods=1).max().where(full_year)
    return score,last,high

def select_hourly_quotes(bars):
    """Prefer 11:15,12:15,13:15; then later bars, then latest earlier bar."""
    bars=bars.copy()
    def priority(label):
        if label in ['11:15','12:15','13:15']:return ['11:15','12:15','13:15'].index(label)
        minute=int(label[:2])*60+int(label[3:5])
        return 10+minute if label>'13:15' else 3000-minute
    bars['priority']=bars.label.map(priority)
    return bars.sort_values(['date','symbol','priority']).drop_duplicates(['date','symbol'])

def candle_available_at(timestamp,source):
    timestamp=pd.Timestamp(timestamp)
    if source=='daily':return timestamp.normalize()+pd.Timedelta(hours=15,minutes=30)
    end=timestamp+pd.Timedelta(hours=1)
    if timestamp.strftime('%H:%M')=='15:15':
        end=timestamp.normalize()+pd.Timedelta(hours=15,minutes=30)
    return end

def quote_metadata(date,label,quote):
    source=quote.get('price_source',CFG['execution_mode'])
    stamp=pd.Timestamp(quote.get('price_source_timestamp',str(date.date())+' '+label))
    known=candle_available_at(stamp,source)
    executed=candle_available_at(str(date.date())+' '+label,CFG['execution_mode'])
    assert known<=executed,(stamp,known,executed)
    return dict(assumed_last_price_exit=bool(quote.get('assumed_last_price_exit',False)),
      price_source=source,price_source_timestamp=stamp,price_available_at=known,
      execution_available_at=executed,price_age_days=(executed-known).total_seconds()/86400)

class LastKnownPrices:
    """Read-only, cached lookup across both sources using candle completion time."""
    def __init__(self,directory):
        self.daily=sqlite3.connect((Path(directory)/'kite_daily.db').as_uri()+'?mode=ro',uri=True)
        self.hourly=sqlite3.connect((Path(directory)/'kite_hourly.db').as_uri()+'?mode=ro',uri=True)
        self.cache={}
    def close(self):
        self.daily.close();self.hourly.close()
    def __call__(self,symbol,date,label):
        key=(symbol,pd.Timestamp(date),label,CFG['execution_mode'])
        if key in self.cache:return self.cache[key].copy()
        cutoff=candle_available_at(str(date.date())+' '+label,CFG['execution_mode'])
        candidates=[]
        for ts,price in self.hourly.execute('SELECT ts,close FROM hourly WHERE symbol=? AND ts<=? AND close>0 AND volume>0 ORDER BY ts DESC',
              (symbol,cutoff.strftime('%Y-%m-%d %H:%M'))):
            stamp=pd.Timestamp(ts)
            if candle_available_at(stamp,'hourly')<=cutoff:
                candidates.append(dict(close=float(price),price_source='hourly',price_source_timestamp=stamp));break
        latest_day=cutoff.normalize() if cutoff.hour>=16 or (cutoff.hour==15 and cutoff.minute>=30) else cutoff.normalize()-pd.Timedelta(days=1)
        row=self.daily.execute('SELECT date,close FROM daily WHERE symbol=? AND date<=? AND close>0 AND volume>0 ORDER BY date DESC LIMIT 1',
              (symbol,latest_day.strftime('%Y-%m-%d'))).fetchone()
        if row:candidates.append(dict(close=float(row[1]),price_source='daily',price_source_timestamp=pd.Timestamp(row[0])+pd.Timedelta(hours=15,minutes=30)))
        if not candidates:raise ValueError(f'No known historical price for held stock {symbol} at {cutoff}')
        chosen=max(candidates,key=lambda q:(candle_available_at(q['price_source_timestamp'],q['price_source']),q['price_source']==CFG['execution_mode']))
        chosen.update(label=label,assumed_last_price_exit=True)
        self.cache[key]=chosen
        return chosen.copy()

def missing_quote(data,symbol,date,label):
    if 'last_price_lookup' in data:return data['last_price_lookup'](symbol,date,label)
    # In-memory simulations use prior completed daily closes only.
    available=data['C'].loc[data['C'].index<date,symbol].dropna()
    if available.empty:raise ValueError(f'No historical price for {symbol}')
    return dict(label=label,close=float(available.iloc[-1]),assumed_last_price_exit=True,
      price_source='daily',price_source_timestamp=available.index[-1]+pd.Timedelta(hours=15,minutes=30))

def load():
    c=sqlite3.connect((ROOT/'kite_data/kite_daily.db').as_uri()+'?mode=ro',uri=True)
    d=pd.read_sql('select symbol,date,high,close,volume from daily',c,parse_dates=['date'])
    stocks=sorted(x[0] for x in c.execute("select symbol from instruments where kind='stock'"));c.close()
    cal=pd.DatetimeIndex(sorted(d.loc[d.symbol=='NIFTY 50','date'].unique()))
    close=d.pivot(index='date',columns='symbol',values='close').reindex(cal)
    C=close[stocks];H=d.pivot(index='date',columns='symbol',values='high').reindex(index=cal,columns=stocks)
    I=close[BENCH];F=pd.DataFrame(1.,index=cal,columns=stocks)
    events,unparsed=split_events(ROOT/'kite_data/nse_corporate_actions_2014_2026.csv',cal[-1])
    for e in events.itertuples():
        if e.symbol in F: F.loc[F.index<e.date,e.symbol]*=e.factor
    raw=C/F
    score,last,high=available_signals(C,H)
    lastrank=last.rank(axis=1,ascending=False,method='first')
    eligible=score.notna()&C.notna()&(raw<=20000)&(C>=.75*high)
    regime=I[CFG['regime_index']]>=I[CFG['regime_index']].rolling(50,min_periods=50).mean()
    h=sqlite3.connect((ROOT/'kite_data/kite_hourly.db').as_uri()+'?mode=ro',uri=True)
    session_types=pd.read_sql("SELECT substr(ts,1,10) date,max(CASE WHEN substr(ts,12) IN ('11:15','12:15','13:15') THEN 1 ELSE 0 END) midday FROM hourly GROUP BY substr(ts,1,10)",h)
    special_only=set(pd.to_datetime(session_types.loc[session_types.midday==0,'date']))
    # Evening-only sessions cannot execute the stated midday strategy. Entirely missing
    # dates are NOT inferred to be holidays merely because the data are absent.
    sched=rebalance_dates(cal[~cal.isin(special_only)],CFG['rebalance_frequency'],
      CFG['rebalance_weekday'],CFG['monthly_occurrence']); dates=[x for x in sched if x>=pd.Timestamp(CFG['start'])]
    h.execute('CREATE TEMP TABLE wanted(date TEXT PRIMARY KEY)')
    h.executemany('INSERT INTO wanted VALUES (?)',[(x.strftime('%Y-%m-%d'),) for x in dates])
    if CFG['execution_mode']=='hourly':
        bars=pd.read_sql("SELECT symbol,ts,close,volume FROM hourly h JOIN wanted w ON substr(h.ts,1,10)=w.date",h)
        bars['date']=pd.to_datetime(bars.ts.str[:10]);bars['label']=bars.ts.str[11:]
        bars=bars[bars.symbol.isin(stocks)&bars.close.gt(0)&bars.volume.gt(0)]
        if CFG['midday_only']:
            bars=bars[bars.label.isin(['11:15','12:15','13:15'])]
        first=select_hourly_quotes(bars)
        CFG['execution_candle_labels']=sorted(first.label.unique())
    else:
        bars=d[d.symbol.isin(stocks)&d.date.isin(dates)&d.close.gt(0)&d.volume.gt(0)].copy()
        bars['label']='15:30'
        first=bars
        CFG['execution_candle_labels']=['15:30']
    h.close()
    quotes={date:g.set_index('symbol')[['label','close']].to_dict('index') for date,g in first.groupby('date')}
    events.to_csv(OUT/'split_bonus_events_for_price_cap.csv',index=False)
    unparsed.to_csv(OUT/'unparsed_split_bonus_events.csv',index=False)
    coverage=dict(stock_count=len(stocks),calendar_start=str(cal[0].date()),calendar_end=str(cal[-1].date()),
      full_signal_eligible_counts={str(t.date()):int(eligible.loc[t].sum()) for t in cal if t in sched},
      unparsed_split_bonus_events=len(unparsed),execution_rows=len(bars),evening_only_sessions=[str(x.date()) for x in sorted(special_only)])
    return dict(C=C,H=H,I=I,F=F,raw=raw,score=score,last=lastrank,last_scores=last,high=high,eligible=eligible,regime=regime,sched=sched,quotes=quotes,coverage=coverage,
      last_price_lookup=LastKnownPrices(ROOT/'kite_data'))

def run(data,taxed):
    C=data['C'];cal=C.index;days=cal[cal>=pd.Timestamp(CFG['start'])];marks=C.ffill()
    positions={};cash=CFG['capital'];st=lt=0.;lotno=0;total_paid=0.
    daily=[];trades=[];closed=[];rebalances=[];taxrows=[];holdings=[];warnings=[]
    first_fill=None;costs=slippage=0.;blocked=0
    def reserve():return tax_amount(st,lt) if taxed else 0.
    def mv(prices):return sum(p.units*float(prices[s]) for s,p in positions.items())
    for di,t in enumerate(days):
        if di and t.year!=days[di-1].year:st=lt=0.
        i=cal.get_loc(t);signal=cal[i-1] if i else None
        yearend=di==len(days)-1 or days[di+1].year!=t.year
        if t in data['sched'] and signal is not None:
            good=not CFG['regime_filter'] or bool(data['regime'].loc[signal])
            q=data['quotes'].get(t,{})
            available=pd.Series(C.columns.isin(q),index=C.columns)
            if 'last_scores' in data:
                last_rank=data['last_scores'].loc[signal].where(available&C.loc[signal].notna()).rank(ascending=False,method='first')
            else:last_rank=data['last'].loc[signal]
            valid=data['eligible'].loc[signal]&available&(last_rank<=650)
            scores=data['score'].loc[signal]
            ranked=list(scores[valid].sort_values(ascending=False,kind='stable').index)
            ranks={s:j+1 for j,s in enumerate(ranked)}
            prior=marks.loc[signal].to_dict();known=prior.copy();q=dict(data['quotes'].get(t,{}))
            planned={}
            for s,p in positions.items():
                reasons=[]
                if not valid.get(s,False):
                    if data['raw'].at[signal,s]>20000:reasons.append('price_above_20000')
                    if not np.isfinite(last_rank.get(s,np.nan)) or last_rank.get(s,np.nan)>650:reasons.append('LASTMSR_or_missing')
                    if not np.isfinite(data['high'].at[signal,s]) or C.at[signal,s]<.75*data['high'].at[signal,s]:reasons.append('below_1y_high_or_missing')
                    if not np.isfinite(scores[s]) or not np.isfinite(C.at[signal,s]):reasons.append('insufficient_or_missing_data')
                    if not reasons:reasons=['entry_filter_failed']
                elif ranks[s]>CFG['wrh']:reasons.append(f"filtered_rank_above_{CFG['wrh']}")
                if reasons:planned[s]=';'.join(reasons)
            missing_held=[s for s in positions if s not in q]
            primary_label='11:15' if CFG['execution_mode']=='hourly' else '15:30'
            for s in missing_held:
                q[s]=missing_quote(data,s,t,primary_label)
                planned[s]='missing_execution_data;last_available_price_exit;'+planned.get(s,'')
            sold=set();attempted=set();day_buys=day_sells=0;funding_scaled=False
            for label in sorted(set(CFG['execution_candle_labels'])|({primary_label} if missing_held else set())):
                current={s:float(v['close']) for s,v in q.items() if v['label']==label}
                known.update(current)
                # Sells first at this timestamp. Later fallback sale proceeds cannot finance earlier buys.
                for s in list(positions):
                    if s not in current:continue
                    p=positions[s];reason=planned.get(s,'')
                    if current[s]<=.8*p.average_fill:reason=(reason+';' if reason else '')+'stop_20pct'
                    if not reason:continue
                    lots=sale_lots(p,t,current[s]);units=p.units;fill=current[s]*.999;fee=units*fill*.00125
                    cash+=units*fill-fee;costs+=fee;slippage+=units*current[s]*.001
                    pnl=sum(z['pnl'] for z in lots)
                    for z in lots:
                        z.update(symbol=s,reason=reason);closed.append(z)
                        if z['category']=='STCG':st+=z['pnl']
                        else:lt+=z['pnl']
                    trades.append(dict(date=t,signal_date=signal,candle_label=label,side='SELL',symbol=s,units=units,
                      reference_price=current[s],fill_price=fill,fees=fee,cash_flow=units*fill-fee,reason=reason,
                      filtered_rank=ranks.get(s,np.nan),lastmsr=last_rank.get(s,np.nan),realised_pnl=pnl,cash_after=cash,
                      **quote_metadata(t,label,q[s])))
                    if q[s].get('assumed_last_price_exit',False):
                        warnings.append(dict(date=t,symbol=s,issue='forced_exit_last_available_price',reason=reason,
                          reference_price=current[s],**quote_metadata(t,label,q[s])))
                    del positions[s];sold.add(s);day_sells+=1
                capacity=CFG['holdings']-len(positions)
                # Unfilled higher-ranked names wait for their fallback; known exhausted names are skipped.
                candidates=[s for s in ranked if s not in positions and s not in sold and s not in attempted]
                candidates=[s for s in candidates if s in q and q[s]['label']>=label][:capacity]
                target_equity=cash+mv(known)
                desired={};kind={}
                for s in candidates:
                    desired[s]=target_equity*(CFG['normal_weight'] if good else CFG['weak_weight'])*(1+.00125)
                    kind[s]='ENTRY'
                if good:
                    for s,p in positions.items():
                        if p.recovery_pending and s not in planned and s in q and q[s]['label']>=label and s not in attempted:
                            need=CFG['normal_weight']*target_equity-p.units*known[s]
                            if need>1e-6:desired[s]=need*(1+.00125);kind[s]='TOPUP'
                            else:p.recovery_pending=False
                available=max(0.,cash-reserve());budgets=proportional_budgets(desired,available)
                funding_scaled |= sum(desired.values())>available+1e-6
                for s in desired:
                    if s not in current:continue
                    attempted.add(s);budget=budgets[s]
                    if budget<.01:blocked+=1;continue
                    ref=current[s];fill=ref*1.001;units=budget/(fill*1.00125);fee=units*fill*.00125
                    cash-=budget;costs+=fee;slippage+=units*ref*.001;lotno+=1
                    if s not in positions:positions[s]=Position(recovery_pending=not good)
                    p=positions[s];p.lots.append(Lot(t,units,fill,fill*.00125,lotno))
                    if kind[s]=='TOPUP' and units*fill>=desired[s]/1.00125*(1-1e-8):p.recovery_pending=False
                    trades.append(dict(date=t,signal_date=signal,candle_label=label,side='BUY',symbol=s,units=units,
                      reference_price=ref,fill_price=fill,fees=fee,cash_flow=-budget,reason=kind[s],
                      filtered_rank=ranks[s],lastmsr=last_rank.get(s,np.nan),realised_pnl=0.,cash_after=cash,
                      lot_id=lotno,regime_good=good,**quote_metadata(t,label,q[s])))
                    first_fill=first_fill or t;day_buys+=1
                assert cash>=-1e-5,(t,cash)
                assert cash+1e-5>=reserve(),(t,cash,reserve())
                assert len(positions)<=CFG['holdings']
            assert not set(missing_held).intersection(positions)
            rebalances.append(dict(date=t,signal_date=signal,scheduled_anchor=data['sched'][t],scheduled_thursday=data['sched'][t],regime_good=good,
              ranked_eligible=len(ranked),positions=len(positions),buys=day_buys,sells=day_sells,
              cash=cash,tax_reserved=reserve(),funding_scaled=funding_scaled))
        day_marks=marks.loc[t]
        value=mv(day_marks);pre_tax=cash+value;payment=0.
        if yearend:
            liability=tax_amount(st,lt)
            payment=liability if taxed else 0.
            assert cash+1e-5>=payment
            cash-=payment;total_paid+=payment
            taxrows.append(dict(year=t.year,through_date=t,partial_year=t.month!=12,
              net_stcg=st,net_ltcg=lt,stcg_tax=max(st,0)*.2,ltcg_tax=max(lt,0)*.125,
              computed_tax=liability,tax_deducted=payment,equity_before_payment=pre_tax,equity_after_payment=cash+value))
        equity=cash+value
        weights={s:p.units*day_marks[s]/equity for s,p in positions.items()}
        stale=[s for s in positions if pd.isna(C.at[t,s])]
        daily.append(dict(date=t,equity=equity,cash=cash,market_value=value,positions=len(positions),
          invested=value/equity,max_weight=max(weights.values(),default=0),tax_reserve=0 if yearend else reserve(),
          tax_paid=payment,stale_marks=len(stale)))
        if t in data['sched'] or t==days[-1]:
            for s,p in positions.items():holdings.append(dict(date=t,symbol=s,units=p.units,mark=day_marks[s],
              value=p.units*day_marks[s],weight=weights[s],average_fill=p.average_fill,recovery_pending=p.recovery_pending))
    eq=pd.DataFrame(daily).set_index('date')
    tr=pd.DataFrame(trades) if trades else pd.DataFrame(columns=['cash_flow','side','realised_pnl','signal_date','date','lastmsr'])
    cl=pd.DataFrame(closed) if closed else pd.DataFrame(columns=['pnl'])
    result=dict(equity=eq,trades=tr,closed_lots=cl,rebalances=pd.DataFrame(rebalances),
      taxes=pd.DataFrame(taxrows),holdings=pd.DataFrame(holdings),warnings=pd.DataFrame(warnings),
      costs=costs,slippage=slippage,first_fill=str(first_fill.date()) if first_fill else None,taxes_paid=total_paid,
      blocked_cash_orders=blocked)
    # Independent ledger reconciliation to terminal cash and realised FIFO gains.
    assert abs(CFG['capital']+tr.cash_flow.sum()-total_paid-cash)<.01
    assert abs(tr.loc[tr.side=='SELL','realised_pnl'].sum()-cl.pnl.sum())<.01
    assert (tr.signal_date<tr.date).all()
    assert (tr.loc[tr.side=='BUY','lastmsr']<=650).all()
    assert eq.cash.min()>=-1e-5
    return result

def metrics(eq):
    returns=eq.pct_change(fill_method=None).dropna();years=(eq.index[-1]-eq.index[0]).days/365.25
    dd=eq/eq.cummax()-1
    return dict(start=str(eq.index[0].date()),end=str(eq.index[-1].date()),years=years,final_equity=float(eq.iloc[-1]),
      cagr=float((eq.iloc[-1]/CFG['capital'])**(1/years)-1),max_drawdown=float(dd.min()),
      max_drawdown_date=str(dd.idxmin().date()),volatility=float(returns.std(ddof=1)*np.sqrt(252)),
      sharpe_zero_rf=float(returns.mean()/returns.std(ddof=1)*np.sqrt(252)))

def yearly(result):
    rows=[];opening=CFG['capital']
    for year,g in result['equity'].groupby(result['equity'].index.year):
        tx=result['taxes'].set_index('year').loc[year];ending=float(g.equity.iloc[-1])
        rows.append(dict(year=year,opening_equity=opening,ending_equity=ending,
          return_before_annual_tax=(ending+tx.tax_deducted)/opening-1,
          tax_drag=tx.tax_deducted/opening,return_after_tax=ending/opening-1,
          net_stcg=tx.net_stcg,net_ltcg=tx.net_ltcg,tax=tx.tax_deducted,partial_year=bool(tx.partial_year)))
        opening=ending
    return pd.DataFrame(rows)

def main():
    global OUT
    parser=argparse.ArgumentParser()
    parser.add_argument('--execution',choices=['hourly','daily'],default='hourly')
    parser.add_argument('--out',default=None)
    parser.add_argument('--no-regime',action='store_true',help='Use normal allocation at every rebalance')
    parser.add_argument('--rebalance',choices=['weekly','monthly'],default='weekly')
    parser.add_argument('--midday-only',action='store_true',help='Hourly fills only at 11:15, 12:15 or 13:15 candle closes')
    parser.add_argument('--weekday',choices=['MON','TUE','WED','THU','FRI'],default='THU')
    parser.add_argument('--monthly-occurrence',choices=['first','second','third','last'],default='last')
    parser.add_argument('--holdings',type=int,default=20)
    parser.add_argument('--wrh',type=int,default=60)
    parser.add_argument('--sharpe-months',type=int,nargs='+',choices=[1,3,6,9,12],default=[3,6],
        help='Equally weighted Sharpe horizons, each month represented by 21 trading sessions')
    args=parser.parse_args()
    CFG['execution_mode']=args.execution
    CFG['regime_filter']=not args.no_regime
    CFG['rebalance_frequency']=args.rebalance
    CFG['midday_only']=args.midday_only
    CFG['rebalance_weekday']=args.weekday
    CFG['monthly_occurrence']=args.monthly_occurrence
    if args.holdings<1 or args.wrh<args.holdings:
        parser.error('holdings must be positive and WRH must be at least the holdings count')
    CFG['holdings']=args.holdings
    CFG['wrh']=args.wrh
    CFG['normal_weight']=1/args.holdings
    CFG['weak_weight']=.5/args.holdings
    if len(set(args.sharpe_months))!=len(args.sharpe_months):
        parser.error('Sharpe horizons must be unique')
    CFG['sharpe_sessions']=[21*m for m in args.sharpe_months]
    OUT=Path(args.out).resolve() if args.out else HERE/('results_'+args.execution)
    OUT.mkdir(parents=True,exist_ok=True)
    print('Loading candles and building prior-session signals...',flush=True);data=load()
    print('Simulating without tax...',flush=True);pre=run(data,False)
    print('Simulating with annual realised-gain taxes...',flush=True);post=run(data,True)
    data['last_price_lookup'].close()
    for label,result in [('pretax',pre),('posttax',post)]:
        for key in ['equity','trades','closed_lots','rebalances','taxes','holdings','warnings']:
            result[key].to_csv(OUT/f'{label}_{key}.csv',index=key=='equity')
    yr=yearly(post);yr['pretax_counterfactual_return']=yearly(pre).return_after_tax.values
    for b in BENCH:
        s=data['I'][b].loc[post['equity'].index].ffill();opening=s.iloc[0];vals=[]
        for y,g in s.groupby(s.index.year):vals.append(g.iloc[-1]/opening-1);opening=g.iloc[-1]
        yr[b]=vals
    yr.to_csv(OUT/'calendar_year_results.csv',index=False)
    benchmark={}
    for b in BENCH:
        s=data['I'][b].loc[post['equity'].index].ffill();benchmark[b]=metrics(s/s.iloc[0]*CFG['capital'])
    summary=dict(config=CFG,coverage=data['coverage'],pretax=metrics(pre['equity'].equity),posttax=metrics(post['equity'].equity),
      benchmarks=benchmark,first_trade=post['first_fill'],total_tax=post['taxes_paid'],fees=post['costs'],slippage=post['slippage'],
      buy_orders=int((post['trades'].side=='BUY').sum()),sell_orders=int((post['trades'].side=='SELL').sum()),
      closed_lots=len(post['closed_lots']),mean_invested=float(post['equity'].invested.mean()),
      max_position_weight=float(post['equity'].max_weight.max()),stale_mark_days=int((post['equity'].stale_marks>0).sum()),
      missing_execution_exits=0,forced_missing_data_exits=int(post['trades'].assumed_last_price_exit.sum()),
      funding_scaled_rebalances=int(post['rebalances'].funding_scaled.sum()),
      execution_label_counts=post['trades'].candle_label.value_counts().to_dict(),
      rebalance_weekday_counts=post['rebalances'].date.dt.day_name().value_counts().to_dict(),
      validations=['cash ledger reconciles','FIFO realised gains reconcile','no negative cash','tax reserve funded',
      f"at most {CFG['holdings']} holdings",'all signal dates precede fills','all buys LASTMSR <=650'])
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2,default=str),encoding='utf-8')
    data['I'].loc[post['equity'].index].to_csv(OUT/'benchmark_closes.csv')
    print(json.dumps({k:v for k,v in summary.items() if k not in ['config','coverage','benchmarks']},indent=2),flush=True)
    print(yr.to_string(index=False),flush=True)

if __name__=='__main__':main()
