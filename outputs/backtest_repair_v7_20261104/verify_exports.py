"""Independently replay exported cash, quantities, FIFO lots and NAV."""
import json
import math
from collections import defaultdict, deque
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

OUT = Path(__file__).resolve().parent


def same(a, b):
    assert math.isclose(a, b, abs_tol=5e-8, rel_tol=1e-9), (a, b)


def check_tax(daily, closed, taxed):
    carry_st = carry_lt = 0.0
    st = lt = 0.0
    payable = 0.0
    previous_year = None
    daily_gain = closed.groupby(["date", "long_term"]).gain.sum().to_dict()

    def liability(st, lt, cs, cl):
        excess_st, excess_lt = max(0.0, -st), max(0.0, -lt)
        available_st, available_lt = max(st, 0.0), max(lt, 0.0)
        current_offset = min(excess_st, available_lt)
        available_lt -= current_offset
        excess_st -= current_offset
        cs_use_st = min(cs, available_st)
        cs_use_lt = min(cs-cs_use_st, available_lt)
        net_st = available_st - cs_use_st
        net_lt = available_lt - cs_use_lt
        cl_use = min(cl, net_lt)
        return (net_st*.20 + (net_lt-cl_use)*.125 if taxed else 0.0,
                cs-cs_use_st-cs_use_lt+excess_st, cl-cl_use+excess_lt)

    for row in daily.itertuples():
        year = row.date.year - (row.date.month < 4)
        if previous_year is not None and year != previous_year:
            finalized, carry_st, carry_lt = liability(st, lt, carry_st, carry_lt)
            payable += finalized
            st = lt = 0.0
        previous_year = year
        st += daily_gain.get((row.date, False), 0)
        lt += daily_gain.get((row.date, True), 0)
        same(row.tax_accrued, liability(st, lt, carry_st, carry_lt)[0])
        same(row.tax_payable, payable - row.tax_payment)
        payable = row.tax_payable


def reconcile(prefix, primary):
    daily = pd.read_csv(OUT / (prefix + "_daily.csv"), parse_dates=["date"], float_precision="round_trip")
    trades = pd.read_csv(OUT / (prefix + "_trades.csv"), parse_dates=["date"], float_precision="round_trip")
    closed = pd.read_csv(OUT / (prefix + "_closed_lots.csv"), parse_dates=["date", "acquired"], float_precision="round_trip")
    tax_path = OUT / (prefix + "_tax_events.csv")
    paid = pd.read_csv(tax_path, parse_dates=["date"], float_precision="round_trip").groupby("date").paid.sum() if tax_path.stat().st_size > 3 else {}
    daily["tax_payment"] = daily.date.map(paid).fillna(0.0)
    check_tax(daily, closed, prefix.split("_")[2] == "1")
    cash = 100.0
    lots = defaultdict(deque)
    realised = []
    trade_days = {day: list(group.itertuples()) for day, group in trades.groupby("date", sort=False)}
    for row in daily.itertuples():
        for trade in trade_days.get(row.date, ()):
            if trade.side == "TRANSFER":
                same(trade.cash_change, 0)
                existing = lots.pop(trade.symbol)
                lots[trade.successor].extend(
                    (date, units * trade.ratio, price / trade.ratio) for date, units, price in existing
                )
                lots[trade.successor] = deque(sorted(lots[trade.successor], key=lambda lot: lot[0]))
                continue
            same(trade.gross, trade.quantity * trade.price)
            cash += trade.cash_change
            if trade.side == "BUY":
                same(trade.fee, -trade.cash_change * .001)
                lots[trade.symbol].append((row.date, trade.quantity, -trade.cash_change/trade.quantity))
                continue
            same(trade.fee, trade.gross * .001)
            same(trade.cash_change, trade.gross - trade.fee - trade.brokerage)
            remain = trade.quantity
            while remain > 1e-12 and lots[trade.symbol]:
                acquired, units, basis = lots[trade.symbol].popleft()
                used = min(remain, units)
                proceeds = used * (trade.price - (trade.fee + trade.brokerage)/trade.quantity)
                realised.append((row.date, trade.symbol, acquired, used, used*basis, proceeds,
                                 proceeds-used*basis, row.date > acquired+relativedelta(months=12)))
                remain -= used
                if units - used > 1e-12:
                    lots[trade.symbol].appendleft((acquired, units-used, basis))
            same(remain, 0)
            if not lots[trade.symbol]:
                del lots[trade.symbol]
        cash -= paid.get(row.date, 0)
        same(cash, row.cash)
        quantities = json.loads(row.quantities)
        marks = json.loads(row.marks)
        assert quantities.keys() == lots.keys(), (row.date, quantities.keys(), lots.keys())
        for symbol, quantity in quantities.items():
            same(quantity, sum(lot[1] for lot in lots[symbol]))
        stock = sum(quantity*marks[symbol] for symbol, quantity in quantities.items())
        same(stock, row.stock_value)
        same(cash + stock - row.tax_payable - row.tax_accrued, row.nav)
    assert len(realised) == len(closed)
    for x, y in zip(realised, closed.itertuples(index=False)):
        expected = (y.date,y.symbol,y.acquired,y.quantity,y.basis,y.proceeds,y.gain,y.long_term)
        for i, (actual, target) in enumerate(zip(x, expected)):
            if i in (0, 1, 2, 7):
                assert actual == target
            else:
                same(actual, target)
    assumption_path = OUT / (prefix + "_assumptions.csv")
    source_days = {}
    if assumption_path.stat().st_size > 3:
        assumptions = pd.read_csv(assumption_path)
        assert len(assumptions) == trades.reason.eq("assumed last-price exit").sum()
        for a in assumptions.itertuples():
            assert not a.symbol.startswith("CLAIM:")
            age = (pd.Timestamp(a.cutoff) - pd.Timestamp(a.available_at)).total_seconds()
            same(age, a.age_seconds)
            assert age > 0
            source_days[pd.Timestamp(a.date), a.symbol] = pd.Timestamp(a.source_day)
    for trade in trades.loc[trades.side.isin(["BUY", "SELL"])].itertuples():
        day = source_days.get((trade.date, trade.symbol), trade.date) if trade.reason == "assumed last-price exit" else trade.date
        price, volume = primary.loc[(trade.symbol, day)]
        same(price, trade.price)
        assert volume > 0
    return dict(sessions=len(daily), trade_rows=len(trades), realised_lots=len(realised))


if __name__ == "__main__":
    primary = pd.read_parquet(OUT / "provisional_close_revision.parquet",
                              columns=["research_security_id", "date", "Close", "Volume"])
    primary = primary.set_index(["research_security_id", "date"]).sort_index()
    assert primary.index.is_unique
    output = {p.name.removesuffix("_daily.csv"):reconcile(p.name.removesuffix("_daily.csv"), primary)
              for p in sorted(OUT.glob("*_daily.csv"))}
    assert len(output) == 8, output.keys()
    (OUT / "independent_reconciliation.json").write_text(json.dumps(output, indent=2))
    print(json.dumps(output, indent=2))
