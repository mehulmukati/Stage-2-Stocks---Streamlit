"""Independent exported-trade reconstruction using original accepted quote frames.

Does not call the quantity engine, its mark/weight helpers, or its tax function.
Also audits each daily accrued liability with an independent FY calculation.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

import pandas as pd


def _tax(gains, cf_st, cf_lt, fy, st, lt):
    # signed realized gain totals, positive and negative categories aggregated
    short = gains[0]
    long = gains[1]
    future_st = max(0.0, -short - max(0.0, long))
    future_lt = max(0.0, -long)
    taxable_lt = max(0.0, long + min(0.0, short))
    taxable_st = max(0.0, short)
    balances = [taxable_st, taxable_lt]
    carry = [[], []]
    for category, buckets in enumerate([cf_st, cf_lt]):
        for expiry, amount in sorted(buckets):
            if expiry < fy:
                continue
            for target in [0, 1] if category == 0 else [1]:
                offset = min(amount, balances[target])
                amount -= offset
                balances[target] -= offset
            if amount > 0:
                carry[category].append((expiry, amount))
    if future_st:
        carry[0].append((fy + 8, future_st))
    if future_lt:
        carry[1].append((fy + 8, future_lt))
    return balances[0] * st + balances[1] * lt, carry[0], carry[1]


def reconcile(result, frames, config, tolerance=1e-7):
    reports = {}
    for label, trades in result["execution_ledgers"].items():
        by_day = defaultdict(list)
        for trade in trades:
            by_day[pd.Timestamp(trade["date"])].append(trade)
        cash = 100.0
        quantities = {}
        fifo = defaultdict(list)
        marks = {}
        closed = []
        gains = [0.0, 0.0]
        cf_st = []
        cf_lt = []
        payable = 0.0
        fy = None
        max_cash = max_nav = max_qty = max_tax = 0.0
        fills = 0
        accepted_closes = {s: f.Close.where(f.Close.gt(0)).dropna() for s, f in frames.items()}
        for record in result["daily_ledgers"][label]:
            day = pd.Timestamp(record["date"])
            year = day.year - (day.month < 4)
            if fy is not None and year != fy:
                due, cf_st, cf_lt = _tax(gains, cf_st, cf_lt, fy, config.stcg_rate, config.ltcg_rate)
                payable += due
                gains = [0.0, 0.0]
            fy = year
            for trade in by_day[day]:
                s = trade["symbol"]
                if trade["side"] == "TRANSFER":
                    new = trade["successor"]
                    ratio = trade["ratio"]
                    quantities[new] = quantities.get(new, 0.0) + quantities.pop(s, 0.0) * ratio
                    fifo[new].extend([[q * ratio, p / ratio, d] for q, p, d in fifo.pop(s, [])])
                    fifo[new].sort(key=lambda lot: lot[2])
                    if s in marks:
                        marks[new] = marks[s] / ratio
                    continue
                frame = frames[s]
                assert day in frame.index and pd.notna(frame.at[day, "Close"]), (label, s, day, "missing quote")
                assert frame.at[day, "Volume"] > 0 and bool(frame.at[day, "is_tradable"]), (
                    label,
                    s,
                    day,
                    "unpermitted fill",
                )
                p = float(frame.at[day, "Close"])
                q = trade["quantity"]
                gross = q * p
                assert abs(p - trade["price"]) < tolerance
                assert abs(gross - trade["gross"]) < tolerance
                fills += 1
                if trade["side"] == "BUY":
                    budget = gross / (1 - config.transaction_cost_pct)
                    assert abs(trade["fee"] - (budget - gross)) < tolerance
                    cash -= budget
                    quantities[s] = quantities.get(s, 0.0) + q
                    fifo[s].append([q, budget / q, day])
                else:
                    fee = gross * config.transaction_cost_pct
                    flat = config.brokerage_per_sale * 100 / config.initial_capital
                    assert abs(trade["fee"] - fee) < tolerance and abs(trade["brokerage"] - flat) < tolerance
                    cash += gross - fee - flat
                    quantities[s] -= q
                    remaining = q
                    while remaining > 1e-11:
                        lot = fifo[s][0]
                        used = min(remaining, lot[0])
                        basis = used * lot[1]
                        gain = used * p - basis - (fee + flat) * used / q
                        anniversary = lot[2].date()
                        try:
                            boundary = date(anniversary.year + 1, anniversary.month, anniversary.day)
                        except ValueError:
                            boundary = date(anniversary.year + 1, 2, 28)
                        long = day.date() > boundary
                        gains[int(long)] += gain
                        closed.append(dict(gain=gain, quantity=used, basis=basis))
                        lot[0] -= used
                        remaining -= used
                        if lot[0] < 1e-11:
                            fifo[s].pop(0)
                    if quantities[s] < 1e-10:
                        quantities.pop(s)
            for payment in result["tax_events"][label]:
                if pd.Timestamp(payment["date"]) == day:
                    expected = min(cash, payable)
                    assert abs(payment["paid"] - expected) < tolerance
                    cash -= expected
                    payable -= expected
            accrued, _, _ = _tax(gains, cf_st, cf_lt, fy, config.stcg_rate, config.ltcg_rate)
            for s in quantities:
                eligible = accepted_closes[s].loc[:day]
                if len(eligible):
                    marks[s] = float(eligible.iloc[-1])
                assert abs(marks[s] - record["marks"][s]) < tolerance
            stock = sum(q * marks[s] for s, q in quantities.items())
            nav = cash + stock - payable - accrued
            max_cash = max(max_cash, abs(cash - record["cash"]))
            max_nav = max(max_nav, abs(nav - record["nav"]))
            max_tax = max(max_tax, abs(accrued - record["tax_accrued"]), abs(payable - record["tax_payable"]))
            for s in set(quantities) | set(record["quantities"]):
                max_qty = max(max_qty, abs(quantities.get(s, 0) - record["quantities"].get(s, 0)))
            assert cash >= -tolerance
        assert len(closed) == len(result["closed_lots"][label])
        for independent, actual in zip(closed, result["closed_lots"][label]):
            for k in ["gain", "quantity", "basis"]:
                assert abs(independent[k] - actual[k]) < tolerance, (label, k, independent, actual)
        assert max(max_cash, max_nav, max_qty, max_tax) < tolerance, (label, max_cash, max_nav, max_qty, max_tax)
        reports[label] = dict(
            passed=True,
            fills=fills,
            closed_lots=len(closed),
            daily_rows=len(result["daily_ledgers"][label]),
            max_cash_error=max_cash,
            max_nav_error=max_nav,
            max_quantity_error=max_qty,
            max_tax_error=max_tax,
        )
    return reports
