"""Fractional quantity ledger for adjusted-price research, never a live order writer.

Cash earns zero. Buys consume a cash budget: fee = budget * rate, stock =
budget * (1-rate). Sales pay rate * proceeds. This preserves the historical
per-side cost convention (100 -> 99.9 -> 199.6002 at 0.1%).
Tax is a simplified constant-rate FY model on adjusted units, not nominal law.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

ACCOUNTING_VERSION = "quantity-cash-v2"
POLICIES = {
    "accounting": ACCOUNTING_VERSION,
    "execution": "verified-contemporaneous-close; skip-unfilled-until-next-signal-v1",
    "valuation": "last-accepted-mark-unlimited-with-staleness-v1",
    "cash": "zero-return-no-borrowing-sales-before-buys-v1",
    "cost": "buy-budget-rate-sell-proceeds-rate-all-orders-v1",
    "tax": "simplified-adjusted-unit-FY-accrual-cash-funded-v1",
    "prices": "vendor-auto-adjusted-research-no-extra-dividends-v1",
    "cap": "final-net-NAV-after-fees-and-realized-tax-locked-holdings-disclosed-v2",
    "selection": "independent-books-actual-displacement-slots-v2",
    "holdings_statistics": "actual-end-of-session-count-including-initial-cash-v2",
    "calendar": "completed-exchange-periods-fixed-biweekly-parity-v1",
    "annualization": "elapsed-calendar-days/365.25; risk-free-zero; full-period-drawdown",
    "identity": "dated-evidence-only-v1; legacy-history-uncertified",
}


@dataclass
class QuantityLedger:
    cash: float = 100.0
    lots: dict = field(default_factory=dict)
    marks: dict = field(default_factory=dict)
    mark_dates: dict = field(default_factory=dict)
    trades: list = field(default_factory=list)
    closed_lots: list = field(default_factory=list)
    failures: list = field(default_factory=list)
    daily: list = field(default_factory=list)
    tax_events: list = field(default_factory=list)
    fy: int | None = None
    gains: list = field(default_factory=lambda: [0.0, 0.0, 0.0, 0.0])
    cf_st: list = field(default_factory=list)
    cf_lt: list = field(default_factory=list)
    payable: float = 0.0
    accrued: float = 0.0
    fees: float = 0.0
    brokerage: float = 0.0
    tax_paid: float = 0.0

    def quantities(self):
        return {s: sum(lot["shares"] for lot in ls) for s, ls in self.lots.items() if ls}

    def nav(self):
        return self.cash + sum(q * self.marks[s] for s, q in self.quantities().items()) - self.payable - self.accrued

    def weights(self):
        nav = self.nav()
        return {s: q * self.marks[s] / nav for s, q in self.quantities().items()} if nav > 0 else {}

    def mark(self, day, frames):
        for s in self.quantities():
            frame = frames.get(s)
            if frame is not None and day in frame.index:
                p = frame.at[day, "Close"]
                if pd.notna(p) and np.isfinite(p) and p > 0:
                    self.marks[s] = float(p)
                    self.mark_dates[s] = day

    def transfer(self, day, action):
        old, new = action["old_symbol"], action["successor_symbol"]
        if old not in self.lots:
            return
        ratio = float(action.get("share_ratio", 1.0))
        for lot in self.lots.pop(old):
            self.lots.setdefault(new, []).append(
                {**lot, "shares": lot["shares"] * ratio, "price": lot["price"] / ratio}
            )
        self.lots[new].sort(key=lambda x: x["date"])
        self.marks.setdefault(new, self.marks[old] / ratio)
        self.mark_dates.setdefault(new, self.mark_dates[old])
        self.trades.append(
            dict(
                date=day,
                side="TRANSFER",
                symbol=old,
                successor=new,
                ratio=ratio,
                quantity=0.0,
                gross=0.0,
                fee=0.0,
                brokerage=0.0,
                cash_change=0.0,
            )
        )

    def sell(self, day, s, qty, price, rate, flat, reason="rebalance"):
        held = self.quantities().get(s, 0.0)
        qty = min(qty, held)
        if qty <= 1e-12 or price * qty * (1 - rate) <= flat:
            return 0.0
        gross, fee = qty * price, qty * price * rate
        remaining = qty
        while remaining > 1e-12 and self.lots.get(s):
            lot = self.lots[s][0]
            used = min(remaining, lot["shares"])
            allocated_cost = (fee + flat) * used / qty
            gain = used * (price - lot["price"]) - allocated_cost
            long_term = day > lot["date"] + relativedelta(months=12)
            self.gains[(2 if long_term else 0) + (gain < 0)] += abs(gain)
            self.closed_lots.append(
                dict(
                    date=day,
                    symbol=s,
                    acquired=lot["date"],
                    quantity=used,
                    basis=used * lot["price"],
                    proceeds=used * price - allocated_cost,
                    gain=gain,
                    long_term=long_term,
                    reason=reason,
                )
            )
            lot["shares"] -= used
            remaining -= used
            if lot["shares"] <= 1e-12:
                self.lots[s].pop(0)
        if not self.lots.get(s):
            self.lots.pop(s, None)
        cash_change = gross - fee - flat
        self.cash += cash_change
        self.fees += fee
        self.brokerage += flat
        self.trades.append(
            dict(
                date=day,
                side="SELL",
                symbol=s,
                quantity=qty,
                price=price,
                gross=gross,
                fee=fee,
                brokerage=flat,
                cash_change=cash_change,
                reason=reason,
            )
        )
        return cash_change

    def buy(self, day, s, budget, price, rate):
        budget = min(max(0.0, budget), max(0.0, self.cash - self.payable - self.accrued))
        if budget <= 1e-10:
            return
        gross, fee = budget * (1 - rate), budget * rate
        qty = gross / price
        self.cash -= budget
        self.fees += fee
        self.marks[s], self.mark_dates[s] = price, day
        self.lots.setdefault(s, []).append(dict(date=day, price=budget / qty, shares=qty))
        self.trades.append(
            dict(
                date=day,
                side="BUY",
                symbol=s,
                quantity=qty,
                price=price,
                gross=gross,
                fee=fee,
                brokerage=0.0,
                cash_change=-budget,
            )
        )

    def accrue_tax(self, day, tax_fn, st, lt):
        fy = day.year if day.month >= 4 else day.year - 1
        if self.fy is not None and fy != self.fy:
            due, self.cf_st, self.cf_lt = tax_fn(self.fy, *self.gains, self.cf_st, self.cf_lt, st, lt)
            self.payable += due
            self.gains = [0.0, 0.0, 0.0, 0.0]
            self.accrued = 0.0
        self.fy = fy
        self.accrued = tax_fn(fy, *self.gains, self.cf_st, self.cf_lt, st, lt)[0]

    def settle_tax(self, day, quotes, rate, flat):
        # Outstanding tax is a liability, never implicit borrowing. If there is
        # no fill, leave it payable. Funding sales are genuine new FY realizations.
        if self.payable <= 1e-12:
            return
        for s in sorted(self.quantities()):
            shortfall = self.payable - self.cash
            if shortfall <= 1e-10:
                break
            p = quotes.get(s)
            if p is not None:
                self.sell(day, s, (shortfall + flat) / (p * (1 - rate)), p, rate, flat, "tax funding")
        paid = min(max(0.0, self.cash), self.payable)
        self.cash -= paid
        self.payable -= paid
        self.tax_paid += paid
        if paid:
            self.tax_events.append(dict(date=day, paid=paid, remaining=self.payable))

    def rebalance(self, day, targets, quotes, rate, flat, tax_fn, st, lt):
        before = self.nav()
        quantities = self.quantities()
        values = {s: q * self.marks[s] for s, q in quantities.items()}
        locked = {s for s in values if quotes.get(s) is None}
        for s in sorted(set(targets) | set(values)):
            if quotes.get(s) is None and abs(targets.get(s, 0.0) * before - values.get(s, 0.0)) > 1e-9:
                self.failures.append(
                    dict(
                        date=day,
                        symbol=s,
                        reason="no executable contemporaneous quote",
                        side="SELL" if s in values else "BUY",
                    )
                )

        # Solve target post-fee NAV, leaving locked positions at their actual
        # quantities. Unfilled buy allocations remain cash; there is no retry
        # before the next independent scheduled signal.
        def plan(net):
            desired = {s: w * net for s, w in targets.items() if quotes.get(s) is not None}
            available = max(0.0, net - sum(values[s] for s in locked))
            total = sum(desired.values())
            if total > available and total > 0:
                desired = {s: v * available / total for s, v in desired.items()}
            return desired

        def sell_to_plan(book, desired):
            for s in sorted(set(values) - locked):
                value = desired.get(s, 0.0)
                qty = quantities[s] if value <= 1e-10 else max(0.0, quantities[s] - value / quotes[s])
                book.sell(day, s, qty, quotes[s], rate, flat)
            book.accrue_tax(day, tax_fn, st, lt)

        def final_nav(net):
            # Simulate only mutable accounting state, never copy historical
            # daily/trade exports. Trial sales use the same real FIFO basis,
            # fee allocation and FY offsets as execution; no trial is a fill.
            trial = copy.copy(self)
            trial.lots = {s: [dict(lot) for lot in lots] for s, lots in self.lots.items()}
            trial.gains = list(self.gains)
            trial.trades = []
            trial.closed_lots = []
            desired = plan(net)
            sell_to_plan(trial, desired)
            remaining = trial.quantities()
            buy_fee = sum(
                max(0.0, v - remaining.get(s, 0.0) * quotes[s]) * rate / (1 - rate) for s, v in desired.items()
            )
            return trial.nav() - buy_fee

        # A realized loss can release an existing accrual, so the upper bound
        # is gross equity less already-payable tax, not prior net NAV alone.
        lo, hi = 0.0, max(0.0, before + self.accrued)
        for _ in range(60):
            mid = (lo + hi) / 2
            if mid > final_nav(mid):
                hi = mid
            else:
                lo = mid
        desired = plan(lo)
        start = len(self.trades)
        sell_to_plan(self, desired)
        buys = {
            s: max(0.0, value - self.quantities().get(s, 0.0) * quotes[s]) / (1 - rate) for s, value in desired.items()
        }
        total = sum(buys.values())
        scale = min(1.0, max(0.0, self.cash - self.payable - self.accrued) / total) if total else 0.0
        for s in sorted(buys):
            self.buy(day, s, buys[s] * scale, quotes[s], rate)
        traded = sum(t["gross"] for t in self.trades[start:])
        return traded / before if before > 0 else 0.0

    def snapshot(self, day):
        quantities = self.quantities()
        stock = sum(q * self.marks[s] for s, q in quantities.items())
        nav = self.nav()
        if self.cash < -1e-8 or not np.isfinite(nav):
            raise ValueError("Quantity ledger cash/NAV invariant failed")
        self.daily.append(
            dict(
                date=day,
                cash=self.cash,
                stock_value=stock,
                nav=nav,
                tax_payable=self.payable,
                tax_accrued=self.accrued,
                quantities=quantities.copy(),
                marks={s: self.marks[s] for s in quantities},
                stale_marks={s: str(self.mark_dates[s].date()) for s in quantities if self.mark_dates[s] < day},
            )
        )
