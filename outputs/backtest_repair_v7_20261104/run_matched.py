"""Standalone, explicitly uncertified comparison. Run from the repository root."""
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

import backtest_engine as app
from backtest_accounting import QuantityLedger
from equity_data import session_frame
from historical_price_model import primary_event_factor
from research_entitlements import apply_reviewed_decisions
from research_execution import session_close_times
from research_ownership import merger_claim

OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[1]
R3, R4, R5, R6 = [OUT.parent / f"backtest_repair_v{v}_20261103" for v in range(3, 7)]
NATIVE = ROOT / "MoMo/backtest/available_observations/run_backtest.py"
POLICY = ROOT / "MoMo/backtest/MISSING_DATA_POLICY.md"
PLAN = R4 / "frozen_comparison_plan_v4.json"
PRICE = R5 / "reviewed_model_v2/primary_adjusted_research_candidate.parquet"
ACTION = R5 / "reviewed_model_v2/action_decisions.csv"
ECON = R6 / "finite_economic_review.json"
MEMBER = R3 / "candidate_v3/economic_provisional_membership.parquet"
FAMILY = R4 / "primary_model_v8/identity_families.json"
OWN = R5 / "ownership_replay.json"
SCHEDULE = R5 / "completed_schedule_v3.csv"
VARIANTS = ("Full Rebalance", "Marginal Rebalance", "Prop Rebalance")


def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def load_native():
    assert "exit that stock" in POLICY.read_text(encoding="utf-8")
    spec = importlib.util.spec_from_file_location("round7_native_momo", NATIVE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def stage():
    prices = pd.read_parquet(PRICE)
    review = json.loads(ECON.read_text())
    for path, expected in review["inputs"].items():
        assert sha(OUT.parent / Path(path)) == expected, path
    source = pd.read_csv(ACTION, low_memory=False)
    proofs = []
    for review_row in review["model_overrides"]:
        family, ex = review_row["research_security_id"], review_row["date"]
        actions = source.loc[source.research_security_id.eq(family) & source.date.eq(ex)]
        records = actions.astype(object).where(actions.notna(), None).to_dict("records")
        checked = apply_reviewed_decisions(records, [review_row], ROOT, family)
        action = next(a for a in checked if a["source_row_id"] == review_row["source_row_id"])
        owns = prices.research_security_id.eq(family)
        prior = owns & prices.date.lt(ex)
        previous = prices.loc[prior].sort_values("date").iloc[-1]
        factor = primary_event_factor(previous.CLOSE, action)
        for field in ("Open", "High", "Low", "Close", "adjustment_factor"):
            prices.loc[prior, field] *= factor
        proofs.append(dict(family=family, ex=ex, prior_close=previous.CLOSE, factor=factor,
                           prior_quote=previous.source_row_id, changed_rows=int(prior.sum())))
    assert len(proofs) == 3 and not prices.accepted_for_backtest.any()
    prices = prices.sort_values(["research_security_id", "date"])
    columns = ["date", "research_security_id", "ISIN", "SYMBOL", "Close", "Volume", "adjustment_factor",
               "source_row_id", "accepted_for_backtest"]
    prices[columns].to_parquet(OUT / "provisional_close_revision.parquet", index=False)
    (OUT / "rights_application.json").write_text(json.dumps(proofs, indent=2))
    return prices


def inputs(prices):
    config = next(p["config"] for p in json.loads(PLAN.read_text())["profiles"] if p["name"] == "original_batch_defaults")
    start, end = pd.Timestamp(config["start_date"]), pd.Timestamp(config["end_date"])
    cal = pd.DatetimeIndex(sorted(prices.date.unique()))
    days = cal[(cal >= start) & (cal <= end)]
    membership = pd.read_parquet(MEMBER)
    family = json.loads(FAMILY.read_text())
    membership["SYMBOL"] = membership.SECURITY_ID.str.removeprefix("ISIN:").map(family)
    assert membership.SYMBOL.notna().all()
    assert not membership.duplicated(["TIME_STAMP", "SYMBOL"]).any()
    states = {d: set(g.SYMBOL) for d, g in membership.groupby("TIME_STAMP")}
    needed = set().union(*states.values())
    ownership = json.loads(OWN.read_text())
    assert ownership["price_sha256"] == sha(PRICE)
    actions, marks = [], {}
    for event in ownership["events"]:
        for path, expected in event["evidence"].items():
            assert sha(ROOT / Path(path)) == expected
        old, new = event["old"], event["successor"]
        assert family[old] == old and family[new] == new
        needed.update((old, new))
        old_quote = prices.loc[prices.ISIN.eq(old) & prices.date.eq(event["last_old_close"])].set_index("date").iloc[0]
        successor = prices.loc[prices.ISIN.eq(new)].set_index("date")
        transfer, claim_marks = merger_claim(old, new, event["suspension"], event["first_newly_allotted_tradable"],
                                             event["nominal_share_ratio"], old_quote, successor,
                                             mark_from_listed_class=event["name"] == "CAPF")
        actions.extend(transfer)
        if not claim_marks.empty:
            claim_marks["price_status"] = "verified_mark"
            claim_marks["verification_source"] = "dated same-class listed close"
            claim_marks["verified_at"] = claim_marks.index.strftime("%Y-%m-%d")
            marks[transfer[0]["successor_symbol"]] = session_frame(claim_marks, cal)
    assert not prices.loc[prices.research_security_id.isin(needed)].duplicated(["research_security_id", "date"]).any()
    frames = {}
    for sym, group in prices.loc[prices.research_security_id.isin(needed)].groupby("research_security_id"):
        # High remains uncertified, and neither scoring treatment uses it.
        frame = group.set_index("date")[["Close", "Volume"]].assign(High=np.nan)
        frames[sym] = session_frame(frame, cal)
    frames.update(marks)
    supplied = pd.read_csv(SCHEDULE, parse_dates=["execution", "rank"])
    schedules = {}
    for freq in ("weekly", "monthly"):
        schedule = supplied.loc[supplied.frequency.eq(freq)]
        grid = app.extend_sessions(cal, end + pd.Timedelta(days=190))
        expected = [d for d in app.get_rebalance_dates(grid, freq) if days[0] < d <= days[-1]]
        assert schedule.execution.tolist() == expected
        assert all(cal[cal.get_loc(row.execution)-1] == row.rank for row in schedule.itertuples())
        schedules[freq] = dict(zip(schedule.execution, schedule["rank"]))
    return config, cal, days, states, frames, actions, schedules


def rankings(native, config, cal, states, frames, schedules):
    tradable = app._precompute_tradable_dates(frames)
    pre = app._precompute_all_metrics(frames, cal)
    arrays = {s: {c: g[c].to_numpy() for c in g} for s, g in pre.items()}
    closes = pd.DataFrame({s: f.Close.reindex(cal) for s, f in frames.items()}, index=cal)
    momo_score, _, _ = native.available_signals(closes, closes)
    result, audit = {}, []
    for freq, schedule in schedules.items():
        for execution, signal in schedule.items():
            members = states[max(d for d in states if d <= execution)]
            ranked, excluded = app.rank_universe_at_date(
                frames, signal, config["sort_method"], valid_symbols=members, min_history_days=252,
                apply_volume_filter=True, minimum_median_volume=config["minimum_median_volume"],
                max_stale_sessions=config["max_stale_sessions"], precomputed=pre, metric_arrays=arrays,
                trading_calendar=cal, tradable_dates=tradable, return_excluded_reasons=True,
            )
            momo = [s for s in ranked if np.isfinite(momo_score.at[signal, s])]
            momo.sort(key=lambda s: (-momo_score.at[signal, s], s))
            result[freq, execution] = ranked, momo
            positions = {s: i for i, s in enumerate(ranked, 1)}
            momo_positions = {s: i for i, s in enumerate(momo, 1)}
            for symbol in sorted(members):
                audit.append((freq, execution, signal, symbol, positions.get(symbol), momo_positions.get(symbol),
                              excluded.get(symbol, "" if symbol in positions else "no_frame")))
    return result, pd.DataFrame(audit, columns=["frequency", "execution", "signal", "family", "app_rank",
                                                 "momo_rank", "eligibility_reason"])


def native_book(native, rate):
    """Adapt native MoMo FIFO to the shared solver, fees and fiscal tax convention."""
    native.CFG.update(fees_per_side=rate, slippage_per_side=0.0)

    class MoMoBook(QuantityLedger):
        def sell(self, day, s, qty, price, rate, flat, reason="rebalance"):
            qty = min(qty, self.quantities().get(s, 0.0))
            if qty <= 1e-12 or qty * price * (1 - rate) <= flat:
                return 0.0
            assert flat == 0.0
            remaining = qty
            position = native.Position()
            for i, lot in enumerate(self.lots[s]):
                if remaining <= 1e-12:
                    break
                used = min(remaining, lot["shares"])
                position.lots.append(native.Lot(lot["date"], used, lot["price"], 0.0, i))
                lot["shares"] -= used
                remaining -= used
            self.lots[s] = [lot for lot in self.lots[s] if lot["shares"] > 1e-12]
            if not self.lots[s]:
                del self.lots[s]
            for item in native.sale_lots(position, day, price):
                q = item["units"]
                acquired = item["entry_date"]
                long_term = day > acquired + relativedelta(months=12)
                gain = item["pnl"]
                self.gains[(2 if long_term else 0) + (gain < 0)] += abs(gain)
                self.closed_lots.append(dict(date=day, symbol=s, acquired=acquired, quantity=q,
                                             basis=q * item["entry_fill"], proceeds=q * price-item["sell_fee"],
                                             gain=gain, long_term=long_term, reason=reason))
            gross = qty * price
            fee = gross * rate
            cash_change = gross - fee
            self.cash += cash_change
            self.fees += fee
            self.trades.append(dict(date=day, side="SELL", symbol=s, quantity=qty, price=price, gross=gross,
                                    fee=fee, brokerage=0.0, cash_change=cash_change, reason=reason))
            return cash_change

    return MoMoBook

def quote_grid(frames, days):
    by_day = {day: {} for day in days}
    for symbol, frame in frames.items():
        visible = frame.loc[frame.index.intersection(days)]
        visible = visible.loc[visible.is_tradable]
        for day, close in visible.Close.items():
            by_day[day][symbol] = float(close)
    return by_day


def simulate(book_type, config, frames, actions, days, schedule, ranked, quotes_by_day, variant, taxed):
    book = book_type()
    rate = config["transaction_cost_pct"]
    st = config["stcg_rate"] if taxed else 0.0
    lt = config["ltcg_rate"] if taxed else 0.0
    tax = app._compute_fy_tax
    stamps = dict(zip(days, session_close_times(days)))
    latest = {}
    assumptions = []
    obsolete = set()
    for day in days:
        quotes = quotes_by_day[day]
        for symbol, price in quotes.items():
            latest[symbol] = (day, price)
        for action in actions:
            if pd.Timestamp(action["effective_date"]) == day:
                obsolete.add(action["old_symbol"])
                book.transfer(day, action)
        book.mark(day, frames)
        book.accrue_tax(day, tax, st, lt)
        book.settle_tax(day, {s: p for s, p in quotes.items() if s in book.lots}, rate, 0.0)
        book.accrue_tax(day, tax, st, lt)
        if day in schedule:
            ranks = ranked[day]
            forced = set()
            prior_weights = book.weights()
            for symbol, quantity in sorted(book.quantities().items()):
                if symbol in quotes or symbol in obsolete or symbol.startswith("CLAIM:"):
                    continue
                if symbol not in latest:
                    raise ValueError(f"No prior executable close for held {symbol} on {day}")
                quote_day, price = latest[symbol]
                assert quote_day < day
                book.sell(day, symbol, quantity, price, rate, 0.0, "assumed last-price exit")
                assumptions.append(dict(date=day, symbol=symbol, source_day=quote_day,
                                        available_at=stamps[quote_day].isoformat(),
                                        cutoff=stamps[day].isoformat(),
                                        age_seconds=(stamps[day]-stamps[quote_day]).total_seconds(),
                                        price=price, reason="ordinary held security missing executable close"))
                forced.add(symbol)
            book.accrue_tax(day, tax, st, lt)
            held = set(book.quantities())
            eligible_entries = [s for s in ranks if s in quotes and s not in forced and s not in obsolete]
            top = set(ranks[:config["m"]])
            keep = set(ranks[:config["n"]])
            entries = (top & set(eligible_entries)) - held
            # Preserve the original band; replace failed top-M allocations and
            # missing exits using the next eligible unheld symbol.
            needed = max(len(top - held), len(forced))
            for candidate in eligible_entries:
                if len(entries) >= needed:
                    break
                if candidate not in held:
                    entries.add(candidate)
            exits = held - keep
            wanted = (held - exits) | entries
            target = app._compute_weight_variants(
                wanted, entries, exits | forced, prior_weights, prior_weights, len(wanted)
            )[variant]
            book.rebalance(day, target, quotes, rate, 0.0, tax, st, lt)
        book.snapshot(day)
    return book, assumptions


def compare(left, right):
    maxima = {}

    def walk(a, b, group):
        if isinstance(a, dict):
            assert a.keys() == b.keys(), (group, a.keys(), b.keys())
            for k in a:
                walk(a[k], b[k], group)
        elif isinstance(a, (list, tuple)):
            assert len(a) == len(b), (group, len(a), len(b))
            for x, y in zip(a, b):
                walk(x, y, group)
        elif isinstance(a, (int, float, np.number)) and not isinstance(a, bool):
            delta = abs(a - b)
            maxima[group] = max(maxima.get(group, 0), delta)
            assert np.isclose(a, b, rtol=1e-9, atol=2e-9), (group, a, b)
        else:
            assert a == b, (group, a, b)

    for group in ("daily", "trades", "closed_lots", "tax_events", "lots"):
        walk(getattr(left, group), getattr(right, group), group)
    walk(left.tax_paid, right.tax_paid, "total_tax_paid")
    return maxima


def summarize(book, config):
    nav = pd.Series({row["date"]: row["nav"] for row in book.daily})
    years = (pd.Timestamp(config["end_date"]) - pd.Timestamp(config["start_date"])).days / 365.25
    return dict(cagr_pct=100 * ((nav.iloc[-1] / 100) ** (1 / years) - 1),
                max_drawdown_pct=100 * (nav / nav.cummax() - 1).min(),
                final_rupees=nav.iloc[-1] * config["initial_capital"] / 100,
                fills=sum(t["side"] in ("BUY", "SELL") for t in book.trades),
                tax_paid_rupees=book.tax_paid * config["initial_capital"] / 100)


def export(book, assumptions, prefix):
    for group in ("daily", "trades", "closed_lots", "tax_events"):
        data = getattr(book, group)
        if group == "daily":
            data = [{**row, **{key: json.dumps(row[key], sort_keys=True)
                               for key in ("quantities", "marks", "stale_marks")}} for row in data]
        pd.DataFrame(data).to_csv(OUT / (prefix + "_" + group + ".csv"), index=False)
    pd.DataFrame(assumptions).to_csv(OUT / (prefix + "_assumptions.csv"), index=False)


def main():
    native = load_native()
    print("Stage prices", flush=True)
    prices = stage()
    config, cal, days, states, frames, actions, schedules = inputs(prices)
    del prices
    print("Rank full dated universe", flush=True)
    ranked, eligibility = rankings(native, config, cal, states, frames, schedules)
    eligibility.to_csv(OUT / "eligibility.csv", index=False)
    quotes = quote_grid(frames, days)
    native_type = native_book(native, config["transaction_cost_pct"])
    summaries, checks = [], []
    for frequency, schedule in schedules.items():
        for variant, label in enumerate(VARIANTS):
            for taxed in (False, True):
                common = {d: ranked[frequency, d][0] for d in schedule}
                alternate = {d: ranked[frequency, d][1] for d in schedule}
                prefix = f"{frequency}_{variant}_{int(taxed)}"
                a, assumed = simulate(QuantityLedger, config, frames, actions, days, schedule, common, quotes, variant, taxed)
                b, native_assumed = simulate(native_type, config, frames, actions, days, schedule, common, quotes, variant, taxed)
                checks.append(dict(frequency=frequency, variant=label, taxed=taxed, maxima=compare(a, b)))
                assert assumed == native_assumed
                # The comparison changes only score ordering. Both use the
                # frozen app eligibility mask and common allocation and tax.
                for engine, scoring, book in [("App ledger", "App", a), ("MoMo FIFO adapter", "App", b)]:
                    summaries.append(dict(frequency=frequency, variant=label, taxed=taxed,
                                          engine=engine, scoring=scoring, **summarize(book, config)))
                if variant == 0:
                    m, momo_assumptions = simulate(native_type, config, frames, actions, days, schedule,
                                                   alternate, quotes, variant, taxed)
                    summaries.append(dict(frequency=frequency, variant=label, taxed=taxed,
                                          engine="MoMo FIFO adapter", scoring="MoMo", **summarize(m, config)))
                    export(a, assumed, prefix + "_app")
                    export(m, momo_assumptions, prefix + "_momo")
                print(prefix, "reconciled", len(a.trades), flush=True)
    pd.DataFrame(summaries).to_csv(OUT / "results.csv", index=False)
    (OUT / "reconciliation.json").write_text(json.dumps(checks, indent=2))
    revision = OUT / "provisional_close_revision.parquet"
    bound = [PRICE, ACTION, ECON, MEMBER, FAMILY, OWN, PLAN, SCHEDULE, NATIVE, POLICY, revision,
             ROOT / "backtest_engine.py", ROOT / "backtest_accounting.py", ROOT / "research_execution.py",
             ROOT / "research_ownership.py", ROOT / "momentum_engine.py", ROOT / "equity_data.py",
             ROOT / "data/nse_session_calendar.json", Path(__file__)]
    (OUT / "run_manifest.json").write_text(json.dumps(dict(
        accepted=False, config=config, ownership_actions=actions, warmup_start=str(cal[0].date()),
        observed_sessions=len(days), schedules={k:len(v) for k,v in schedules.items()},
        scope="Full dated NIFTY 500 candidate, adjusted Close and nominal Volume",
        nav_normalization="100 units equal INR 1,000,000; fees 0.1% per side, zero flat and extra slippage",
        tax="app fiscal-year FIFO rates, offsets and liability; pretax rates set to zero",
        missing_policy="ordinary missing sells assumed at latest eligible completed close; locked claims never executable",
        economic_convention="unresolved events left unadjusted; no invented entitlement mark or cash; results conditional",
        files={str(p.relative_to(ROOT)):sha(p) for p in bound},
    ), indent=2))


if __name__ == "__main__":
    main()
