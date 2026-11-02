"""External quantity/cash oracles for repaired execution and accounting."""

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import backtest_engine as e
from backtest_accounting import QuantityLedger
from security_identity import load_aliases, resolve_name, resolve_source_events


def data(close, days):
    c = pd.Series(close, index=days, dtype=float)
    return pd.DataFrame(dict(Open=c, High=c, Low=c, Close=c, Volume=1e6), index=days)


def run(monkeypatch, prices, events, rank, **kwargs):
    days = pd.bdate_range("2024-07-01", periods=30)
    frames = {s: data(c, days) for s, c in prices.items()}
    monkeypatch.setattr(e, "get_rebalance_dates", lambda *_: [days[i] for i in events])
    monkeypatch.setattr(e, "rank_universe_at_date", lambda _, day, *a, **k: (rank(days.get_loc(day)), {}))
    cfg = e.BacktestConfig(
        m=kwargs.pop("m", 1),
        n=3,
        rebalance_freq="weekly",
        sort_method="3 months",
        start_date=str(days[0].date()),
        end_date=str(days[-1].date()),
        min_history_days=1,
        apply_volume_filter=False,
        market_calendar=days,
        transaction_cost_pct=kwargs.pop("cost", 0),
        corporate_actions=[],
        **kwargs
    )
    result = e.run_backtest(frames, {}, cfg)
    assert "error" not in result, result.get("error")
    return result, frames


def test_missing_buy_cannot_earn_resumption(monkeypatch):
    r, _ = run(monkeypatch, {"A": [100] * 3 + [np.nan] + [200] * 26}, [3], lambda _: ["A"])
    assert (r["nav"].iloc[-1] == 100).all()
    assert r["holdings_log"][0]["entries"] == []
    assert r["fill_failures"]["Full Rebalance"][0]["side"] == "BUY"
    assert r["execution_ledgers"]["Full Rebalance"] == []


def test_missing_sell_retains_quantity_and_uncertain_mark(monkeypatch):
    r, _ = run(monkeypatch, {"A": [100] * 10 + [np.nan] + [200] * 19}, [3, 10], lambda i: ["A"] if i < 9 else [])
    assert r["holdings_log"][-1]["holdings"] == ["A"]
    assert r["holdings_log"][-1]["exits"] == []
    assert r["nav"].iloc[-1, 0] == pytest.approx(200)
    assert r["daily_ledgers"]["Full Rebalance"][10]["stale_marks"] == {"A": "2024-07-12"}


@pytest.mark.parametrize("gap", [1, 8])
def test_gap_resumption_is_quantity_times_mark(monkeypatch, gap):
    a = [100] * 4 + [np.nan] * gap + [200, 400] + [400] * (24 - gap)
    b = [100] * 4 + [200] * 26
    r, _ = run(monkeypatch, {"A": a, "B": b}, [3], lambda _: ["A", "B"], m=2)
    assert r["nav"].iloc[-1, 0] == pytest.approx(300)
    final = r["daily_ledgers"]["Full Rebalance"][-1]
    assert final["quantities"] == {"A": 0.5, "B": 0.5}
    assert final["cash"] == pytest.approx(0, abs=1e-9)


def test_all_cash_costs_fifo_closure_reentry(monkeypatch):
    r, _ = run(monkeypatch, {"A": [100] * 4 + [200] * 26}, [3, 10], lambda i: ["A"] if i < 9 else [], cost=0.001)
    for label in r["execution_ledgers"]:
        assert r["nav"].iloc[-1][label] == pytest.approx(199.6002)
        assert r["daily_ledgers"][label][-1]["quantities"] == {}
        assert sum(x["quantity"] for x in r["closed_lots"][label]) == pytest.approx(0.999)
    lots = {"A": [dict(date=pd.Timestamp("2024-01-01"), price=100.0, shares=1.0)]}
    days = pd.bdate_range("2024-07-01", periods=30)
    gains = e._realise_weight_reductions(lots, {"A": 1.0}, {}, {"A": data([200] * 30, days)}, days[10], 199.8)
    assert lots == {}
    assert gains[0] == 100
    r, _ = run(monkeypatch, {"A": [100] * 30}, [3, 10, 17], lambda i: ["A"] if i < 9 or i > 15 else [])
    assert len(r["execution_ledgers"]["Full Rebalance"]) == 3
    assert r["daily_ledgers"]["Full Rebalance"][-1]["quantities"] == {"A": 1.0}


def test_infeasible_cap_retains_cash_and_does_not_earn_equity_returns(monkeypatch):
    assert e._apply_weight_cap({"A": 0.5, "B": 0.5}, 0.2) == {"A": 0.2, "B": 0.2}
    r, _ = run(
        monkeypatch,
        {"A": [100] * 4 + [200] * 26, "B": [100] * 4 + [200] * 26},
        [3],
        lambda _: ["A", "B"],
        m=2,
        max_position_pct=20,
    )
    assert r["nav"].iloc[-1, 0] == pytest.approx(140)
    assert r["daily_ledgers"]["Full Rebalance"][-1]["cash"] == pytest.approx(60)


def test_independent_trade_cash_fifo_daily_reconciliation(monkeypatch):
    r, frames = run(
        monkeypatch,
        {"A": [100] * 4 + [200] * 26, "B": [100] * 30},
        [3, 10, 17],
        lambda i: ["A", "B"] if i < 9 else ["B"],
        m=2,
        cost=0.001,
        stcg_rate=0.2,
    )
    for label, trades in r["execution_ledgers"].items():
        cash = 100.0
        qty = {}
        fifo = {}
        gain = 0.0
        for day in r["daily_ledgers"][label]:
            for t in [x for x in trades if x["date"] == day["date"]]:
                assert t["price"] == frames[t["symbol"]].loc[t["date"], "Close"]
                s = t["symbol"]
                q = t["quantity"]
                cash += t["cash_change"]
                if t["side"] == "BUY":
                    qty[s] = qty.get(s, 0) + q
                    fifo.setdefault(s, []).append([q, -t["cash_change"] / q])
                else:
                    qty[s] -= q
                    remaining = q
                    while remaining > 1e-10:
                        lot = fifo[s][0]
                        used = min(lot[0], remaining)
                        gain += used * (t["price"] - lot[1]) - (t["fee"] + t["brokerage"]) * used / q
                        remaining -= used
                        lot[0] -= used
                        if lot[0] < 1e-10:
                            fifo[s].pop(0)
            for payment in [x for x in r["tax_events"][label] if x["date"] == day["date"]]:
                cash -= payment["paid"]
            assert cash == pytest.approx(day["cash"])
            assert {s: q for s, q in qty.items() if q > 1e-10} == pytest.approx(day["quantities"])
            stock = sum(q * day["marks"][s] for s, q in qty.items() if q > 1e-10)
            assert cash + stock - day["tax_payable"] - day["tax_accrued"] == pytest.approx(day["nav"])
        assert gain == pytest.approx(sum(t["gain"] for t in r["closed_lots"][label]))


def test_external_export_auditor_and_full_exit_tax_oracle(monkeypatch):
    from equity_data import session_frame
    from scripts.reconcile_backtest_ledger import reconcile

    r, frames = run(
        monkeypatch, {"A": [100] * 4 + [200] * 26}, [3, 10], lambda i: ["A"] if i < 9 else [], cost=0.001, stcg_rate=0.2
    )
    # Entry cost basis 100; exit net proceeds 199.6002; taxable gain 99.6002.
    for label in r["execution_ledgers"]:
        assert r["nav"].iloc[-1][label] == pytest.approx(199.6002 - 0.2 * 99.6002)
    cfg = e.BacktestConfig(
        m=1,
        n=3,
        rebalance_freq="weekly",
        sort_method="3 months",
        start_date="2024-07-01",
        end_date="2024-08-09",
        transaction_cost_pct=0.001,
        stcg_rate=0.2,
    )
    accepted = {s: session_frame(f, r["trading_days"]) for s, f in frames.items()}
    reports = reconcile(r, accepted, cfg)
    assert all(x["passed"] for x in reports.values())


def test_verified_no_trade_mark_cannot_fill_and_trim_costs():
    days = pd.bdate_range("2024-07-01", periods=3)
    f = data([100, 200, 300], days)
    f.loc[days[1], "Volume"] = 0
    f.loc[days[1], "price_status"] = "verified_mark"
    f.loc[days[1], "verification_source"] = "https://exchange.example/mark"
    f.loc[days[1], "verified_at"] = "2024-07-02"
    from equity_data import session_frame

    accepted = session_frame(f, days)
    assert accepted.loc[days[1], "Close"] == 200
    assert e._close_price({"A": accepted}, "A", days[1]) is None
    ledger = QuantityLedger(
        cash=0, lots={"A": [dict(date=days[0], price=100.0, shares=1.0)]}, marks={"A": 200.0}, mark_dates={"A": days[1]}
    )
    ledger.rebalance(days[1], {"A": 0.5}, {"A": 200.0}, 0.001, 0, e._compute_fy_tax, 0, 0)
    sale = ledger.trades[0]
    assert sale["side"] == "SELL" and sale["fee"] == pytest.approx(sale["gross"] * 0.001)
    assert ledger.quantities()["A"] == pytest.approx(1 - sale["quantity"])
    assert ledger.cash == pytest.approx(sale["gross"] - sale["fee"])


@pytest.mark.parametrize("sold,long_term", [("2025-02-28", False), ("2025-03-01", True)])
def test_simplified_holding_period_boundary(sold, long_term):
    ledger = QuantityLedger(
        cash=0,
        lots={"A": [dict(date=pd.Timestamp("2024-02-29"), price=100.0, shares=1.0)]},
        marks={"A": 200.0},
        mark_dates={"A": pd.Timestamp(sold)},
    )
    ledger.sell(pd.Timestamp(sold), "A", 1.0, 200.0, 0, 0)
    assert ledger.closed_lots[0]["long_term"] == long_term


def test_effective_corporate_action_preserves_quantity_cost_and_acquisition():
    acquired = pd.Timestamp("2020-01-01")
    ledger = QuantityLedger(
        cash=0,
        lots={"OLD": [dict(date=acquired, price=100.0, shares=2.0)]},
        marks={"OLD": 200.0},
        mark_dates={"OLD": pd.Timestamp("2024-01-01")},
    )
    ledger.transfer(pd.Timestamp("2024-01-02"), dict(old_symbol="OLD", successor_symbol="NEW", share_ratio=0.5))
    assert ledger.quantities() == {"NEW": 1.0}
    assert ledger.nav() == 400.0
    assert ledger.lots["NEW"][0]["price"] == 200.0
    assert ledger.lots["NEW"][0]["date"] == acquired


def test_tax_funding_is_real_sale_and_tax_liability_survives_missing_fill():
    ledger = QuantityLedger(
        cash=0,
        lots={"A": [dict(date=pd.Timestamp("2024-01-01"), price=100.0, shares=1.0)]},
        marks={"A": 200.0},
        mark_dates={"A": pd.Timestamp("2025-04-01")},
        payable=20.0,
    )
    ledger.settle_tax(pd.Timestamp("2025-04-01"), {"A": None}, 0, 0)
    assert ledger.payable == 20 and ledger.quantities() == {"A": 1.0}
    ledger.settle_tax(pd.Timestamp("2025-04-02"), {"A": 200.0}, 0.001, 0)
    assert ledger.cash == pytest.approx(0)
    assert ledger.payable == pytest.approx(0)
    assert ledger.quantities()["A"] == pytest.approx(1 - 20 / (200 * 0.999))
    assert ledger.closed_lots[0]["reason"] == "tax funding"
    assert sum(ledger.gains) > 0


def test_schedule_prefix_and_cagr():
    full = pd.bdate_range("2024-07-01", "2024-09-30")
    # run_backtest schedules against the full exchange grid, then crops.
    for freq in ["weekly", "biweekly", "monthly", "quarterly", "half-yearly"]:
        for cutoff in [pd.Timestamp("2024-07-16"), pd.Timestamp("2024-07-31")]:
            from equity_data import extend_sessions

            a = e.get_rebalance_dates(extend_sessions(full[full <= cutoff], cutoff + pd.Timedelta(days=190)), freq)
            b = e.get_rebalance_dates(extend_sessions(full, full[-1] + pd.Timedelta(days=190)), freq)
            assert [d for d in a if d <= cutoff] == [d for d in b if d <= cutoff]
    stats = e._compute_summary_stats(
        pd.DataFrame({"x": [100.0, 200.0]}, index=pd.to_datetime(["2020-01-01", "2021-01-01"]))
    )
    assert stats.loc["x", "CAGR (%)"] == pytest.approx(round((2 ** (365.25 / 366) - 1) * 100, 2))


def test_end_date_extension_preserves_actual_nav_and_prior_events(monkeypatch):
    from dataclasses import replace

    days = pd.bdate_range("2024-07-01", periods=40)
    frames = {"A": data(np.linspace(100, 200, 40), days)}
    monkeypatch.setattr(e, "rank_universe_at_date", lambda *_a, **_k: (["A"], {}))
    cfg = e.BacktestConfig(
        m=1,
        n=2,
        rebalance_freq="weekly",
        sort_method="3 months",
        start_date="2024-07-01",
        end_date=str(days[28].date()),
        transaction_cost_pct=0.001,
        stcg_rate=0.2,
        min_history_days=1,
        apply_volume_filter=False,
        corporate_actions=[],
    )
    first = e.run_backtest({"A": frames["A"].loc[: days[28]]}, {}, cfg)
    longer = e.run_backtest(frames, {}, replace(cfg, end_date=str(days[-1].date())))
    pd.testing.assert_frame_equal(first["nav"], longer["nav"].loc[: days[28]])
    assert [x["date"] for x in first["holdings_log"]] == [
        x["date"] for x in longer["holdings_log"] if x["date"] <= days[28]
    ]


def test_evidence_dates_do_not_backproject_identity_and_all_heuristics_quarantined():
    aliases = load_aliases()
    assert resolve_name("Balkrishna Industries Ltd.", "1998-01-01", aliases) is None
    assert resolve_name("Balkrishna Industries Ltd.", "2026-05-08", aliases)["exchange_symbol"] == "BALKRISIND"
    events = pd.DataFrame(
        dict(
            INDEX_NAME=["NIFTY 500"] * 2,
            EVENT_DATE=["1998-01-01"] * 2,
            SCRIP_NAME=["Balkrishna Industries Ltd.", "Abbott India Ltd."],
            EVENT_TYPE=["INCL", "EXCL"],
        )
    )
    proposals = pd.DataFrame(dict(SCRIP_NAME=events.SCRIP_NAME, SYMBOL=["RITES", "ABB"], CONFIDENCE=["high"] * 2))
    resolved = resolve_source_events(events, proposals)
    assert len(resolved) == 2 and resolved.SYMBOL.isna().all()
    assert not resolved.RECONSTRUCTION_ALLOWED.any()
    assert resolved.INCEPTION_REVIEW_REQUIRED.tolist() == [False, True]
    assert not resolved.LEGACY_EXCLUDED.any()


def test_alias_overlap_rejected(tmp_path):
    import json

    a = load_aliases()[0]
    b = {**a, "security_id": "ISIN:OTHER"}
    p = tmp_path / "aliases.json"
    p.write_text(json.dumps({"aliases": [a, b]}))
    with pytest.raises(ValueError, match="overlapping"):
        load_aliases(p)


def reconstruction_module():
    spec = importlib.util.spec_from_file_location(
        "reconstruct_indices", Path(__file__).parents[1] / "data/reconstruct_indices.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reconstruction_event_direction_and_no_unresolved_publish():
    m = reconstruction_module()
    f = pd.DataFrame(dict(INDEX_NAME=["X", "X"], TIME_STAMP=["2020-01-10"] * 2, SYMBOL=["B", "C"]))
    events = pd.DataFrame(
        dict(
            INDEX_NAME=["X", "X"],
            EVENT_DATE=["2020-01-03", "2020-01-07"],
            SYMBOL=["B", "A"],
            EVENT_TYPE=["INCL", "EXCL"],
        )
    )
    comp, _, _ = m.reconstruct(f, f.iloc[:0], events)
    assert set(comp.loc[comp.TIME_STAMP.eq("2020-01-03"), "SYMBOL"]) == {"A", "B", "C"}
    assert set(comp.loc[comp.TIME_STAMP.eq("2020-01-07"), "SYMBOL"]) == {"B", "C"}
    with pytest.raises(ValueError, match="validation"):
        m.refresh_constituents(comp)
    events.loc[0, "SYMBOL"] = None
    with pytest.raises(ValueError, match="Unresolved"):
        m.reconstruct(f, f.iloc[:0], events)
