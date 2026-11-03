"""Boundary oracles from the independent round-one reassessment."""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import backtest_engine as e
from backtest_accounting import QuantityLedger
from tests.test_backtest_accuracy import run


def seeded(symbol, *, cash=0.0, price=100.0, shares=1.0, basis=100.0):
    day = pd.Timestamp("2024-07-01")
    return QuantityLedger(
        cash=cash,
        lots={symbol: [dict(date=day, price=basis, shares=shares)]},
        marks={symbol: price},
        mark_dates={symbol: day},
    )


def test_failed_buy_actual_holdings_average_is_zero(monkeypatch):
    result, _ = run(monkeypatch, {"A": [100] * 3 + [np.nan] + [200] * 26}, [3], lambda _: ["A"])
    assert (result["stats"]["Avg Holdings"] == 0).all()


@pytest.mark.parametrize("blocked", ["A", "both"])
def test_blocked_displacement_exits_occupy_real_slots_across_signals(monkeypatch, blocked):
    a = [100.0] * 30
    b = [100.0] * 30
    for i in [10, 17]:
        a[i] = np.nan
        if blocked == "both":
            b[i] = np.nan
    result, _ = run(
        monkeypatch,
        dict(A=a, B=b, C=[100] * 30, D=[100] * 30),
        [3, 10, 17],
        lambda i: ["A", "B"] if i < 9 else ["C", "D"],
        m=2,
        band_rule="displacement",
        cost=0.001,
    )
    expected = {"A", "C"} if blocked == "A" else {"A", "B"}
    for event in result["holdings_log"][1:]:
        for actual in event["holdings_by_variant"].values():
            assert set(actual) == expected


def test_divergent_incumbents_are_book_specific(monkeypatch):
    states = [seeded("A"), seeded("B"), seeded("A")]
    with patch.object(e, "QuantityLedger", side_effect=states):
        result, _ = run(
            monkeypatch, dict(A=[100] * 30, B=[100] * 30), [3], lambda _: ["A", "B"], m=1, band_rule="displacement"
        )
    assert result["holdings_log"][0]["holdings_by_variant"] == {
        "Full Rebalance": ["A"],
        "Marginal Rebalance": ["B"],
        "Prop Rebalance": ["A"],
    }


def test_reachable_divergence_from_different_sale_affordability(monkeypatch):
    # Full redistributes A's rise at the middle signal; marginal/prop retain
    # the original small B lot. B's proceeds cannot cover a 60-unit sale fee
    # in those books, so it cannot free their replacement slot.
    result, _ = run(
        monkeypatch,
        dict(A=[100] * 4 + [10000] * 26, B=[100] * 30, C=[100] * 30),
        [3, 10, 17],
        lambda i: ["A", "B"] if i < 16 else ["A", "C"],
        m=2,
        band_rule="displacement",
        initial_capital=100,
        brokerage_per_sale=60,
    )
    actual = result["holdings_log"][-1]["holdings_by_variant"]
    assert actual["Full Rebalance"] == ["A", "C"]
    assert actual["Marginal Rebalance"] == ["A", "B"]
    assert actual["Prop Rebalance"] == ["A", "B"]
    for label, book in result["daily_ledgers"].items():
        assert result["stats"].loc[label, "Avg Holdings"] == round(np.mean([len(d["quantities"]) for d in book]), 1)


def test_unfilled_top_candidate_does_not_consume_replacement_slot(monkeypatch):
    a = [100.0] * 30
    c = [100.0] * 30
    a[10] = np.nan
    c[10] = np.nan
    result, _ = run(
        monkeypatch,
        dict(A=a, B=[100] * 30, C=c, D=[100] * 30),
        [3, 10],
        lambda i: ["A", "B"] if i < 9 else ["C", "D"],
        m=2,
        band_rule="displacement",
        cost=0.001,
    )
    assert all(v == ["A", "D"] for v in result["holdings_log"][-1]["holdings_by_variant"].values())


def test_taxable_cap_has_independently_solved_final_quantities():
    ledger = seeded("A", price=200.0)
    day = pd.Timestamp("2024-07-15")
    ledger.rebalance(day, {"A": 0.5, "B": 0.5}, {"A": 200.0, "B": 100.0}, 0, 0, e._compute_fy_tax, 0.2, 0.125)
    # Sell x of A: tax=20x, net NAV=200-20x, retained value=200(1-x).
    # 200(1-x)=(200-20x)/2 -> x=10/19; net NAV=3600/19.
    assert ledger.quantities()["A"] == pytest.approx(9 / 19)
    assert ledger.quantities()["B"] == pytest.approx(18 / 19)
    assert ledger.accrued == pytest.approx(200 / 19)
    assert ledger.cash == pytest.approx(200 / 19)
    assert ledger.nav() == pytest.approx(3600 / 19)
    assert max(ledger.weights().values()) <= 0.5 + 1e-9


def test_public_engine_taxable_final_cap(monkeypatch):
    result, _ = run(
        monkeypatch,
        dict(A=[100] * 4 + [200] * 26, B=[100] * 30),
        [3, 10],
        lambda i: ["A"] if i < 9 else ["A", "B"],
        m=2,
        max_position_pct=50,
        stcg_rate=0.2,
        ltcg_rate=0.125,
        cost=0.001,
    )
    for weights in ["full_weights", "marg_weights", "prop_weights"]:
        assert max(result["holdings_log"][-1][weights].values()) <= 50 + 1e-6


@pytest.mark.parametrize("rate", [0.0, 0.001, 0.04])
def test_cap_with_fees_all_cash_and_partial_cash(rate):
    for cash in [0.0, 35.0]:
        ledger = seeded("A", cash=cash, price=200.0)
        ledger.rebalance(
            pd.Timestamp("2024-07-15"),
            {"A": 0.3, "B": 0.3},
            {"A": 200.0, "B": 100.0},
            rate,
            0,
            e._compute_fy_tax,
            0.2,
            0.125,
        )
        assert max(ledger.weights().values()) <= 0.3 + 1e-8
        assert ledger.cash >= ledger.accrued - 1e-8
        assert ledger.nav() == pytest.approx(
            ledger.cash + sum(q * ledger.marks[s] for s, q in ledger.quantities().items()) - ledger.accrued
        )


def test_live_replay_view_uses_selected_book_without_mutating_engine_event():
    import copy

    from app_live_signal import _variant_event_view

    event = dict(
        holdings=["A", "B"],
        entries=[],
        exits=[],
        holdings_by_variant={"Full Rebalance": ["A"], "Marginal Rebalance": ["B"], "Prop Rebalance": ["A"]},
        pre_rebalance_full_weights={"B": 100.0},
        pre_rebalance_marg_weights={"B": 100.0},
        pre_rebalance_prop_weights={"A": 100.0},
    )
    original = copy.deepcopy(event)
    full = _variant_event_view(event, "Full Rebalance")
    marginal = _variant_event_view(event, "Marginal Rebalance")
    assert full["holdings"] == ["A"] and full["entries"][0].startswith("A (") and full["exits"][0].startswith("B (")
    assert marginal["holdings"] == ["B"] and marginal["entries"] == marginal["exits"] == []
    assert event == original


def test_live_actual_position_checkpoint_event_retains_legacy_interface():
    from app_live_signal import _variant_event_view

    event = dict(holdings=["A"], entries=["A (rank #1)"], exits=[], full_weights={"A": 100.0})
    assert _variant_event_view(event, "Full Rebalance") == event
