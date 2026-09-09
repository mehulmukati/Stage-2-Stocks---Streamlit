import pandas as pd
import pytest

import quant_portfolio_engine as engine
from quant_portfolio_engine import QuantPortfolioConfig, run_quant_portfolio
from quant_strategy import SIGNAL_COLUMNS, StrategyDefinition


class _FakeStrategy:
    definition = StrategyDefinition("fake", "Fake", "Daily", "test")
    config = object()

    def compute(self, prices):
        symbol_score = float(prices.attrs["score"])
        dates = prices.index
        return pd.DataFrame(
            [
                [dates[1], dates[2], True, True, False, True, symbol_score, "entry", "exit", {"gate": True}],
                [dates[3], dates[4], True, False, True, False, symbol_score, "entry", "exit", {"gate": True}],
            ],
            columns=SIGNAL_COLUMNS,
        )


def _prices(score):
    dates = pd.bdate_range("2024-01-01", periods=6)
    frame = pd.DataFrame(
        {
            "Open": [10, 10, 10, 12, 12, 12],
            "High": 13,
            "Low": 9,
            "Close": [10, 10, 11, 12, 12, 12],
            "Volume": 1_000_000,
        },
        index=dates,
    )
    frame.attrs["score"] = score
    return frame


def test_fixed_slots_rank_entries_and_keep_complete_debug_log(monkeypatch):
    monkeypatch.setattr(engine, "build_strategy", lambda *_args, **_kwargs: _FakeStrategy())
    monkeypatch.setattr(
        engine, "strategy_metadata", lambda strategy: {"definition": strategy.definition.__dict__, "config": {}}
    )
    prices = {"LOW": _prices(1), "HIGH": _prices(2)}
    result = run_quant_portfolio(
        prices,
        {},
        QuantPortfolioConfig(
            strategy="fake",
            max_holdings=1,
            initial_capital=1_000,
            transaction_cost_pct=0,
            min_history_days=1,
            minimum_median_volume=0,
        ),
    )
    buys = result["decision_log"].query("Event == 'BUY'").set_index("Symbol")
    assert buys.at["HIGH", "Outcome"] == "EXECUTED"
    assert buys.at["LOW", "Outcome"] == "SKIPPED_FULL"
    assert result["trades"].query("Status == 'CLOSED'").iloc[0]["Symbol"] == "HIGH"
    assert result["metrics"]["Skipped · Portfolio Full"] == 1


def test_empty_signal_run_still_returns_nav_and_metrics(monkeypatch):
    fake = _FakeStrategy()
    fake.compute = lambda prices: pd.DataFrame(columns=SIGNAL_COLUMNS)
    monkeypatch.setattr(engine, "build_strategy", lambda *_args, **_kwargs: fake)
    monkeypatch.setattr(
        engine, "strategy_metadata", lambda strategy: {"definition": strategy.definition.__dict__, "config": {}}
    )
    result = run_quant_portfolio(
        {"AAA": _prices(1)},
        {},
        QuantPortfolioConfig(strategy="fake", min_history_days=1, minimum_median_volume=0),
    )
    assert result["trades"].empty
    assert result["metrics"]["Signals"] == 0
    assert result["nav"]["Quant Portfolio"].eq(100).all()


def test_non_liquidity_strategy_does_not_require_or_gate_on_volume(monkeypatch):
    monkeypatch.setattr(engine, "build_strategy", lambda *_args, **_kwargs: _FakeStrategy())
    monkeypatch.setattr(
        engine, "strategy_metadata", lambda strategy: {"definition": strategy.definition.__dict__, "config": {}}
    )
    prices = _prices(1).drop(columns="Volume")

    result = run_quant_portfolio(
        {"NO_VOLUME": prices},
        {},
        QuantPortfolioConfig(
            strategy="fake",
            initial_capital=1_000,
            transaction_cost_pct=0,
            min_history_days=1,
            minimum_median_volume=999_999_999,
        ),
    )

    buy = result["decision_log"].query("Event == 'BUY'").iloc[0]
    assert buy["Outcome"] == "EXECUTED"


def test_exact_score_tie_is_resolved_by_symbol(monkeypatch):
    engine._signal_cache.clear()
    monkeypatch.setattr(engine, "build_strategy", lambda *_args, **_kwargs: _FakeStrategy())
    monkeypatch.setattr(
        engine, "strategy_metadata", lambda strategy: {"definition": strategy.definition.__dict__, "config": {}}
    )

    result = run_quant_portfolio(
        {"BBB": _prices(1), "AAA": _prices(1)},
        {},
        QuantPortfolioConfig(
            strategy="fake",
            max_holdings=1,
            initial_capital=1_000,
            transaction_cost_pct=0,
            min_history_days=1,
        ),
    )

    buys = result["decision_log"].query("Event == 'BUY'").set_index("Symbol")
    assert buys.at["AAA", "Outcome"] == "EXECUTED"
    assert buys.at["BBB", "Outcome"] == "SKIPPED_FULL"


def test_relative_strength_ranking_is_point_in_time_and_ha_only():
    dates = pd.bdate_range("2025-01-01", periods=8)
    columns = list(SIGNAL_COLUMNS)
    signal_row = [dates[5], dates[6], True, True, False, True, 0.0, "entry", "exit", {}]
    signals = {
        "A": pd.DataFrame([signal_row], columns=columns),
        "B": pd.DataFrame([signal_row], columns=columns),
    }
    prices = {
        "A": pd.DataFrame({"Close": [100, 100, 100, 100, 100, 120, 999, 999]}, index=dates),
        "B": pd.DataFrame({"Close": [100, 100, 100, 100, 100, 110, 50, 50]}, index=dates),
    }
    benchmark = pd.Series([100, 100, 100, 100, 100, 105, 200, 200], index=dates)
    strategy = _FakeStrategy()
    strategy.definition = StrategyDefinition("ha_ema", "HA + EMA", "Weekly", "test")
    config = QuantPortfolioConfig(
        strategy="ha_ema",
        ranking_method=engine.RANK_RELATIVE_STRENGTH,
        ranking_lookback_sessions=3,
    )

    ranked, error = engine._apply_entry_ranking(signals, prices, {"Nifty 100": benchmark}, strategy, config)

    assert error is None
    assert ranked["A"].iloc[0]["Score"] == pytest.approx(1.2 / 1.05)
    assert ranked["B"].iloc[0]["Score"] == pytest.approx(1.1 / 1.05)
    assert ranked["A"].iloc[0]["Diagnostics"]["Stock_Return_Pct"] == pytest.approx(20.0)

    strategy.definition = StrategyDefinition("ichimoku", "Ichimoku", "Weekly", "test")
    _, error = engine._apply_entry_ranking(signals, prices, {"Nifty 100": benchmark}, strategy, config)
    assert "only for HA + EMA" in error
