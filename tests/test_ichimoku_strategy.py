from dataclasses import replace

import pandas as pd

from ichimoku_engine import compute_ichimoku
from ichimoku_strategy import (
    CHIKOU_HIGH,
    CHIKOU_NONE,
    ENTRY_COMPLETE_STRUCTURE,
    ENTRY_KUMO_BREAKOUT,
    ENTRY_TK_CROSS,
    EXIT_KUMO_ENTRY,
    IchimokuStrategyConfig,
    build_strategy_comparison,
    compute_ichimoku_signals,
    matching_preset,
    strategy_preset,
)
from strategy_replay import ReplayConfig


def _strategy_frame(n: int = 14) -> pd.DataFrame:
    index = pd.bdate_range("2025-01-01", periods=n)
    close = pd.Series([100.0 + i for i in range(n)], index=index)
    frame = pd.DataFrame(
        {
            "Open": close - 0.5,
            "High": close + 1.0,
            "Low": close - 2.0,
            "Close": close,
            "Volume": 1_000_000.0,
            "Tenkan": close - 0.5,
            "Kijun": close - 1.0,
            "Senkou_A": close - 10.0,
            "Senkou_B": close - 12.0,
            "TK_Cross": pd.NA,
            "Cross_Strength": pd.NA,
            "IsFuture": False,
        },
        index=index,
    )
    return frame


def _small_config(**changes) -> IchimokuStrategyConfig:
    base = IchimokuStrategyConfig(
        name="Custom",
        entry_event=ENTRY_COMPLETE_STRUCTURE,
        price_location="Above cloud",
        require_tk_alignment=True,
        require_forward_bullish=True,
        chikou_mode=CHIKOU_HIGH,
        displacement=2,
        span_b_period=3,
        exit_event=EXIT_KUMO_ENTRY,
    )
    return replace(base, **changes)


def test_presets_are_stable_and_custom_matching_is_exact():
    for name in ("Aggressive", "Balanced", "Conservative"):
        assert matching_preset(strategy_preset(name)) == name

    custom = replace(strategy_preset("Balanced"), exit_confirmation=2)
    assert matching_preset(custom) is None


def test_balanced_structure_waits_for_all_point_in_time_gates():
    frame = _strategy_frame()
    result = compute_ichimoku_signals(frame, _small_config())

    buy_dates = result.index[result["Signal"].eq("BUY")]
    assert len(buy_dates) == 1
    buy = buy_dates[0]
    assert result.at[buy, "Strategy_Ready"]
    assert result.at[buy, "Chikou_Above_High"]
    assert result.at[buy, "Forward_Senkou_A"] > result.at[buy, "Forward_Senkou_B"]


def test_aggressive_cross_rejects_below_cloud_and_accepts_inside_cloud():
    frame = _strategy_frame()
    first, second = frame.index[5], frame.index[8]
    frame.loc[first, ["Close", "Open", "High", "Low"]] = [80.0, 79.0, 81.0, 78.0]
    frame.loc[first, ["TK_Cross", "Cross_Strength"]] = ["bullish", "weak bullish"]
    frame.loc[second, ["Close", "Open", "High", "Low"]] = [90.0, 89.0, 91.0, 88.0]
    frame.loc[second, ["Senkou_A", "Senkou_B"]] = [92.0, 88.0]
    frame.loc[second, ["TK_Cross", "Cross_Strength"]] = ["bullish", "neutral bullish"]

    config = replace(strategy_preset("Aggressive"), displacement=2, span_b_period=3)
    result = compute_ichimoku_signals(frame, config)

    assert result.at[first, "Signal"] == ""
    assert result.at[second, "Signal"] == "BUY"


def test_conservative_breakout_requires_two_qualifying_closes():
    frame = _strategy_frame()
    frame["Close"] = 89.0
    frame["Open"] = 88.0
    frame["High"] = 90.0
    frame["Low"] = 87.0
    frame["Tenkan"] = 88.0
    frame["Kijun"] = 87.0
    frame["Senkou_A"] = 92.0
    frame["Senkou_B"] = 90.0
    breakout = frame.index[7]
    confirmation = frame.index[8]
    frame.loc[breakout:, "Close"] = 95.0
    frame.loc[breakout:, "High"] = 96.0
    frame.loc[breakout:, "Open"] = 94.0
    frame.loc[breakout:, "Tenkan"] = 94.0
    frame.loc[breakout:, "Kijun"] = 90.0

    config = _small_config(
        entry_event=ENTRY_KUMO_BREAKOUT,
        chikou_mode=CHIKOU_NONE,
        kijun_direction="Non-falling",
        entry_confirmation=2,
    )
    result = compute_ichimoku_signals(frame, config)

    assert result.at[breakout, "Signal"] == ""
    assert result.at[confirmation, "Signal"] == "BUY"


def test_breakout_does_not_arm_when_a_selected_qualifier_fails():
    frame = _strategy_frame()
    frame.loc[frame.index[:6], "Close"] = frame.loc[frame.index[:6], "Senkou_A"] - 1.0
    breakout = frame.index[6]
    frame.loc[breakout:, "Tenkan"] = frame.loc[breakout:, "Kijun"] - 1.0
    config = _small_config(
        entry_event=ENTRY_KUMO_BREAKOUT,
        chikou_mode=CHIKOU_NONE,
        require_forward_bullish=False,
    )

    result = compute_ichimoku_signals(frame, config)

    assert not bool(result.at[breakout, "Entry_Event"])
    assert not result["Signal"].eq("BUY").any()


def test_cloud_entry_exit_is_position_aware_and_executes_after_buy():
    frame = _strategy_frame()
    result = compute_ichimoku_signals(frame, _small_config(chikou_mode=CHIKOU_NONE))
    buy = result.index[result["Signal"].eq("BUY")][0]
    exit_date = result.index[result.index.get_loc(buy) + 3]
    frame.loc[exit_date, "Close"] = frame.loc[exit_date, ["Senkou_A", "Senkou_B"]].max()

    result = compute_ichimoku_signals(frame, _small_config(chikou_mode=CHIKOU_NONE))
    assert result.at[exit_date, "Signal"] == "EXIT"
    assert not result.at[exit_date, "Position"]


def test_future_rows_do_not_participate_in_strategy_signals():
    frame = _strategy_frame()
    future = frame.tail(2).copy()
    future.index = pd.bdate_range(frame.index[-1] + pd.offsets.BDay(1), periods=2)
    future["IsFuture"] = True
    future[["Open", "High", "Low", "Close"]] = 10_000.0

    result = compute_ichimoku_signals(pd.concat([frame, future]), _small_config())
    assert result.index.equals(frame.index)


def test_appending_later_market_bars_cannot_change_earlier_signals():
    index = pd.bdate_range("2024-01-01", periods=140)
    close = pd.Series([100.0 + position * 0.4 for position in range(140)], index=index)
    source = pd.DataFrame(
        {
            "Open": close - 0.2,
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
            "Volume": 1_000_000.0,
        }
    )
    config = strategy_preset("Balanced")
    full = compute_ichimoku_signals(compute_ichimoku(source), config)
    prefix = compute_ichimoku_signals(compute_ichimoku(source.iloc[:110]), config)

    pd.testing.assert_series_equal(full.loc[prefix.index, "Signal"], prefix["Signal"])
    pd.testing.assert_series_equal(full.loc[prefix.index, "Forward_Senkou_B"], prefix["Forward_Senkou_B"])


def test_fixed_stop_is_an_immediate_safety_override():
    frame = _strategy_frame()
    config = _small_config(
        chikou_mode=CHIKOU_NONE,
        exit_event="Close below entire Kumo",
        fixed_stop_loss_pct=5.0,
    )
    initial = compute_ichimoku_signals(frame, config)
    buy = initial.index[initial["Signal"].eq("BUY")][0]
    stop_date = initial.index[initial.index.get_loc(buy) + 2]
    frame.loc[stop_date, ["Close", "Senkou_A", "Senkou_B"]] = [90.0, 60.0, 50.0]

    result = compute_ichimoku_signals(frame, config)

    assert result.at[stop_date, "Signal"] == "EXIT"
    assert result.at[stop_date, "Signal_Reason"] == "Fixed stop-loss (5%)"


def test_comparison_uses_one_common_cash_start_and_keeps_no_trade_curve_flat():
    frame = _strategy_frame(90)
    custom = _small_config(
        entry_event=ENTRY_TK_CROSS,
        price_location="Any location",
        require_tk_alignment=False,
        require_forward_bullish=False,
        chikou_mode=CHIKOU_NONE,
    )
    comparison, replays, common_start = build_strategy_comparison(
        frame,
        custom,
        ReplayConfig(
            initial_capital=100_000.0,
            transaction_cost_pct=0.0,
            brokerage_per_order=0.0,
            stcg_rate=0.0,
            ltcg_rate=0.0,
        ),
    )

    assert common_start == comparison.index[0]
    assert set(comparison.columns) == {"Aggressive", "Balanced", "Conservative", "Custom"}
    assert (comparison.iloc[0] == 100_000.0).all()
    assert (comparison["Custom"] == 100_000.0).all()
    assert set(replays) == set(comparison.columns)
