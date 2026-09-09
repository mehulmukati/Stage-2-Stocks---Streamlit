import math

import pandas as pd
import pytest

from ha_ema_engine import (
    HAEMAStrategyConfig,
    compute_ha_ema_signals,
    latest_ha_ema_state,
    resample_weekly_ohlcv,
    weekly_decision_mask,
)
from strategy_replay import ReplayConfig, replay_single_stock


def _signal_source() -> pd.DataFrame:
    weekly_ohlc = [
        (100.0, 110.0, 90.0, 100.0),
        (100.0, 130.0, 100.0, 120.0),
        (120.0, 125.0, 100.0, 110.0),
        (100.0, 110.0, 80.0, 90.0),
        (85.0, 95.0, 80.0, 90.0),
    ]
    frames = []
    for week, (open_, high, low, close) in enumerate(weekly_ohlc):
        dates = pd.bdate_range(pd.Timestamp("2025-01-06") + pd.Timedelta(weeks=week), periods=5)
        path = pd.Series([open_ + (close - open_) * position / 4 for position in range(5)], index=dates)
        frames.append(
            pd.DataFrame(
                {
                    "Open": path,
                    "High": high,
                    "Low": low,
                    "Close": path,
                    "Volume": 200_000.0,
                }
            )
        )
    return pd.concat(frames)


def _config() -> HAEMAStrategyConfig:
    return HAEMAStrategyConfig(
        ema_fast=2,
        ema_slow=3,
        average_volume_length=1,
        minimum_average_volume=100.0,
        return_length=1,
        minimum_return_pct=0.0,
        wick_tolerance=0.0,
    )


def test_heikin_ashi_uses_recursive_open_formula():
    source = _signal_source()
    result = compute_ha_ema_signals(source, _config())
    weekly = resample_weekly_ohlcv(source)

    expected_first_close = weekly.iloc[0][["Open", "High", "Low", "Close"]].mean()
    expected_first_open = (weekly["Open"].iloc[0] + weekly["Close"].iloc[0]) / 2.0
    assert result["HA_Close"].iloc[0] == expected_first_close
    assert result["HA_Open"].iloc[0] == expected_first_open
    assert result["HA_Open"].iloc[1] == (expected_first_open + expected_first_close) / 2.0


def test_state_machine_emits_one_weekend_buy_then_one_weekend_exit():
    result = compute_ha_ema_signals(_signal_source(), _config())
    events = result[result["Signal"].ne("")]["Signal"].tolist()

    assert events == ["BUY", "EXIT"]
    assert result.loc[result["Signal"] == "BUY", "Position"].all()
    assert not result.loc[result["Signal"] == "EXIT", "Position"].any()
    assert result.index[result["Signal"] == "BUY"][0].day_name() == "Friday"
    assert result.index[result["Signal"] == "EXIT"][0].day_name() == "Friday"
    assert not result.loc[~result["Decision_Eligible"], "Signal"].ne("").any()
    buy = result[result["Signal"] == "BUY"].iloc[0]
    assert buy["Previous_Two_Wicks"]
    assert buy["No_Lower_Wick"]


def test_entry_requires_the_previous_weekly_ha_candle_to_have_two_wicks():
    source = _signal_source()
    first_week = source.index.to_period("W-FRI") == source.index[0].to_period("W-FRI")
    source.loc[first_week, "Low"] = 100.0
    source.loc[first_week, "High"] = 100.0

    result = compute_ha_ema_signals(source, _config())

    assert not result.iloc[0]["Two_Wicks"]
    assert not result.iloc[1]["Previous_Two_Wicks"]
    assert result.iloc[1]["No_Lower_Wick"]
    assert result.iloc[1]["Signal"] == ""


def test_daily_source_is_aggregated_to_true_weekly_ohlcv():
    source = _signal_source()
    result = compute_ha_ema_signals(source, _config())

    assert len(result) == 5
    assert result.index[0] == pd.Timestamp("2025-01-10")
    assert result.iloc[0]["Open"] == 100.0
    assert result.iloc[0]["High"] == 110.0
    assert result.iloc[0]["Low"] == 90.0
    assert result.iloc[0]["Close"] == 100.0
    assert result.iloc[0]["Volume"] == 1_000_000.0
    assert result.iloc[0]["Execution_Date"] == pd.Timestamp("2025-01-06")
    assert result.iloc[1]["Return_Pct"] == pytest.approx(20.0)


def test_ema_values_are_calculated_from_weekly_closes():
    result = compute_ha_ema_signals(_signal_source(), _config())
    weekly_closes = pd.Series([100.0, 120.0, 110.0, 90.0, 90.0], index=result.index)

    pd.testing.assert_series_equal(
        result["EMA_Fast"],
        weekly_closes.ewm(span=2, adjust=False).mean(),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        result["EMA_Slow"],
        weekly_closes.ewm(span=3, adjust=False).mean(),
        check_names=False,
    )


def test_return_gate_requires_complete_lookback():
    result = compute_ha_ema_signals(_signal_source(), _config())
    assert pd.isna(result["Return_Pct"].iloc[0])
    assert not result["Return_Filter"].iloc[0]


def test_latest_state_reports_gates_and_last_signal():
    result = compute_ha_ema_signals(_signal_source(), _config())
    state = latest_ha_ema_state(result, "TEST", _config())

    assert state["sufficient_data"] is True
    assert len(state["conditions"]) == 8
    assert state["last_signal"]["type"] == "EXIT"


def test_replay_executes_weekend_decisions_at_following_week_open():
    result = compute_ha_ema_signals(_signal_source(), _config())
    replay = replay_single_stock(
        result,
        ReplayConfig(
            initial_capital=1_000.0,
            transaction_cost_pct=0.0,
            brokerage_per_order=0.0,
            stcg_rate=0.0,
            ltcg_rate=0.0,
        ),
    )
    trade = replay["trades"].iloc[0]
    buy_signal_date = result.index[result["Signal"] == "BUY"][0]
    exit_signal_date = result.index[result["Signal"] == "EXIT"][0]

    assert trade["Entry Signal"] == buy_signal_date
    buy_execution_row = result.iloc[result.index.get_loc(buy_signal_date) + 1]
    assert trade["Entry Date"] == buy_execution_row["Execution_Date"]
    assert trade["Exit Signal"] == exit_signal_date
    exit_execution_row = result.iloc[result.index.get_loc(exit_signal_date) + 1]
    assert trade["Exit Date"] == exit_execution_row["Execution_Date"]
    assert trade["Shares"] == math.floor(1_000.0 / trade["Entry Price"])
    assert trade["Entry Date"].day_name() == "Monday"
    assert trade["Exit Date"].day_name() == "Monday"
    assert replay["summary"]["comparison_start_date"] == trade["Entry Date"]
    assert replay["equity"].index[0] == result.index[result.index.get_loc(buy_signal_date) + 1]
    assert replay["equity"]["Buy_Hold_Value"].notna().all()
    assert replay["summary"]["comparison_start_date"] == pd.Timestamp("2025-01-20")


def test_incomplete_current_week_is_not_a_decision_week():
    dates = pd.bdate_range("2025-01-06", periods=4)
    mask = weekly_decision_mask(dates, as_of_date="2025-01-09")

    assert not mask.any()


def test_stale_data_cannot_turn_a_truncated_week_into_a_completed_decision():
    dates = pd.bdate_range("2025-01-06", periods=4)
    mask = weekly_decision_mask(dates, as_of_date="2025-01-20")

    assert not mask.any()


def test_friday_holiday_uses_thursday_as_the_weekend_decision(tmp_path):
    dates = pd.bdate_range("2025-01-06", periods=4)
    holiday_file = tmp_path / "holidays.json"
    holiday_file.write_text(
        '{"CM": [{"tradingDate": "10-Jan-2025"}]}',
        encoding="utf-8",
    )

    mask = weekly_decision_mask(dates, as_of_date="2025-01-09", holidays_path=str(holiday_file))

    assert mask.sum() == 1
    assert mask.loc[pd.Timestamp("2025-01-09")]


def test_monday_holiday_executes_at_tuesdays_actual_open():
    dates = pd.to_datetime(["2025-01-03", "2025-01-07", "2025-01-10", "2025-01-13"])
    signals = pd.DataFrame(
        {
            "Open": [10.0, 11.0, 12.0, 13.0],
            "Close": [10.0, 11.0, 12.0, 13.0],
            "Signal": ["BUY", "", "EXIT", ""],
        },
        index=dates,
    )
    replay = replay_single_stock(
        signals,
        ReplayConfig(
            initial_capital=1_000.0,
            transaction_cost_pct=0.0,
            brokerage_per_order=0.0,
            stcg_rate=0.0,
            ltcg_rate=0.0,
        ),
    )

    trade = replay["trades"].iloc[0]
    assert trade["Entry Date"] == pd.Timestamp("2025-01-07")
    assert trade["Entry Price"] == 11.0


def test_tax_estimate_reduces_liquidation_value_for_profitable_short_term_trade():
    dates = pd.bdate_range("2025-01-01", periods=5)
    signals = pd.DataFrame(
        {
            "Open": [10.0, 10.0, 12.0, 15.0, 15.0],
            "Close": [10.0, 11.0, 12.0, 15.0, 15.0],
            "Signal": ["BUY", "", "", "EXIT", ""],
        },
        index=dates,
    )
    replay = replay_single_stock(
        signals,
        ReplayConfig(
            initial_capital=1_000.0,
            transaction_cost_pct=0.0,
            brokerage_per_order=0.0,
            stcg_rate=0.20,
            ltcg_rate=0.0,
        ),
    )

    summary = replay["summary"]
    assert summary["estimated_liquidation_tax"] > 0
    assert summary["after_tax_liquidation_value"] < summary["pre_tax_liquidation_value"]
    assert summary["buy_hold_pre_tax_return_pct"] > 0


def test_invalid_fast_slow_pair_is_rejected():
    with pytest.raises(ValueError, match="Fast EMA"):
        compute_ha_ema_signals(_signal_source(), HAEMAStrategyConfig(ema_fast=30, ema_slow=10))


def test_final_signal_is_reported_as_unexecuted():
    dates = pd.bdate_range("2025-01-01", periods=2)
    signals = pd.DataFrame({"Open": [10.0, 11.0], "Close": [10.0, 11.0], "Signal": ["", "BUY"]}, index=dates)
    replay = replay_single_stock(signals, ReplayConfig(stcg_rate=0.0, ltcg_rate=0.0))
    assert replay["summary"]["comparison_available"] is False
    assert replay["summary"]["unexecuted_signal"] == "BUY"


def test_no_buy_returns_no_comparison_instead_of_flat_performance():
    dates = pd.bdate_range("2025-01-01", periods=3)
    signals = pd.DataFrame(
        {"Open": [10.0, 11.0, 12.0], "Close": [10.0, 11.0, 12.0], "Signal": ["", "", ""]}, index=dates
    )
    replay = replay_single_stock(signals)
    assert replay["summary"]["comparison_available"] is False
    assert replay["equity"].empty


def test_replay_can_record_an_execution_date_distinct_from_period_end():
    period_ends = pd.to_datetime(["2025-01-03", "2025-01-10", "2025-01-17"])
    signals = pd.DataFrame(
        {
            "Open": [10.0, 11.0, 12.0],
            "Close": [10.0, 12.0, 13.0],
            "Execution_Date": pd.to_datetime(["2024-12-30", "2025-01-06", "2025-01-13"]),
            "Signal": ["BUY", "EXIT", ""],
        },
        index=period_ends,
    )

    replay = replay_single_stock(
        signals,
        ReplayConfig(
            initial_capital=1_000.0,
            transaction_cost_pct=0.0,
            brokerage_per_order=0.0,
            stcg_rate=0.0,
            ltcg_rate=0.0,
        ),
    )
    trade = replay["trades"].iloc[0]

    assert trade["Entry Signal"] == period_ends[0]
    assert trade["Entry Date"] == pd.Timestamp("2025-01-06")
    assert trade["Exit Signal"] == period_ends[1]
    assert trade["Exit Date"] == pd.Timestamp("2025-01-13")
    assert replay["summary"]["comparison_start_date"] == pd.Timestamp("2025-01-06")
    assert replay["equity"].index[0] == period_ends[1]
