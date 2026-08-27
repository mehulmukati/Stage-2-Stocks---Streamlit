import json

import pandas as pd
import pytest

from ichimoku_engine import compute_ichimoku, latest_ichimoku_state, resample_ohlcv


def _linear_ohlc(n: int = 100, start: str = "2025-01-01") -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=n)
    close = pd.Series([100.0 + i for i in range(n)], index=dates)
    return pd.DataFrame(
        {
            "Open": close - 0.5,
            "High": close + 2.0,
            "Low": close - 2.0,
            "Close": close,
            "Volume": 1_000_000.0,
        }
    )


def test_exact_lines_and_displacement():
    source = _linear_ohlc()
    result = compute_ichimoku(source)

    source_position = 60
    displayed_position = source_position + 26
    close = source["Close"].iloc[source_position]
    assert result["Tenkan"].iloc[source_position] == pytest.approx(close - 4.0)
    assert result["Kijun"].iloc[source_position] == pytest.approx(close - 12.5)
    assert result["Senkou_A"].iloc[displayed_position] == pytest.approx(close - 8.25)
    assert result["Senkou_B"].iloc[displayed_position] == pytest.approx(close - 25.5)
    assert result["Chikou"].iloc[source_position - 26] == pytest.approx(close)


def test_cloud_projects_26_sessions_beyond_price():
    source = _linear_ohlc()
    result = compute_ichimoku(source)
    observed = result[~result["IsFuture"].astype(bool)]
    projected = result[result["IsFuture"].astype(bool)]

    assert len(projected) == 26
    assert projected.index.min() > observed.index.max()
    assert projected["Close"].isna().all()
    assert projected["Senkou_A"].iloc[-1] == pytest.approx((195.0 + 186.5) / 2.0)


def test_weekly_candles_use_true_ohlcv_aggregation():
    source = _linear_ohlc(10, start="2025-01-06")
    weekly = resample_ohlcv(source, "Weekly")

    assert len(weekly) == 2
    assert weekly.index[0] == source.index[4]
    assert weekly.iloc[0]["Open"] == source.iloc[0]["Open"]
    assert weekly.iloc[0]["High"] == source.iloc[:5]["High"].max()
    assert weekly.iloc[0]["Low"] == source.iloc[:5]["Low"].min()
    assert weekly.iloc[0]["Close"] == source.iloc[4]["Close"]
    assert weekly.iloc[0]["Volume"] == source.iloc[:5]["Volume"].sum()


def test_weekly_ichimoku_projects_26_weekly_bars():
    source = _linear_ohlc(400, start="2024-01-01")
    result = compute_ichimoku(source, timeframe="Weekly")
    observed = result[~result["IsFuture"].astype(bool)]
    projected = result[result["IsFuture"].astype(bool)]

    assert len(projected) == 26
    assert all(date.weekday() == 4 for date in projected.index)
    assert projected.index[0] > observed.index[-1]
    assert projected["Senkou_B"].iloc[-1] == pytest.approx(
        observed["High"].iloc[-52:].max() / 2.0 + observed["Low"].iloc[-52:].min() / 2.0
    )


def test_future_dates_skip_configured_nse_holiday(tmp_path):
    source = _linear_ohlc(60, start="2025-01-06")
    next_business_day = source.index[-1] + pd.offsets.BDay(1)
    holiday_file = tmp_path / "holidays.json"
    holiday_file.write_text(
        json.dumps({"CM": [{"tradingDate": next_business_day.strftime("%d-%b-%Y")}]}), encoding="utf-8"
    )

    result = compute_ichimoku(source, displacement=2, holidays_path=str(holiday_file))
    projected = result[result["IsFuture"].astype(bool)]
    assert next_business_day not in projected.index


def test_equality_plateau_produces_only_one_real_cross():
    dates = pd.bdate_range("2025-01-01", periods=7)
    tenkan = pd.Series([1.0, 1.0, 2.0, 2.0, 3.0, 4.0, 4.0], index=dates)
    kijun = pd.Series([2.0, 2.0, 2.0, 2.0, 2.0, 4.0, 3.0], index=dates)
    from ichimoku_engine import _crossovers

    signals = _crossovers(tenkan, kijun)
    assert signals.dropna().tolist() == ["bullish"]
    assert signals.dropna().index[0] == dates[4]


def test_latest_state_classifies_price_and_cross():
    source = _linear_ohlc()
    result = compute_ichimoku(source)
    result.at[source.index[-5], "TK_Cross"] = "bullish"
    result.at[source.index[-5], "Cross_Strength"] = "strong bullish"

    state = latest_ichimoku_state(result, "TEST")
    assert state["price_position"] == "above"
    assert state["displayed_cloud"] == "bullish"
    assert state["projected_cloud"] == "bullish"
    assert state["tk_relation"] == "above"
    assert state["last_cross"]["age_sessions"] == 4
    assert state["last_cross"]["strength"] == "strong bullish"


def test_missing_columns_raise_clear_error():
    source = _linear_ohlc().drop(columns="High")
    with pytest.raises(ValueError, match="High"):
        compute_ichimoku(source)


def test_invalid_timeframe_raises_clear_error():
    with pytest.raises(ValueError, match="Daily.*Weekly"):
        compute_ichimoku(_linear_ohlc(), timeframe="Monthly")


def test_input_is_not_modified_and_dates_are_normalised():
    source = _linear_ohlc(60).sort_index(ascending=False)
    original = source.copy(deep=True)
    compute_ichimoku(source)
    pd.testing.assert_frame_equal(source, original)


def test_empty_input_returns_empty_result():
    result = compute_ichimoku(pd.DataFrame())
    assert result.empty
    assert "Senkou_A" in result.columns
