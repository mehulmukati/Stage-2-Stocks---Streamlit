import numpy as np
import pandas as pd
import pytest

from weekly_hammer_reversal import (
    WEEK,
    HammerGeometry,
    evaluate_outcomes,
    exchange_weeks,
    hammer_features,
    membership_schedule,
    weekly_candles,
    wilder_rsi,
)


def daily_for(dates):
    return pd.DataFrame(
        {"Open": 100.0, "High": 104.0, "Low": 94.0, "Close": 103.0, "Volume": 1000.0, "is_tradable": True},
        index=pd.DatetimeIndex(dates),
    )


def test_shortened_and_partial_exchange_weeks_and_missing_sessions():
    sessions = pd.DatetimeIndex(["2026-04-01", "2026-04-02", "2026-04-06", "2026-04-07", "2026-04-08"])
    daily = daily_for(sessions)
    daily.loc["2026-04-02", ["High", "Low", "Close"]] = [108, 93, 106]
    weeks = exchange_weeks(sessions, "2026-04-08")
    result = weekly_candles(daily, weeks)
    assert list(result.complete_candle) == [True, False]
    assert list(result.iloc[0][["Open", "High", "Low", "Close", "Volume"]]) == [100, 108, 93, 106, 2000]
    assert pd.isna(result.iloc[-1].Open)
    missing = weekly_candles(daily.drop(pd.Timestamp("2026-04-02")), weeks)
    assert not missing.iloc[0].complete_candle
    assert missing.iloc[0].missing_sessions == 1
    known_closed = exchange_weeks(sessions[:2], "2026-04-02", closed_through="2026-04-05")
    assert known_closed.iloc[-1].complete


def test_weekend_special_session_is_in_its_calendar_week():
    dates = ["2026-11-06", "2026-11-08", "2026-11-09"]
    weeks = exchange_weeks(dates, "2026-11-09")
    assert weeks.iloc[0].signal_date == pd.Timestamp("2026-11-08")
    assert weeks.iloc[0].sessions == 2
    assert weeks.iloc[0].complete


@pytest.mark.parametrize(
    "o,h,low,c,expected",
    [
        (100, 104, 94, 103, True),
        (100, 104, 95, 103, False),
        (100, 107, 90, 103, False),
        (100, 105, 94, 100, False),
        (103, 104, 94, 100, False),
        (100, 99, 90, 102, False),
        (100, 100, 100, 100, False),
        (100, 106, 94, 103, True),
        (100, 101, 93, 101, True),
    ],
)
def test_hammer_geometry(o, h, low, c, expected):
    frame = pd.DataFrame(
        {"Open": [o], "High": [h], "Low": [low], "Close": [c], "Volume": [100], "complete_candle": [True]},
        index=pd.period_range("2020-01-05", periods=1, freq=WEEK),
    )
    assert hammer_features(frame).A.item() == expected
    capped = hammer_features(frame, geometry=HammerGeometry(max_body_fraction=0.1))
    assert capped.A.item() == (expected and abs(c - o) <= 0.1 * (h - low))


def test_rsi_reference_and_ema_readiness():
    closes = pd.Series(
        [
            44.34,
            44.09,
            44.15,
            43.61,
            44.33,
            44.83,
            45.10,
            45.42,
            45.84,
            46.08,
            45.89,
            46.03,
            45.61,
            46.28,
            46.28,
            46.00,
            46.03,
        ]
    )
    rsi = wilder_rsi(closes)
    assert rsi.iloc[:14].isna().all()
    assert rsi.iloc[14] == pytest.approx(70.4641, abs=0.0001)
    assert rsi.iloc[15] == pytest.approx(66.2496, abs=0.0001)
    assert wilder_rsi(pd.Series([1.0] * 15)).iloc[-1] == 50
    days = pd.date_range("2019-01-06", periods=31, freq="7D")
    bars = weekly_candles(daily_for(days), exchange_weeks(days, days[-1]))
    output = hammer_features(bars)
    assert output.rsi_ema.iloc[:27].isna().all()
    assert output.rsi_ema.iloc[27] == 50
    assert not hammer_features(bars, price_break=[bars.index[18]]).common_ready.any()


def test_lagged_references_and_volume_reset():
    days = pd.date_range("2019-01-06", periods=55, freq="7D")
    bars = weekly_candles(daily_for(days), exchange_weeks(days, days[-1]))
    bars.loc[bars.index[26], ["Volume", "High"]] = [3000, 112]
    output = hammer_features(bars)
    assert output.rvol20.iloc[26] == 3
    assert output.drawdown26.iloc[26] == pytest.approx(103 / 104 - 1)
    assert output.drawdown26.iloc[27] == pytest.approx(103 / 112 - 1)
    reset = hammer_features(bars, volume_break=[bars.index[26]])
    assert reset.rvol20.iloc[26:47].isna().all()
    assert reset.rvol20.iloc[47] == 1
    assert reset.rsi_ema.iloc[28] == 50
    pd.testing.assert_frame_equal(output.iloc[:30], hammer_features(bars.iloc[:30]))


def test_membership_respects_effective_date_and_never_backfills():
    sessions = pd.to_datetime(["2019-01-04", "2019-01-11", "2019-01-18", "2019-01-25"])
    weeks = exchange_weeks(sessions, "2019-01-27")
    membership = pd.DataFrame(
        {
            "TIME_STAMP": pd.to_datetime(["2019-01-08", "2019-01-22"]),
            "security_id": ["ISIN:A", "ISIN:B"],
        }
    )
    result = membership_schedule(membership, weeks, "2019-01-01", "2019-01-25")
    assert result.security_id.tolist() == ["ISIN:A", "ISIN:A", "ISIN:B"]
    assert result.signal_date.tolist() == list(sessions[1:])


def test_next_week_open_bounded_missing_quote_and_censoring():
    sessions = pd.bdate_range("2019-12-23", "2020-01-21")
    daily = daily_for(sessions)
    daily.loc["2019-12-30", "is_tradable"] = False
    daily.loc["2020-01-06":"2020-01-10", "is_tradable"] = False
    daily.loc["2020-01-13", ["Low", "High"]] = [80, 160]
    weeks = exchange_weeks(sessions, "2020-01-21")
    decisions = pd.DataFrame(
        {
            "week": pd.to_datetime(["2019-12-27", "2020-01-03", "2020-01-10"]).to_period(WEEK),
            "signal_id": [0, 1, 2],
            "segment": ["development"] * 3,
        }
    )
    benchmark = pd.Series(200.0, index=sessions)
    output = evaluate_outcomes(decisions, daily, weeks, "2020-01-21", benchmark, horizons=(1, 2))
    one = output.loc[output.holding_weeks.eq(1)].set_index("signal_id")
    two = output.loc[output.holding_weeks.eq(2)].set_index("signal_id")
    assert one.loc[0, "entry_date"] == pd.Timestamp("2019-12-31")
    assert one.loc[0, "status"] == "missing_exit"
    assert one.loc[1, "status"] == "missing_entry"
    assert two.loc[0, "exit_date"] == pd.Timestamp("2020-01-13")
    assert two.loc[0, "mfe"] == pytest.approx(0.04)
    assert two.loc[2, "status"] == "right_censored_exit"
    assert two.loc[0, "benchmark_open_return"] == 0
    blocked = evaluate_outcomes(
        decisions, daily, weeks, "2020-01-21", benchmark, price_break=[pd.Timestamp("2020-01-13")], horizons=(2,)
    )
    assert blocked.loc[0, "status"] == "unresolved_price_event"
    assert np.isnan(blocked.loc[0, "gross_return"])


def test_segment_crossing_censors_even_if_quote_available():
    sessions = pd.bdate_range("2022-12-19", "2023-01-16")
    daily = daily_for(sessions)
    weeks = exchange_weeks(sessions, "2023-01-16")
    decisions = pd.DataFrame(
        {
            "week": [pd.Period("2022-12-23", freq=WEEK)],
            "signal_id": [0],
            "segment": ["development"],
        }
    )
    outcome = evaluate_outcomes(decisions, daily, weeks, "2023-01-16", daily.Open, horizons=(1,))
    assert outcome.iloc[0].status == "segment_censored"
    assert pd.isna(outcome.iloc[0].gross_return)
