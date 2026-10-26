"""Behavioral checks for raw retention, usable windows and dated eligibility."""

import numpy as np
import pandas as pd
import pytest

import data
import data_backtest as db
import market_data as md
from backtest_engine import BacktestConfig, _precompute_all_metrics, rank_universe_at_date, run_backtest
from equity_data import (
    EQUITY_POLICY_VERSION,
    accepted_equity_rows,
    coverage_by_date,
    observed_sessions,
    recent_coverage,
)
from momentum_engine import precompute_metrics, score_momentum
from momentum_ranking import eligibility_reason, rank_momentum_frame
from stage2_breadth import build_breadth_score_history

from .conftest import make_ohlcv


def snapshot_for(frames):
    prices = pd.concat(
        [
            frame.rename_axis("date").reset_index().assign(symbol=symbol, series_type="equity")
            for symbol, frame in frames.items()
        ],
        ignore_index=True,
    )
    history = pd.DataFrame({"INDEX_NAME": "NIFTY50", "TIME_STAMP": prices.date.min(), "SYMBOL": list(frames)})
    return md.MarketSnapshot(prices, history, "prices", "members", prices.date.max())


def rising(n=600):
    return make_ohlcv(n, close=(100 * np.exp(np.arange(n) * 0.002 + np.sin(np.arange(n)) * 0.01)).tolist())


def test_raw_history_is_unchanged_and_indices_do_not_use_equity_volume_policy():
    frame = rising(270)
    frame.loc[frame.index[100], "Volume"] = 0
    snapshot = snapshot_for({"A": frame})
    index = snapshot.prices.tail(1).assign(symbol="Nifty 50", series_type="index_price", Volume=0)
    raw = pd.concat([snapshot.prices, index], ignore_index=True)
    snapshot = md.MarketSnapshot(raw, snapshot.membership, "prices", "members")
    before = raw.copy(deep=True)
    assert frame.index[100] not in snapshot.ohlcv()["A"].index
    assert len(snapshot.series("index_price")["Nifty 50"]) == 1
    pd.testing.assert_frame_equal(raw, before)


def test_invalid_and_unknown_volume_cannot_establish_a_trading_observation():
    frame = rising(9)
    frame["Volume"] = [100, 0, -1, np.nan, np.inf, 100, 100, 100, 100]
    frame["Close"] = [100, 100, 100, 100, 100, 0, np.nan, np.inf, -1]
    assert accepted_equity_rows(frame).index.tolist() == [frame.index[0]]


def test_momentum_windows_count_real_observations_before_computing_returns():
    raw = rising(300)
    raw.loc[raw.index[[80, 110, 160, 190, 230, 280]], "Volume"] = 0
    expected = raw[raw.Volume.gt(0)]
    metrics = precompute_metrics(raw)
    assert metrics.index.equals(expected.index)
    assert metrics.iloc[-1]["_count"] == 294
    assert metrics.iloc[-1]["1Y_Change"] == round((expected.Close.iloc[-1] / expected.Close.iloc[-252] - 1) * 100, 2)
    assert score_momentum(raw) == score_momentum(expected)
    assert (
        eligibility_reason(precompute_metrics(raw.tail(252)).iloc[-1], minimum_median_volume=0)
        == "insufficient_history"
    )


def test_screener_replay_and_quant_readers_share_filtered_data_and_coverage(monkeypatch):
    healthy = rising()
    sparse = healthy.copy()
    sparse.loc[sparse.index[-230:-10], "Volume"] = 0
    snapshot = snapshot_for({"HEALTHY": healthy, "SPARSE": sparse})
    monkeypatch.setattr(md, "load_snapshot", lambda *a: snapshot)
    screen = data._load_and_score(snapshot.constituents(), True, as_of_date=snapshot.as_of, snapshot=snapshot)
    loaded = db.load_ohlcv_for_backtest(snapshot=snapshot)
    quant = db.load_quant_ohlcv_snapshot(snapshot=snapshot)
    pd.testing.assert_frame_equal(snapshot.ohlcv()["SPARSE"], quant.symbol_data["SPARSE"])
    assert loaded.symbol_data["SPARSE"].Volume.gt(0).all()
    row = screen.set_index("Symbol").loc["SPARSE"]
    assert row["Stale Sessions"] == 0
    assert row["_count"] > 252
    assert eligibility_reason(row) == "incomplete_recent_coverage"
    settings = dict(min_history_days=252, trading_calendar=snapshot.trading_calendar())
    expected = rank_momentum_frame(screen, "1Y").Symbol.tolist()
    prices = snapshot.ohlcv()
    assert rank_universe_at_date(prices, snapshot.as_of, "1Y", **settings) == expected == ["HEALTHY"]
    assert (
        rank_universe_at_date(prices, snapshot.as_of, "1Y", precomputed=_precompute_all_metrics(prices), **settings)
        == expected
    )
    assert "SPARSE" not in set(build_breadth_score_history(snapshot.prices).query("date == @snapshot.as_of").symbol)


def test_coverage_counts_missing_rows_and_preserves_special_weekend_sessions():
    healthy = rising(300)
    saturday = healthy.iloc[[-1]].copy()
    saturday.index = pd.DatetimeIndex([healthy.index[-1] + pd.offsets.Week(weekday=5)])
    healthy = pd.concat([healthy, saturday])
    sparse = healthy.drop(healthy.index[-30:-1])
    holiday = healthy.iloc[[-1]].copy()
    holiday.index += pd.Timedelta(days=2)
    holiday.Volume = 0
    calendar = observed_sessions([pd.concat([healthy, holiday]), sparse])
    assert saturday.index[0] in calendar
    assert holiday.index[0] not in calendar
    assert recent_coverage(healthy.index, calendar[-1], calendar) == 1
    assert recent_coverage(sparse.index, calendar[-1], calendar) < 0.95


def test_historical_coverage_and_metrics_do_not_see_future_gaps():
    raw = rising(600)
    changed = raw.copy()
    changed.loc[changed.index[350:550], "Volume"] = 0
    calendar = raw.index
    past = raw.index[300]
    dates = accepted_equity_rows(changed).index
    assert recent_coverage(dates, past, calendar) == 1
    assert coverage_by_date(dates, calendar).loc[past] == 1
    pd.testing.assert_frame_equal(
        precompute_metrics(raw).loc[:past], precompute_metrics(changed).loc[:past], check_freq=False
    )
    expected = rank_universe_at_date({"A": raw}, past, "1Y", min_history_days=252, trading_calendar=calendar)
    assert (
        rank_universe_at_date({"A": changed}, past, "1Y", min_history_days=252, trading_calendar=calendar) == expected
    )


def test_coverage_threshold_and_listing_start_are_explicit():
    calendar = pd.bdate_range("2020-01-01", periods=400)
    assert recent_coverage(calendar[-100:], calendar[-1], calendar) == 1
    at_limit = calendar.delete(np.arange(148, 160))
    assert recent_coverage(at_limit, calendar[-1], calendar) == pytest.approx(240 / 252)
    assert eligibility_reason({"_count": 400, "Trading Coverage": 240 / 252}, minimum_median_volume=0) is None
    assert (
        eligibility_reason({"_count": 400, "Trading Coverage": 239 / 252}, minimum_median_volume=0)
        == "incomplete_recent_coverage"
    )


def test_breadth_coverage_matches_runtime_coverage_across_gaps_and_window_edges():
    calendar = pd.bdate_range("2020-01-01", periods=600)
    dates = calendar.delete(np.r_[20:30, 270:350, 500:520])
    historical = coverage_by_date(dates, calendar)
    for position in [0, 19, 20, 250, 251, 252, 270, 350, len(dates) - 1]:
        day = dates[position]
        assert historical.loc[day] == pytest.approx(recent_coverage(dates, day, calendar))


def test_policy_change_recalculates_existing_score_cache_without_a_new_parquet(monkeypatch):
    snapshot = snapshot_for({"A": rising(300)})
    target = str(snapshot.as_of.date())
    monkeypatch.setattr(md, "load_snapshot", lambda *a: snapshot)
    monkeypatch.setattr(
        data,
        "_score_cache",
        {"momentum": {"key": (target, snapshot.revisions), "data": pd.DataFrame({"Symbol": ["OBSOLETE"]})}},
    )
    result, _, source = data.resolve_screener_data(True, as_of_date=target)
    assert source == "shared" and result.Symbol.tolist() == ["A"]
    assert result.attrs["equity_policy_version"] == EQUITY_POLICY_VERSION


def test_selected_sparse_universe_keeps_the_whole_market_session_calendar():
    healthy = rising(600)
    sparse = healthy.drop(healthy.index[-230:-10])
    result = run_backtest(
        {"SPARSE": sparse},
        {},
        BacktestConfig(
            m=1,
            n=2,
            rebalance_freq="weekly",
            sort_method="1Y",
            start_date=str(healthy.index[-60].date()),
            end_date=str(healthy.index[-1].date()),
            min_history_days=252,
            market_calendar=healthy.index,
        ),
    )
    assert "error" not in result
    assert len(result["trading_days"]) == 60
    assert result["nav"]["Full Rebalance"].eq(100).all()
