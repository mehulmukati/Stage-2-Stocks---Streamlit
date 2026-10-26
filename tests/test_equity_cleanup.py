"""Selective cleanup, trusted no-trade marks and fixed-session regression cases."""

import json

import numpy as np
import pandas as pd
import pytest

import equity_data as eq
import market_data as md
from backtest_engine import _close_price, _precompute_all_metrics, rank_universe_at_date
from momentum_engine import precompute_metrics
from quant_portfolio_engine import _price
from scripts.clean_equity_observations import cleanup, sha256
from scripts.refresh_market_data import merge_observations

from .conftest import make_ohlcv


def long_frame(frame, symbol="A", kind="equity"):
    return frame.rename_axis("date").reset_index().assign(symbol=symbol, series_type=kind)


def test_cleanup_removes_only_confirmed_holidays_and_keeps_other_series():
    frame = make_ohlcv(4)
    frame.index = pd.to_datetime(["2026-01-14", "2026-01-15", "2026-01-16", "2026-01-19"])
    frame.loc[frame.index[1:], "Volume"] = 0
    raw = long_frame(frame)
    index = raw.iloc[[1]].assign(symbol="Nifty 50", series_type="index_price")
    etf = raw.iloc[[1]].assign(symbol="ETF", series_type="etf_adjusted")
    original = pd.concat([raw, index, etf], ignore_index=True)
    cleaned, removed, repairs = eq.prepare_publication(original)
    assert removed.date.tolist() == [pd.Timestamp("2026-01-15")]
    assert removed.series_type.eq("equity").all() and repairs == []
    assert len(cleaned) == len(original) - 1
    kept = cleaned.query("series_type == 'equity'").set_index("date")
    assert kept.loc["2026-01-16", "observation_status"] == "unverified_zero_volume"
    assert kept.loc["2026-01-19", "observation_status"] == "unverified_zero_volume"
    assert not kept.loc["2026-01-16", "is_tradable"]
    again, removed_again, repairs_again = eq.prepare_publication(cleaned)
    assert removed_again.empty and not repairs_again
    pd.testing.assert_frame_equal(cleaned, again)


def test_positive_volume_on_verified_closure_is_flagged_not_deleted():
    raw = long_frame(make_ohlcv(1, start="2026-01-15"))
    cleaned, removed, _ = eq.prepare_publication(raw)
    assert removed.empty
    assert cleaned.iloc[0].observation_status == "calendar_conflict"
    assert not cleaned.iloc[0].is_tradable


def test_verified_zero_volume_mark_supports_prices_but_never_candles_or_execution():
    frame = make_ohlcv(300, close=np.linspace(100, 150, 300).tolist())
    day = frame.index[-5]
    frame.loc[day, ["Open", "High", "Low", "Close"]] = frame.loc[frame.index[-6], "Close"]
    frame.loc[day, "Volume"] = 0
    frame["price_status"] = ""
    frame["verification_source"] = ""
    frame["verified_at"] = ""
    frame.loc[day, ["price_status", "verification_source", "verified_at"]] = [
        "verified_mark",
        "https://example.com/exchange/closing-mark",
        "2026-10-26T00:00:00Z",
    ]
    retained = eq.accepted_equity_rows(frame)
    aligned = eq.session_frame(retained, frame.index)
    assert retained.loc[day, "observation_status"] == "verified_no_trade"
    assert aligned.loc[day, "Close"] == frame.loc[day, "Close"]
    assert pd.isna(aligned.loc[day, "Open"])
    assert pd.isna(aligned.loc[day, "High"])
    assert _price(aligned, day, "Open") is None
    assert _close_price({"A": aligned}, "A", day) is None
    metrics = precompute_metrics(frame, frame.index)
    returns = aligned.Close.tail(252).pct_change(fill_method=None).dropna()
    assert metrics.iloc[-1]["Sharpe_1Y"] == round(returns.mean() / returns.std() * np.sqrt(252), 3)
    assert metrics.iloc[-1]["_count"] == 300


def test_unverified_mark_does_not_create_a_zero_return_or_bridge_a_gap():
    frame = make_ohlcv(300, close=np.linspace(100, 150, 300).tolist())
    frame.loc[frame.index[-5], "Volume"] = 0
    aligned = eq.session_frame(frame, frame.index)
    assert pd.isna(aligned.Close.iloc[-5])
    returns = aligned.Close.pct_change(fill_method=None)
    assert pd.isna(returns.iloc[-5]) and pd.isna(returns.iloc[-4])
    assert pd.isna(precompute_metrics(frame, frame.index).iloc[-1]["Sharpe_1Y"])
    assert pd.isna(precompute_metrics(frame, frame.index).iloc[-1]["Pos_Days_3M"])
    assert pd.isna(precompute_metrics(frame, frame.index).iloc[-1]["52w_High"])
    from momentum_engine import _calculate_positive_days_pct, _calculate_sharpe

    assert _calculate_sharpe(frame, 63, frame.index) is None
    assert _calculate_positive_days_pct(frame, 3, frame.index) is None


def test_240_recent_plus_12_ancient_rows_cannot_produce_a_years_long_1y_return():
    calendar = pd.bdate_range("2020-01-01", periods=1000)
    frame = make_ohlcv(1000, close=np.linspace(100, 200, 1000).tolist())
    sparse = frame.iloc[np.r_[0:12, 760:1000]].copy()
    metrics = precompute_metrics(sparse, calendar)
    assert len(sparse) == 252
    assert eq.recent_coverage(sparse.index, calendar[-1], calendar) == pytest.approx(240 / 252)
    assert pd.isna(metrics.iloc[-1]["1Y_Change"])
    assert pd.isna(metrics.iloc[-1]["Sharpe_1Y"])
    assert metrics.index[-252] == calendar[-252]
    settings = dict(min_history_days=252, apply_volume_filter=False, trading_calendar=calendar)
    assert rank_universe_at_date({"A": sparse}, calendar[-1], "1Y", **settings) == []
    fast = _precompute_all_metrics({"A": sparse}, calendar)
    assert rank_universe_at_date({"A": sparse}, calendar[-1], "1Y", precomputed=fast, **settings) == []


def test_source_backed_volume_repair_preserves_adjusted_prices(tmp_path, monkeypatch):
    frame = long_frame(make_ohlcv(1))
    frame.loc[0, ["Open", "High", "Low", "Close", "Volume"]] = [50, 51, 49, 50.5, 0]
    (tmp_path / "data").mkdir()
    entry = {
        "symbol": "A",
        "date": "2020-01-01",
        "kind": "volume_correction",
        "volume": 123,
        "source": "https://example.com/exchange/bhavcopy",
        "verified_at": "2026-10-26T00:00:00Z",
        "unadjusted_ohlc": [100, 102, 98, 101],
    }
    (tmp_path / "data" / "equity_observation_verifications.json").write_text(json.dumps({"entries": [entry]}))
    monkeypatch.setattr(eq, "ROOT", tmp_path)
    corrected, repairs = eq.apply_verifications(frame)
    assert corrected.loc[0, "Volume"] == 123 and len(repairs) == 1
    pd.testing.assert_frame_equal(corrected[["Open", "High", "Low", "Close"]], frame[["Open", "High", "Low", "Close"]])
    wrong_shape = frame.copy()
    wrong_shape.loc[0, "High"] = 80
    rejected, repairs = eq.apply_verifications(wrong_shape)
    assert rejected.loc[0, "Volume"] == 0 and repairs == []


def test_refresh_preserves_a_verified_mark_when_the_matching_vendor_quote_is_unchanged():
    old = long_frame(make_ohlcv(1))
    old.Volume = 0
    old["price_status"] = "verified_mark"
    old["verification_source"] = "https://example.com/exchange/closing-mark"
    old["verified_at"] = "2026-10-26T00:00:00Z"
    new = old[md.PRICE_COLUMNS].copy()
    merged = merge_observations(old, new)
    assert eq.classify_equity_rows(merged).iloc[0].price_status == "verified_mark"


def test_cleanup_dry_run_is_read_only_and_apply_archives_the_original(market, tmp_path):
    old_sha = sha256(md.PRICE_PATH)
    membership_sha = sha256(md.MEMBERSHIP_PATH)
    report = cleanup(tmp_path / "audit", apply=False)
    assert not report["applied"] and sha256(md.PRICE_PATH) == old_sha
    applied = cleanup(tmp_path / "audit", apply=True)
    assert applied["applied"] and sha256(applied["archive"]) == old_sha
    assert applied["other_parquet_files_unchanged"]
    assert sha256(md.MEMBERSHIP_PATH) == membership_sha


def test_verified_special_weekend_is_a_session_and_can_be_the_target():
    import data

    observed = pd.DatetimeIndex(pd.to_datetime(["2026-11-06", "2026-11-09"]))
    calendar = eq.session_calendar(observed)
    assert pd.Timestamp("2026-11-08") in calendar
    assert data.get_last_valid_trading_date("2026-11-08", frozenset()) == "2026-11-08"


def test_weekly_strategy_keeps_an_empty_week_and_resumes_without_an_invented_candle():
    from ha_ema_engine import HAEMAStrategyConfig, compute_ha_ema_signals
    from ichimoku_engine import resample_ohlcv
    from quant_strategy_registry import HAEMAAdapter

    frame = make_ohlcv(400, close=np.linspace(100, 200, 400).tolist())
    missing_week = frame.index.to_period("W-FRI").unique()[30]
    raw = frame.loc[frame.index.to_period("W-FRI") != missing_week]
    aligned = eq.session_frame(raw, frame.index)
    config = HAEMAStrategyConfig(
        ema_fast=2, ema_slow=3, average_volume_length=1, return_length=2, minimum_average_volume=0, minimum_return_pct=0
    )
    signals = compute_ha_ema_signals(aligned, config)
    empty = signals.loc[signals.index.to_period("W-FRI") == missing_week].iloc[0]
    assert pd.isna(empty.HA_Open) and pd.isna(empty.HA_Close)
    assert not empty.Long_Condition
    assert signals.loc[signals.index.to_period("W-FRI") > missing_week, "HA_Open"].notna().any()
    adapter = HAEMAAdapter(config)
    pd.testing.assert_frame_equal(adapter.compute(aligned), adapter.compute_many({"A": aligned})["A"])
    weekly = resample_ohlcv(aligned, "Weekly")
    assert weekly.loc[weekly.index.to_period("W-FRI") == missing_week, "Close"].isna().all()
