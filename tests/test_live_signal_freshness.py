from datetime import date, datetime

import pandas as pd

import app_live_signal as live
import data_backtest as db
import market_data as md


def _long(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": symbol, "date": pd.Timestamp(day), "Close": close, "High": close, "Volume": 1000}
            for symbol, day, close in rows
        ]
    )


def test_target_session_rolls_forward_at_established_1900_cutoff(monkeypatch):
    monkeypatch.setattr(db, "load_nse_holidays", lambda: frozenset())

    assert db._get_target_key(datetime(2026, 8, 26, 18, 59)) == "2026-08-25"
    assert db._get_target_key(datetime(2026, 8, 26, 19, 0)) == "2026-08-26"


def test_one_session_partial_coverage_is_usable_for_signal(monkeypatch):
    partial = _long([("A", "2026-08-25", 101), ("B", "2026-08-24", 200)])
    monkeypatch.setattr(db, "load_nse_holidays", lambda: frozenset())

    result = db._assess_ohlcv_freshness(partial, "2026-08-25", ["A", "B"], "parquet+delta")

    assert not result.is_fresh
    assert result.missing_target_symbols == ["B"]
    assert result.stale_symbols == []
    assert result.is_usable_for_signal


def test_unpriced_nonholding_does_not_block_signal_coverage(monkeypatch):
    prices = _long([("A", "2026-08-25", 101)])
    monkeypatch.setattr(db, "load_nse_holidays", lambda: frozenset())
    result = db._assess_ohlcv_freshness(prices, "2026-08-25", ["A", "NEW"], "parquet")

    excluded, blocking, coverage_date = live._signal_price_coverage(result, ["A", "NEW"], set())

    assert excluded == ["NEW"]
    assert blocking == []
    assert coverage_date == "2026-08-25"


def test_unpriced_broker_holding_still_blocks_signal(monkeypatch):
    prices = _long([("A", "2026-08-25", 101)])
    monkeypatch.setattr(db, "load_nse_holidays", lambda: frozenset())
    result = db._assess_ohlcv_freshness(prices, "2026-08-25", ["A", "NEW"], "parquet")

    excluded, blocking, _ = live._signal_price_coverage(result, ["A", "NEW"], {"NEW"})

    assert excluded == []
    assert blocking == ["NEW"]


def test_live_signal_reaches_engine_with_unpriced_nonholding(monkeypatch):
    _pin_live_snapshot(monkeypatch, ["A", "NEW"])
    prices = _long([("A", "2026-08-25", 101)])
    partial = db._assess_ohlcv_freshness(prices, "2026-08-25", ["A", "NEW"], "parquet")
    monkeypatch.setattr(db, "_load_constituents", lambda: {"Nifty 50": ["A", "NEW"]})
    monkeypatch.setattr(db, "load_compositions", lambda snapshot=None: pd.DataFrame())
    monkeypatch.setattr(db, "sync_benchmark_data", lambda: None)
    monkeypatch.setattr(db, "load_ohlcv_for_backtest", lambda **kwargs: partial)
    monkeypatch.setattr(
        db,
        "load_benchmark_series",
        lambda **kwargs: db.BenchmarkLoadResult({}, "2026-08-25", "2026-08-25", "fresh"),
    )
    monkeypatch.setattr("corporate_actions.load_corporate_actions", lambda: [])
    monkeypatch.setattr("backtest_engine.BacktestConfig", lambda **kwargs: kwargs)
    monkeypatch.setattr("backtest_engine.run_backtest", lambda *args: {"error": "engine reached"})

    params = {
        "indices": ["Nifty 50"],
        "signal_date": date(2026, 8, 25),
        "band": "narrow",
        "m": 5,
        "n": 10,
        "sort_method": "1 year",
        "max_pos": 0,
        "s2_drop": False,
        "s2_threshold": 2,
    }
    assert live._run_signal(params)["error"] == "engine reached"


def test_live_signal_blocks_before_backtest_when_refresh_is_not_fresh(monkeypatch):
    _pin_live_snapshot(monkeypatch, ["A"])
    baseline = _long([("A", "2026-08-18", 100)])
    stale = db._assess_ohlcv_freshness(baseline, "2026-08-25", ["A"], "parquet")

    monkeypatch.setattr(db, "_load_constituents", lambda: {"Nifty 50": ["A"]})
    monkeypatch.setattr(db, "load_compositions", lambda snapshot=None: pd.DataFrame())
    monkeypatch.setattr(db, "sync_benchmark_data", lambda: None)
    monkeypatch.setattr(db, "load_ohlcv_for_backtest", lambda **kwargs: stale)
    monkeypatch.setattr(
        db,
        "load_benchmark_series",
        lambda **kwargs: db.BenchmarkLoadResult({}, "2026-08-25", "2026-08-24", "partial", ["Nifty 50"]),
    )

    def should_not_run(*args, **kwargs):
        raise AssertionError("run_backtest must not execute on stale required data")

    monkeypatch.setattr("backtest_engine.run_backtest", should_not_run)

    result = live._run_signal({"indices": ["Nifty 50"], "signal_date": date(2026, 8, 25)})

    assert "error" in result
    assert "Signal not generated" in result["error"]
    assert result["data_freshness"]["actual_latest_date"] == "2026-08-18"


def test_error_does_not_use_newest_individual_price_as_verified_coverage(monkeypatch):
    _pin_live_snapshot(monkeypatch, ["A", "MISSING"])
    baseline = _long([("A", "2026-08-25", 100)])
    partial = db._assess_ohlcv_freshness(baseline, "2026-08-25", ["A", "MISSING"], "parquet")
    monkeypatch.setattr(db, "_load_constituents", lambda: {"Nifty 50": ["A", "MISSING"]})
    monkeypatch.setattr(db, "load_compositions", lambda snapshot=None: pd.DataFrame())
    monkeypatch.setattr(db, "sync_benchmark_data", lambda: None)
    monkeypatch.setattr(db, "load_ohlcv_for_backtest", lambda **kwargs: partial)
    monkeypatch.setattr(
        db,
        "load_benchmark_series",
        lambda **kwargs: db.BenchmarkLoadResult({}, "2026-08-25", None, "partial", ["Nifty 50"]),
    )
    result = live._run_signal(
        {
            "indices": ["Nifty 50"],
            "signal_date": date(2026, 8, 25),
            "broker_snapshot": pd.DataFrame({"Ticker": ["MISSING"], "Quantity": [1]}),
        }
    )
    assert "verified universe coverage: not fully covered" in result["error"]


def _pin_live_snapshot(monkeypatch, symbols):
    membership = pd.DataFrame(dict(INDEX_NAME="NIFTY50", TIME_STAMP=pd.Timestamp("2026-01-01"), SYMBOL=symbols))
    snapshot = md.MarketSnapshot(
        pd.DataFrame(), membership, "price-test", "membership-test", pd.Timestamp("2026-08-25")
    )
    monkeypatch.setattr(md, "load_snapshot", lambda *args, **kw: snapshot)


def test_stale_nonholding_is_excluded_consistently_without_blocking_fresh_candidates(monkeypatch):
    prices = _long([("A", "2026-08-25", 101), ("STALE", "2026-08-01", 200)])
    monkeypatch.setattr(live, "load_nse_holidays", lambda: frozenset())
    result = db._assess_ohlcv_freshness(prices, "2026-08-25", ["A", "STALE"], "shared")
    excluded, blocking, coverage = live._signal_price_coverage(result, ["A", "STALE"], set())
    assert excluded == ["STALE"] and blocking == [] and coverage == "2026-08-25"
