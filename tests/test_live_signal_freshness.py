from datetime import datetime

import pandas as pd

import app_live_signal as live
import data_backtest as db


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


def _reset_runtime_caches(monkeypatch, baseline: pd.DataFrame, target: str = "2026-08-25") -> None:
    monkeypatch.setattr(db, "_get_target_key", lambda: target)
    monkeypatch.setattr(db, "_ensure_baseline_ohlcv", lambda emit: baseline.copy())
    monkeypatch.setattr(db, "_save_ohlcv_delta", lambda data, emit: None)
    monkeypatch.setattr(db, "load_nse_holidays", lambda: frozenset())
    db._merged_ohlcv.clear()
    db._ohlcv_refresh_latches.clear()
    db._ohlcv_refresh_results.clear()


def test_failed_refresh_is_not_cached_and_next_call_retries(monkeypatch):
    baseline = _long([("A", "2026-08-24", 100), ("B", "2026-08-24", 200)])
    _reset_runtime_caches(monkeypatch, baseline)
    calls = []

    def fake_fetch(symbols, last_date, target, emit, max_attempts=3):
        calls.append(list(symbols))
        if len(calls) == 1:
            return db.DeltaFetchResult(
                pd.DataFrame(columns=baseline.columns), list(symbols), attempts=3, error="offline"
            )
        delta = _long([("A", target, 101), ("B", target, 201)])
        return db.DeltaFetchResult(delta, list(symbols), ["A", "B"], attempts=1)

    monkeypatch.setattr(db, "_fetch_ohlcv_delta", fake_fetch)

    first = db.load_ohlcv_for_backtest(required_symbols=["A", "B"])
    assert not first.is_fresh
    assert first.refresh_status == "failed"
    assert first.actual_latest_date == "2026-08-24"
    assert "2026-08-25" not in db._merged_ohlcv

    second = db.load_ohlcv_for_backtest(required_symbols=["A", "B"])
    assert second.is_fresh
    assert second.actual_latest_date == "2026-08-25"
    assert len(calls) == 2


def test_partial_response_reports_conservative_actual_date_and_is_not_cached(monkeypatch):
    baseline = _long([("A", "2026-08-24", 100), ("B", "2026-08-24", 200)])
    _reset_runtime_caches(monkeypatch, baseline)
    delta = _long([("A", "2026-08-25", 101)])
    monkeypatch.setattr(
        db,
        "_fetch_ohlcv_delta",
        lambda symbols, last_date, target, emit, max_attempts=3: db.DeltaFetchResult(
            delta, list(symbols), ["A"], attempts=3, error="B missing"
        ),
    )

    result = db.load_ohlcv_for_backtest(required_symbols=["A", "B"])

    assert result.refresh_status == "partial"
    assert result.max_price_date == "2026-08-25"
    assert result.actual_latest_date == "2026-08-24"
    assert result.missing_target_symbols == ["B"]
    assert not result.is_fresh
    assert "2026-08-25" not in db._merged_ohlcv


def test_one_session_partial_coverage_is_usable_for_signal(monkeypatch):
    partial = _long([("A", "2026-08-25", 101), ("B", "2026-08-24", 200)])
    monkeypatch.setattr(db, "load_nse_holidays", lambda: frozenset())

    result = db._assess_ohlcv_freshness(partial, "2026-08-25", ["A", "B"], "parquet+delta")

    assert not result.is_fresh
    assert result.missing_target_symbols == ["B"]
    assert result.stale_symbols == []
    assert result.is_usable_for_signal


def test_fresh_result_uses_observed_date_and_legacy_unpacking(monkeypatch):
    baseline = _long([("A", "2026-08-24", 100)])
    _reset_runtime_caches(monkeypatch, baseline)
    delta = _long([("A", "2026-08-25", 101)])
    monkeypatch.setattr(
        db,
        "_fetch_ohlcv_delta",
        lambda symbols, last_date, target, emit, max_attempts=3: db.DeltaFetchResult(
            delta, list(symbols), ["A"], attempts=1
        ),
    )

    result = db.load_ohlcv_for_backtest(required_symbols=["A"])
    symbol_data, observed_date, source = result

    assert result.is_fresh
    assert observed_date == "2026-08-25"
    assert source == "parquet+delta"
    assert symbol_data["A"].index.max() == pd.Timestamp("2026-08-25")


def test_delta_fetch_retries_exception_then_succeeds(monkeypatch):
    calls = 0
    idx = pd.DatetimeIndex(["2026-08-25"])
    success = pd.concat(
        {"A.NS": pd.DataFrame({"Close": [101.0], "High": [102.0], "Volume": [1000]}, index=idx)},
        axis=1,
    )

    def fake_download(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary outage")
        return success

    monkeypatch.setattr(db.yf, "download", fake_download)
    monkeypatch.setattr(db.time, "sleep", lambda seconds: None)

    result = db._fetch_ohlcv_delta(["A"], pd.Timestamp("2026-08-24"), "2026-08-25", lambda level, message: None)

    assert calls == 2
    assert result.attempts == 2
    assert result.returned_symbols == ["A"]
    assert result.error is None
    assert result.data.iloc[0]["date"] == pd.Timestamp("2026-08-25")


def test_delta_fetch_retries_when_first_response_has_only_an_older_session(monkeypatch):
    calls = 0

    def fake_download(*args, **kwargs):
        nonlocal calls
        calls += 1
        day = "2026-08-24" if calls == 1 else "2026-08-25"
        frame = pd.DataFrame(
            {"Close": [101.0], "High": [102.0], "Volume": [1000]},
            index=pd.DatetimeIndex([day]),
        )
        return pd.concat({"A.NS": frame}, axis=1)

    monkeypatch.setattr(db.yf, "download", fake_download)
    monkeypatch.setattr(db.time, "sleep", lambda seconds: None)

    result = db._fetch_ohlcv_delta(["A"], pd.Timestamp("2026-08-23"), "2026-08-25", lambda level, message: None)

    assert calls == 2
    assert result.attempts == 2
    assert result.returned_symbols == ["A"]
    assert result.error is None
    assert set(result.data["date"]) == {pd.Timestamp("2026-08-24"), pd.Timestamp("2026-08-25")}


def test_delta_fetch_does_not_count_older_rows_as_updated(monkeypatch):
    frame = pd.DataFrame(
        {"Close": [101.0], "High": [102.0], "Volume": [1000]},
        index=pd.DatetimeIndex(["2026-08-24"]),
    )
    response = pd.concat({"A.NS": frame}, axis=1)
    monkeypatch.setattr(db.yf, "download", lambda *args, **kwargs: response)
    monkeypatch.setattr(db.time, "sleep", lambda seconds: None)

    result = db._fetch_ohlcv_delta(["A"], pd.Timestamp("2026-08-23"), "2026-08-25", lambda level, message: None)

    assert result.attempts == 3
    assert result.returned_symbols == []
    assert "A" in result.error


def test_empty_yahoo_response_is_a_failed_fetch(monkeypatch):
    monkeypatch.setattr(db.yf, "download", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(db.time, "sleep", lambda seconds: None)

    result = db._fetch_ohlcv_delta(["A"], pd.Timestamp("2026-08-24"), "2026-08-25", lambda level, message: None)

    assert result.data.empty
    assert result.attempts == 3
    assert "empty response" in result.error


def test_live_signal_blocks_before_backtest_when_refresh_is_not_fresh(monkeypatch):
    baseline = _long([("A", "2026-08-18", 100)])
    stale = db._assess_ohlcv_freshness(baseline, "2026-08-25", ["A"], "parquet")

    monkeypatch.setattr(db, "_load_constituents", lambda: {"Nifty 50": ["A"]})
    monkeypatch.setattr(db, "load_compositions", lambda: pd.DataFrame())
    monkeypatch.setattr(db, "sync_benchmark_data", lambda: None)
    monkeypatch.setattr(db, "load_ohlcv_for_backtest", lambda **kwargs: stale)
    monkeypatch.setattr(
        db,
        "load_benchmark_series",
        lambda with_status=False: db.BenchmarkLoadResult({}, "2026-08-25", "2026-08-24", "partial", ["Nifty 50"]),
    )

    def should_not_run(*args, **kwargs):
        raise AssertionError("run_backtest must not execute on stale required data")

    monkeypatch.setattr("backtest_engine.run_backtest", should_not_run)

    result = live._run_signal({"indices": ["Nifty 50"]})

    assert "error" in result
    assert "Signal not generated" in result["error"]
    assert result["data_freshness"]["actual_latest_date"] == "2026-08-18"


def test_failed_benchmark_refresh_is_not_hot_cached_and_retries(monkeypatch):
    base = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-08-24"]),
            "Nifty 50": [25000.0],
            "Nifty 500": [22000.0],
        }
    )
    monkeypatch.setattr(db, "_get_target_key", lambda: "2026-08-25")
    monkeypatch.setattr(db, "_ensure_baseline_bench", lambda emit: base.copy())
    monkeypatch.setattr(db, "_save_bench_delta", lambda data: None)
    db._merged_bench.clear()
    calls = 0

    def fake_fetch(last_date, target, emit):
        nonlocal calls
        calls += 1
        if calls == 1:
            return pd.DataFrame()
        return pd.DataFrame({"date": pd.to_datetime([target]), "Nifty 50": [25100.0], "Nifty 500": [22100.0]})

    monkeypatch.setattr(db, "_fetch_bench_delta", fake_fetch)

    first = db.load_benchmark_series(with_status=True)
    assert "2026-08-25" not in db._merged_bench
    second = db.load_benchmark_series(with_status=True)

    assert first.status == "partial"
    assert second.actual_latest_date == "2026-08-25"
    assert calls == 2
