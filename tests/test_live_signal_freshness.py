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


def test_snapshot_mode_never_calls_live_delta_fetch(monkeypatch):
    baseline = _long([("A", "2026-08-24", 100), ("B", "2026-08-23", 200)])
    _reset_runtime_caches(monkeypatch, baseline)

    def unexpected_fetch(*args, **kwargs):
        raise AssertionError("snapshot mode must not contact Yahoo")

    monkeypatch.setattr(db, "_fetch_ohlcv_delta", unexpected_fetch)
    result = db.load_ohlcv_for_backtest(required_symbols=["A", "B"], refresh=False)

    assert result.refresh_status == "snapshot"
    assert set(result.symbol_data) == {"A", "B"}
    assert result.max_price_date == "2026-08-24"


def test_quant_snapshot_requires_and_returns_full_ohlcv(monkeypatch, tmp_path):
    path = tmp_path / "quant.parquet"
    pd.DataFrame(
        {
            "symbol": ["A"],
            "date": [pd.Timestamp("2026-08-24")],
            "Open": [99.0],
            "High": [102.0],
            "Low": [98.0],
            "Close": [100.0],
            "Volume": [1_000],
        }
    ).to_parquet(path, index=False)
    monkeypatch.setattr(db, "OHLCV_PARQUET", str(tmp_path / "missing-long-history.parquet"))
    monkeypatch.setattr(db, "SCREENER_OHLCV_PARQUET", str(path))
    monkeypatch.setattr(db, "_get_target_key", lambda: "2026-08-25")
    monkeypatch.setattr(db, "_quant_snapshot_cache", None)

    result = db.load_quant_ohlcv_snapshot()

    assert result.refresh_status == "snapshot"
    assert result.actual_latest_date == "2026-08-24"
    assert set(result.symbol_data["A"].columns) == {"Open", "High", "Low", "Close", "Volume"}


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
            "Nifty 100": [24000.0],
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
        return pd.DataFrame(
            {
                "date": pd.to_datetime([target]),
                "Nifty 50": [25100.0],
                "Nifty 100": [24100.0],
                "Nifty 500": [22100.0],
            }
        )

    monkeypatch.setattr(db, "_fetch_bench_delta", fake_fetch)

    first = db.load_benchmark_series(with_status=True)
    assert "2026-08-25" not in db._merged_bench
    second = db.load_benchmark_series(with_status=True)

    assert first.status == "partial"
    assert second.actual_latest_date == "2026-08-25"
    assert calls == 2


def test_refresh_only_requests_missing_real_prices(monkeypatch):
    baseline = _long([("A", "2026-08-25", 101), ("B", "2026-08-24", 200), ("DUMMYHEG", "2026-08-24", 1)])
    _reset_runtime_caches(monkeypatch, baseline)
    calls = []

    def fetch(symbols, last_date, target, emit):
        calls.append(symbols)
        return db.DeltaFetchResult(_long([("B", target, 201)]), symbols, ["B"], 1)

    monkeypatch.setattr(db, "_fetch_ohlcv_delta", fetch)
    result = db.load_ohlcv_for_backtest(required_symbols=["A", "B"])
    assert calls == [["B"]]
    assert result.is_fresh


def test_partial_success_is_retained_for_next_refresh(monkeypatch):
    baseline = _long([("A", "2026-08-24", 100), ("B", "2026-08-24", 200)])
    _reset_runtime_caches(monkeypatch, baseline)
    monkeypatch.setattr(db, "_baseline_ohlcv", baseline)
    monkeypatch.setattr(db, "_ensure_baseline_ohlcv", lambda emit: db._baseline_ohlcv)
    calls = []

    def fetch(symbols, last_date, target, emit):
        calls.append(symbols)
        symbol = "A" if len(calls) == 1 else "B"
        return db.DeltaFetchResult(_long([(symbol, target, 201)]), symbols, [symbol], 1)

    monkeypatch.setattr(db, "_fetch_ohlcv_delta", fetch)
    first = db.load_ohlcv_for_backtest(required_symbols=["A", "B"])
    second = db.load_ohlcv_for_backtest(required_symbols=["A", "B"])
    assert first.missing_target_symbols == ["B"]
    assert calls == [["A", "B"], ["B"]]
    assert second.is_fresh


def test_benchmark_retries_nan_target_close(monkeypatch):
    monkeypatch.setattr(db, "BENCHMARK_TICKERS", {"Nifty 50": "^NSEI"})
    monkeypatch.setattr(db.time, "sleep", lambda seconds: None)
    calls = []

    def download(*args, **kwargs):
        calls.append(args)
        return pd.DataFrame(
            {"Close": [25000.0, float("nan") if len(calls) == 1 else 25100.0]},
            index=pd.to_datetime(["2026-08-24", "2026-08-25"]),
        )

    monkeypatch.setattr(db.yf, "download", download)
    result = db._fetch_bench_delta(pd.Timestamp("2026-08-23"), "2026-08-25", lambda *args: None)
    assert len(calls) == 2
    assert result.iloc[-1]["Nifty 50"] == 25100.0


def test_error_does_not_use_newest_individual_price_as_verified_coverage(monkeypatch):
    baseline = _long([("A", "2026-08-25", 100)])
    partial = db._assess_ohlcv_freshness(baseline, "2026-08-25", ["A", "MISSING"], "parquet")
    monkeypatch.setattr(db, "_load_constituents", lambda: {"Nifty 50": ["A", "MISSING"]})
    monkeypatch.setattr(db, "load_compositions", lambda: pd.DataFrame())
    monkeypatch.setattr(db, "sync_benchmark_data", lambda: None)
    monkeypatch.setattr(db, "load_ohlcv_for_backtest", lambda **kwargs: partial)
    monkeypatch.setattr(
        db,
        "load_benchmark_series",
        lambda **kwargs: db.BenchmarkLoadResult({}, "2026-08-25", None, "partial", ["Nifty 50"]),
    )
    result = live._run_signal({"indices": ["Nifty 50"]})
    assert "verified universe coverage: not fully covered" in result["error"]


def test_benchmark_partial_save_preserves_other_valid_closes(monkeypatch, tmp_path):
    path = tmp_path / "bench_delta.parquet"
    monkeypatch.setattr(db, "BENCH_DELTA_PARQUET", str(path))
    pd.DataFrame({"date": pd.to_datetime(["2026-08-25"]), "Nifty 50": [100.0], "Nifty 100": [float("nan")]}).to_parquet(
        path, index=False
    )
    db._save_bench_delta(
        pd.DataFrame({"date": pd.to_datetime(["2026-08-25"]), "Nifty 50": [float("nan")], "Nifty 100": [200.0]})
    )
    result = pd.read_parquet(path)
    assert result.iloc[0]["Nifty 50"] == 100.0
    assert result.iloc[0]["Nifty 100"] == 200.0
