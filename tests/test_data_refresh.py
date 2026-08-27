import pandas as pd
import pytest

import data
import workers
from scripts.refresh_backtest_parquet import _validate_current_tradability as validate_backtest_tradability
from scripts.refresh_screener_parquet import _validate_current_tradability as validate_screener_tradability


def _refresh_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2026-07-13", periods=6)
    rows = []
    for symbol in ["CURRENT", "STALE"]:
        for position, day in enumerate(dates):
            rows.append(
                {
                    "symbol": symbol,
                    "date": day,
                    "Close": 100.0,
                    "High": 101.0,
                    "Volume": 1_000_000 if symbol == "CURRENT" or position < 2 else 0,
                }
            )
    return pd.DataFrame(rows)


def test_refresh_validators_reject_stale_current_constituent():
    frame = _refresh_frame()

    with pytest.raises(RuntimeError, match="STALE"):
        validate_backtest_tradability(frame, current_symbols={"CURRENT", "STALE"})
    with pytest.raises(RuntimeError, match="STALE"):
        validate_screener_tradability(frame, ["CURRENT", "STALE"])


def test_refresh_validators_accept_recent_positive_volume_bars():
    frame = _refresh_frame()

    validate_backtest_tradability(frame, current_symbols={"CURRENT"})
    validate_screener_tradability(frame, ["CURRENT"])


def test_equity_holiday_loader_uses_capital_market_segment_only():
    data.load_nse_holidays.cache_clear()
    holidays = data.load_nse_holidays()

    # 26 Aug is a holiday for some clearing/settlement segments, but the
    # official NSE Capital Market calendar remains open.
    assert "2026-08-26" not in holidays
    assert "2026-09-14" in holidays
    assert data.get_last_valid_trading_date("2026-08-26", holidays) == "2026-08-26"


def test_screener_worker_is_bound_to_current_data_schema():
    assert data.SCREENER_DATA_VERSION == 2
    assert workers.SCREENER_WORKER_VERSION == 2
    assert workers.resolve_screener_data is data.resolve_screener_data


def _screener_baseline() -> pd.DataFrame:
    rows = []
    for symbol, days in {
        "A": ["2024-01-02", "2026-08-21"],
        "B": ["2024-01-02", "2026-08-21"],
        "REMOVED": ["2024-01-02", "2026-05-04"],
    }.items():
        for day in days:
            rows.append(
                {
                    "symbol": symbol,
                    "date": pd.Timestamp(day),
                    "Open": 99.0,
                    "High": 101.0,
                    "Low": 98.0,
                    "Close": 100.0,
                    "Volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _download_records(symbols: list[str], day: str) -> list[dict]:
    return [
        {
            "symbol": symbol,
            "date": pd.Timestamp(day).date(),
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 1_000_000,
        }
        for symbol in symbols
    ]


def _prepare_runtime_sync(monkeypatch, baseline: pd.DataFrame) -> dict:
    captured = {}
    monkeypatch.setattr(data, "_load_screener_baseline", lambda: baseline.copy())
    monkeypatch.setattr(data, "_write_parquet_atomic", lambda frame, path: captured.update(saved=frame.copy()))
    monkeypatch.setattr(data, "load_nse_holidays", lambda: frozenset())
    monkeypatch.setattr(data, "_screener_baseline", None)
    data._ohlcv_sync_attempted.clear()
    data._sync_latches.clear()
    data._ohlcv_cache.clear()
    return captured


def test_screener_health_ignores_removed_historical_symbols(monkeypatch):
    baseline = _screener_baseline()
    target_rows = pd.DataFrame(_download_records(["A", "B"], "2026-08-26")).rename(
        columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    )
    merged = pd.concat([baseline, target_rows], ignore_index=True)
    monkeypatch.setattr(data, "load_nse_holidays", lambda: frozenset())

    healthy, missing, stale, coverage = data._screener_refresh_health(merged, ["A", "B"], "2026-08-26")

    assert healthy
    assert missing == []
    assert stale == []
    assert coverage == 1.0


def test_incremental_anchor_uses_only_requested_current_symbols(monkeypatch):
    baseline = _screener_baseline()
    _prepare_runtime_sync(monkeypatch, baseline)
    download_kwargs = {}

    def fake_download(*args, **kwargs):
        download_kwargs.update(kwargs)
        return pd.DataFrame({"nonempty": [1]})

    monkeypatch.setattr(data.yf, "download", fake_download)
    monkeypatch.setattr(
        data,
        "_parse_yfinance_download",
        lambda raw, tickers: _download_records(["A", "B"], "2026-08-26"),
    )

    synced = data._sync_ohlcv_to_parquet(["A", "B"], target_date="2026-08-26")

    assert synced
    assert download_kwargs["start"] == "2026-08-11"
    assert download_kwargs["end"] == "2026-08-27"
    assert "2026-08-26" in data._ohlcv_sync_attempted


def test_partial_refresh_is_saved_but_remains_retryable(monkeypatch):
    baseline = _screener_baseline()
    captured = _prepare_runtime_sync(monkeypatch, baseline)
    monkeypatch.setattr(data.yf, "download", lambda *args, **kwargs: pd.DataFrame({"nonempty": [1]}))
    monkeypatch.setattr(data, "_parse_yfinance_download", lambda raw, tickers: _download_records(["A"], "2026-08-26"))

    synced = data._sync_ohlcv_to_parquet(["A", "B"], target_date="2026-08-26")

    assert not synced
    assert "saved" in captured
    assert "2026-08-26" not in data._ohlcv_sync_attempted


def test_legacy_score_cache_without_price_date_schema_is_rejected(tmp_path):
    path = tmp_path / "legacy_score.parquet"
    pd.DataFrame({"Symbol": ["A"], "Close": [101.0], "cache_date": ["2026-08-25"]}).to_parquet(path)

    assert data._load_score_cache(str(path), "2026-08-25") is None
    assert data._load_latest_score_cache(str(path)) == (None, None)


def test_scoring_is_truncated_to_labeled_as_of_date(monkeypatch):
    dates = pd.to_datetime(["2026-08-24", "2026-08-25", "2026-08-26"])
    history = pd.DataFrame(
        {
            "Open": [99.0, 100.0, 110.0],
            "High": [101.0, 102.0, 112.0],
            "Low": [98.0, 99.0, 109.0],
            "Close": [100.0, 101.0, 111.0],
            "Volume": [1_000_000, 1_000_000, 1_000_000],
        },
        index=dates,
    )
    monkeypatch.setattr(data, "_ohlcv_cache", {"A": history})
    monkeypatch.setattr(data, "score_stage2", lambda frame: {"Close": float(frame["Close"].iloc[-1]), "Score": 1})
    monkeypatch.setattr(data, "check_weinstein_retest", lambda frame: False)

    scored = data._load_and_score(
        {"Nifty 50": ["A"]},
        for_momentum=False,
        as_of_date="2026-08-25",
    )

    assert scored.iloc[0]["Close"] == 101.0
    assert scored.iloc[0]["Price Date"] == "2026-08-25"
