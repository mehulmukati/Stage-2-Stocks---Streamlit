import pandas as pd

import data


def _baseline(last_date: str = "2026-08-21") -> pd.DataFrame:
    dates = pd.bdate_range(end=last_date, periods=3)
    return pd.DataFrame(
        {
            "symbol": "RELIANCE",
            "date": dates,
            "Open": [100.0, 101.0, 102.0],
            "High": [102.0, 103.0, 104.0],
            "Low": [99.0, 100.0, 101.0],
            "Close": [101.0, 102.0, 103.0],
            "Volume": [1_000_000, 1_100_000, 1_200_000],
        }
    )


def _download() -> pd.DataFrame:
    dates = pd.to_datetime(["2026-08-21", "2026-08-24", "2026-08-25"])
    return pd.DataFrame(
        {
            "Open": [102.5, 103.0, 104.0],
            "High": [104.5, 105.0, 106.0],
            "Low": [101.5, 102.0, 103.0],
            "Close": [103.5, 104.0, 105.0],
            "Volume": [1_250_000, 1_300_000, 1_400_000],
        },
        index=dates,
    )


def test_fresh_chart_baseline_does_not_download(monkeypatch):
    monkeypatch.setattr(data, "_load_screener_baseline", lambda: _baseline("2026-08-25"))
    calls = []
    monkeypatch.setattr(data.yf, "download", lambda *args, **kwargs: calls.append((args, kwargs)))

    result = data._fetch_chart_data_for_target("RELIANCE", "2026-08-25")
    assert result.index[-1] == pd.Timestamp("2026-08-25")
    assert calls == []


def test_stale_chart_baseline_fetches_and_merges_missing_tail(monkeypatch):
    monkeypatch.setattr(data, "_load_screener_baseline", lambda: _baseline())
    calls = []

    def fake_download(*args, **kwargs):
        calls.append((args, kwargs))
        return _download()

    monkeypatch.setattr(data.yf, "download", fake_download)
    result = data._fetch_chart_data_for_target("RELIANCE", "2026-08-25")

    assert result.index[-1] == pd.Timestamp("2026-08-25")
    assert result.index.is_unique
    assert result.loc["2026-08-21", "Close"] == 103.5
    assert calls[0][1]["end"] == "2026-08-26"
    assert "start" in calls[0][1]


def test_failed_chart_delta_fetch_falls_back_to_stored_history(monkeypatch):
    monkeypatch.setattr(data, "_load_screener_baseline", lambda: _baseline())

    def failed_download(*args, **kwargs):
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(data.yf, "download", failed_download)
    result = data._fetch_chart_data_for_target("RELIANCE", "2026-08-25")
    assert result.index[-1] == pd.Timestamp("2026-08-21")
    assert len(result) == 3


def test_missing_symbol_downloads_full_chart_history(monkeypatch):
    monkeypatch.setattr(data, "_load_screener_baseline", pd.DataFrame)
    calls = []

    def fake_download(*args, **kwargs):
        calls.append((args, kwargs))
        return _download()

    monkeypatch.setattr(data.yf, "download", fake_download)
    result = data._fetch_chart_data_for_target("NEWIPO", "2026-08-25")
    assert result.index[-1] == pd.Timestamp("2026-08-25")
    assert calls[0][1]["period"] == "2y"


def test_chart_cache_key_includes_target_trading_date(monkeypatch):
    captured = {}
    monkeypatch.setattr(data, "_get_target_key", lambda: "2026-08-25")

    def fake_cached(symbol, target_date):
        captured.update(symbol=symbol, target_date=target_date)
        return pd.DataFrame()

    monkeypatch.setattr(data, "_fetch_chart_data_cached", fake_cached)
    data.fetch_chart_data("RELIANCE")
    assert captured == {"symbol": "RELIANCE", "target_date": "2026-08-25"}
