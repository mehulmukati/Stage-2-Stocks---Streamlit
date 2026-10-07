"""Charts are revision-aware readers of the shared price store."""

import pandas as pd
import pytest

import data
import market_data as md


def test_chart_date_is_sliced_without_upstream_fetch(market, monkeypatch):
    import yfinance as yf

    monkeypatch.setattr(yf, "download", lambda *a, **kw: pytest.fail("chart contacted upstream"))
    _, _, days = market
    result = data._fetch_chart_data_for_target("A", str(days[-5].date()))
    assert result.index.max() == days[-5]
    assert len(result) == len(days) - 4


def test_missing_symbol_is_reported_without_private_download(market, monkeypatch):
    import yfinance as yf

    monkeypatch.setattr(yf, "download", lambda *a, **kw: pytest.fail("missing symbol triggered private fetch"))
    assert data._fetch_chart_data_for_target("MISSING", "2026-08-25").empty


def test_chart_cache_key_includes_date_and_both_revisions(market, monkeypatch):
    captured = {}

    def cached(symbol, target_date, source_revisions):
        captured.update(symbol=symbol, date=target_date, revisions=source_revisions)
        return pd.DataFrame()

    monkeypatch.setattr(data, "_fetch_chart_data_cached", cached)
    data.fetch_chart_data("A")
    assert captured == dict(symbol="A", date=data._get_target_key(), revisions=md.source_revisions())
