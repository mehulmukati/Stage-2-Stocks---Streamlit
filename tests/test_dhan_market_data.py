from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import market_data as md
from scripts import dhan_market_data as dhan
from scripts.refresh_market_data import PublicationRejected, refresh_prices


def test_daily_candles_use_indian_session_date_and_validate_array_lengths():
    # Midnight India is the previous UTC date.
    timestamp = int(datetime(2026, 11, 18, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp())
    payload = dict(timestamp=[timestamp], open=[813.9], high=[816.5], low=[804.2], close=[808.15], volume=[21579720])
    frame = dhan.parse_candles(payload, "HDFCBANK", "equity", "2026-11-18", "2026-11-18")
    assert frame.date.tolist() == [pd.Timestamp("2026-11-18")]
    assert frame.Close.tolist() == [808.15]
    with pytest.raises(ValueError, match="different lengths"):
        dhan.parse_candles(payload | {"volume": []}, "HDFCBANK", "equity", "2026-11-18", "2026-11-18")
    with pytest.raises(ValueError, match="High below"):
        dhan.parse_candles(payload | {"high": [800]}, "HDFCBANK", "equity", "2026-11-18", "2026-11-18")


def test_exact_instrument_matching_excludes_other_exchanges_and_rejects_ambiguity():
    master = pd.DataFrame(
        [
            ["NSE", "E", "EQUITY", "1333", "HDFCBANK", "EQ"],
            ["BSE", "E", "EQUITY", "500180", "HDFCBANK", "A"],
            ["NSE", "I", "INDEX", "13", "NIFTY 50", ""],
        ],
        columns=[
            "SEM_EXM_EXCH_ID",
            "SEM_SEGMENT",
            "SEM_INSTRUMENT_NAME",
            "SEM_SMST_SECURITY_ID",
            "SEM_TRADING_SYMBOL",
            "SEM_SERIES",
        ],
    )
    assert dhan.resolve_instrument(master, "HDFCBANK.NS", "etf_adjusted")["securityId"] == "1333"
    assert dhan.resolve_instrument(master, "Nifty 50", "index_price")["exchangeSegment"] == "IDX_I"
    duplicate = pd.concat([master, master.iloc[[0]].assign(SEM_SMST_SECURITY_ID="999")])
    with pytest.raises(ValueError, match="found 2"):
        dhan.resolve_instrument(duplicate, "HDFCBANK", "equity")


def test_client_excludes_end_date_and_stops_on_expired_auth(monkeypatch):
    monkeypatch.setenv("DHAN_CLIENT_ID", "test-client")
    monkeypatch.setenv("DHAN_ACCESS_TOKEN", "test-token")
    client = dhan.DhanDownloader()
    calls = []

    class Response:
        status_code = 401

        def json(self):
            return {"errorCode": "807", "errorMessage": "test-token"}

    def post(url, **kwargs):
        calls.append(kwargs)
        return Response()

    monkeypatch.setattr(client.session, "post", post)
    with pytest.raises(RuntimeError, match="authentication") as error:
        client._history(
            {"securityId": "1333", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY"},
            "HDFCBANK",
            "equity",
            "2026-11-18",
            "2026-11-18",
        )
    assert len(calls) == 1
    assert calls[0]["json"]["toDate"] == "2026-11-19"
    assert "test-token" not in str(error.value)


@pytest.mark.parametrize("complete", [False, True])
def test_provider_change_requires_full_history_even_when_overlap_prices_agree(market, monkeypatch, complete):
    from apps.dual_momentum import config
    from scripts import refresh_market_data as publisher

    frame, _, days = market
    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    benchmarks = ["Nifty 50", "Nifty 100", "Nifty 500"]
    monkeypatch.setattr(publisher, "INDEX_TICKERS", {name: name for name in benchmarks})
    index_rows = pd.concat(
        [frame.iloc[[-1]].assign(symbol=name, series_type="index_price") for name in benchmarks],
        ignore_index=True,
    )
    frame = pd.concat([frame, index_rows], ignore_index=True)
    md.write_source(frame, md.PRICE_PATH)
    target = str((days[-1] + pd.offsets.BDay()).date())
    before = md.PRICE_PATH.read_bytes()
    calls = []

    class Provider:
        provider = "dhan"
        adjustment_policy = "dhan_daily_corporate_action_adjusted"
        errors = {}

        def __call__(self, requests, start, end):
            calls.append(start)
            new = frame if complete else frame.groupby("symbol").tail(5)
            tail = frame.groupby("symbol").tail(1).assign(date=pd.Timestamp(target))
            return pd.concat([new, tail], ignore_index=True), None

    if not complete:
        with pytest.raises(PublicationRejected):
            refresh_prices(target_date=target, fetch=Provider(), require_coverage=True)
        assert md.PRICE_PATH.read_bytes() == before
    else:
        report = refresh_prices(target_date=target, fetch=Provider(), require_coverage=True)
        assert report["target_session_symbol_count"] == 3
        assert set(report["series_sources"].values()) == {"dhan"}
        assert md.load_snapshot().prices.date.min() == days[0]
    assert pd.Timestamp(calls[0]) <= days[0]


def test_explicit_yahoo_switch_is_also_guarded(market, monkeypatch):
    from apps.dual_momentum import config
    from scripts import refresh_market_data as publisher

    frame, _, days = market
    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    monkeypatch.setattr(publisher, "INDEX_TICKERS", {})
    sources = {f"equity:{symbol}": "dhan" for symbol in frame.symbol.unique()}
    md.write_source(frame, md.PRICE_PATH, {"series_sources": sources})
    before = md.PRICE_PATH.read_bytes()
    with pytest.raises(PublicationRejected):
        refresh_prices(
            target_date=str(days[-1].date()),
            fetch=lambda *args: (frame.groupby("symbol").tail(5), None),
            require_coverage=True,
        )
    assert md.PRICE_PATH.read_bytes() == before
