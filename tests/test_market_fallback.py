import pandas as pd
import pytest

import market_data as md
from scripts import recover_market_data as recovery
from scripts import refresh_market_data as publisher


class Provider:
    def __init__(self, frame, name="dhan", error=None):
        self.frame = frame
        self.provider = name
        self.adjustment_policy = name + "_adjusted"
        self.error = error
        self.calls = []

    def __call__(self, requests, start, target):
        self.calls.append((dict(requests), start, target))
        if self.error:
            raise RuntimeError(self.error)
        identities = {(kind, symbol) for symbol, kind in requests.values()}
        selected = pd.MultiIndex.from_frame(self.frame[["series_type", "symbol"]]).isin(identities)
        selected &= self.frame.date.between(pd.Timestamp(start), pd.Timestamp(target))
        return self.frame[selected].copy(), None


def add_quotes(frame, target, symbols, factor=1.0):
    tail = frame[frame.symbol.isin(symbols)].groupby(["series_type", "symbol"]).tail(1).copy()
    tail.date = pd.Timestamp(target)
    result = pd.concat([frame, tail], ignore_index=True)
    result.loc[result.symbol.isin(symbols), ["Open", "High", "Low", "Close"]] *= factor
    return result


@pytest.fixture
def fallback_market(market, monkeypatch):
    from apps.dual_momentum import config

    frame, membership, days = market
    indices = ["Nifty 50", "Nifty 100", "Nifty 500"]
    monkeypatch.setattr(publisher, "INDEX_TICKERS", {name: name for name in indices})
    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    benchmarks = pd.concat(
        [frame.iloc[[-1]].assign(symbol=name, series_type="index_price") for name in indices], ignore_index=True
    )
    frame = pd.concat([frame, benchmarks], ignore_index=True)
    sources = {f"{kind}:{symbol}": "dhan" for kind, symbol in frame[["series_type", "symbol"]].drop_duplicates().values}
    md.write_source(frame, md.PRICE_PATH, {"series_sources": sources})
    target = str((days[-1] + pd.offsets.BDay()).date())
    return frame, target, indices


def test_fallback_fills_only_unresolved_series_and_preserves_primary_quotes(fallback_market):
    frame, target, indices = fallback_market
    primary = Provider(add_quotes(frame[frame.symbol.ne("LOWVOL")], target, ["A"] + indices))
    fallback = Provider(add_quotes(frame, target, ["B", "LOWVOL"], factor=0.5), "yahoo")
    report = publisher.refresh_prices(target, fetch=primary, fallback_fetch=fallback, require_coverage=True)
    assert report["target_session_symbol_count"] == 3
    assert report["fallback_requested_series"] == ["B.NS", "LOWVOL.NS"]
    assert report["fallback_accepted_series"] == ["B.NS", "LOWVOL.NS"]
    assert report["missing_target_series"] == []
    assert report["series_sources"]["equity:A"] == "dhan"
    assert report["series_sources"]["equity:B"] == "yahoo"
    assert report["provider_diagnostics"]["dhan"]
    assert not report["errors"]
    accepted = md.load_snapshot().ohlcv()
    assert accepted["A"].Close.iloc[-1] == frame[frame.symbol.eq("A")].Close.iloc[-1]
    assert accepted["B"].Close.iloc[0] == frame[frame.symbol.eq("B")].Close.iloc[0] * 0.5


def test_incomplete_fallback_cannot_create_adjustment_seam(fallback_market):
    frame, target, indices = fallback_market
    primary = Provider(add_quotes(frame, target, ["A"] + indices))
    fallback = Provider(add_quotes(frame, target, ["B", "LOWVOL"], 0.5).groupby("symbol").tail(3), "yahoo")
    report = publisher.refresh_prices(target, fetch=primary, fallback_fetch=fallback)
    assert report["fallback_accepted_series"] == []
    assert report["target_session_symbol_count"] == 1
    assert report["series_sources"]["equity:B"] == "dhan"
    actual = md.load_snapshot().ohlcv()["B"].Close
    expected = frame[frame.symbol.eq("B")].set_index("date").Close
    pd.testing.assert_series_equal(actual, expected, check_names=False)


@pytest.mark.parametrize("failure_at", ["initialization", "download"])
def test_provider_outage_still_attempts_yahoo(fallback_market, monkeypatch, failure_at):
    from scripts import dhan_market_data

    frame, target, indices = fallback_market
    primary = Provider(frame, error="Dhan is unavailable")
    fallback = Provider(add_quotes(frame, target, ["A", "B", "LOWVOL"] + indices), "yahoo")
    if failure_at == "initialization":

        def unavailable():
            raise RuntimeError("Dhan credentials missing")

        monkeypatch.setattr(dhan_market_data, "DhanDownloader", unavailable)
        primary = None
    report = publisher.refresh_prices(target, fetch=primary, fallback_fetch=fallback, require_coverage=True)
    assert report["primary_provider"] == "dhan"
    assert report["target_session_symbol_count"] == 3
    assert len(report["fallback_accepted_series"]) == 6
    assert set(report["series_sources"].values()) == {"yahoo"}
    assert report["provider_diagnostics"]["dhan"]


def test_stale_fallback_does_not_replace_primary_history(fallback_market):
    frame, target, indices = fallback_market
    primary = Provider(add_quotes(frame, target, ["A"] + indices))
    stale = frame.copy()
    stale[["Open", "High", "Low", "Close"]] *= 0.5
    report = publisher.refresh_prices(target, fetch=primary, fallback_fetch=Provider(stale, "yahoo"))
    assert report["fallback_accepted_series"] == []
    assert report["series_sources"]["equity:B"] == "dhan"
    actual = md.load_snapshot().ohlcv()["B"].Close
    pd.testing.assert_series_equal(actual, frame[frame.symbol.eq("B")].set_index("date").Close, check_names=False)


def test_bad_series_does_not_discard_other_primary_quotes(fallback_market):
    frame, target, indices = fallback_market
    primary_frame = add_quotes(frame, target, ["A", "B"] + indices)
    primary_frame.loc[primary_frame.symbol.eq("A"), "Close"] = -1
    fallback = Provider(add_quotes(frame, target, ["A", "LOWVOL"]), "yahoo")
    report = publisher.refresh_prices(
        target, fetch=Provider(primary_frame), fallback_fetch=fallback, require_coverage=True
    )
    assert report["fallback_requested_series"] == ["A.NS", "LOWVOL.NS"]
    assert report["series_sources"]["equity:B"] == "dhan"
    assert "Invalid" in report["provider_diagnostics"]["dhan"]["A.NS"]


def test_fallback_failure_preserves_successful_primary_updates(fallback_market):
    frame, target, indices = fallback_market
    primary = Provider(add_quotes(frame, target, ["A"] + indices))
    fallback = Provider(frame, "yahoo", error="Yahoo unavailable")
    report = publisher.refresh_prices(target, fetch=primary, fallback_fetch=fallback)
    assert md.load_snapshot().ohlcv()["A"].index.max() == pd.Timestamp(target)
    assert report["target_session_symbol_count"] == 1
    assert report["fallback_accepted_series"] == []
    assert len(fallback.calls) == 1


def test_dhan_is_retried_after_a_previous_yahoo_fallback(fallback_market):
    frame, target, indices = fallback_market
    publisher.refresh_prices(
        target,
        fetch=Provider(add_quotes(frame, target, ["A"] + indices)),
        fallback_fetch=Provider(add_quotes(frame, target, ["B", "LOWVOL"], 0.5), "yahoo"),
        require_coverage=True,
    )
    next_day = str((pd.Timestamp(target) + pd.offsets.BDay()).date())
    primary_frame = add_quotes(frame, target, ["A", "B", "LOWVOL"] + indices)
    primary_frame = add_quotes(primary_frame, next_day, ["A", "B", "LOWVOL"] + indices)
    fallback = Provider(frame, "yahoo", error="Must not fetch when primary is complete")
    report = publisher.refresh_prices(
        next_day, fetch=Provider(primary_frame), fallback_fetch=fallback, require_coverage=True
    )
    assert not fallback.calls
    assert set(report["series_sources"].values()) == {"dhan"}
    assert md.load_snapshot().prices.date.min() == frame.date.min()


def test_fallback_includes_missing_etfs_and_benchmarks(fallback_market, monkeypatch):
    from apps.dual_momentum import config

    frame, target, indices = fallback_market
    monkeypatch.setattr(config, "ETF_UNIVERSE", {"ETF.NS": "test ETF"})
    etf = frame[frame.symbol.eq("A")].assign(symbol="ETF.NS", series_type="etf_adjusted")
    frame = pd.concat([frame, etf], ignore_index=True)
    sources = {f"{kind}:{symbol}": "dhan" for kind, symbol in frame[["series_type", "symbol"]].drop_duplicates().values}
    md.write_source(frame, md.PRICE_PATH, {"series_sources": sources})
    primary_frame = add_quotes(frame, target, ["A", "B", "LOWVOL", "Nifty 50", "Nifty 500"])
    fallback_frame = add_quotes(frame, target, ["ETF.NS", "Nifty 100"])
    report = publisher.refresh_prices(
        target,
        fetch=Provider(primary_frame),
        fallback_fetch=Provider(fallback_frame, "yahoo"),
        require_coverage=True,
    )
    assert report["fallback_requested_series"] == ["ETF.NS", "Nifty 100"]
    assert report["fallback_accepted_series"] == ["ETF.NS", "Nifty 100"]
    assert not report["missing_target_benchmarks"]


@pytest.mark.parametrize("exhausted", [False, True])
def test_recovery_retries_partial_coverage_even_above_acceptance_threshold(fallback_market, monkeypatch, exhausted):
    frame, target, indices = fallback_market
    # Nineteen of twenty stocks already cover the target session.
    stocks = []
    for number in range(20):
        stock = frame[frame.symbol.eq("A")].assign(symbol=f"S{number:02}")
        if number < 19:
            stock = add_quotes(stock, target, [f"S{number:02}"])
        stocks.append(stock)
    stocks = pd.concat(stocks, ignore_index=True)
    indices_frame = add_quotes(frame[frame.series_type.eq("index_price")], target, indices)
    combined = pd.concat([stocks, indices_frame], ignore_index=True)
    md.write_source(combined, md.PRICE_PATH)
    md.write_source(
        pd.DataFrame(dict(INDEX_NAME="Nifty 50", TIME_STAMP=frame.date.min(), SYMBOL=stocks.symbol.unique())),
        md.MEMBERSHIP_PATH,
    )
    monkeypatch.setattr("scripts.refresh_constituents.refresh", lambda: None)
    provider = Provider(add_quotes(combined, target, ["S19"]), "yahoo")
    monkeypatch.setattr(recovery, "refresh_prices", lambda **kwargs: publisher.refresh_prices(fetch=provider, **kwargs))
    report = recovery.run_attempt(dict(target_date=target, attempt_allowed=not exhausted, final_attempt=exhausted))
    if exhausted:
        assert report["status"] == "already_current"
        assert report["recovery_window_exhausted"] is True
        assert not provider.calls
    else:
        assert report["status"] == "recovered"
        assert report["target_session_symbol_count"] == 20
        assert provider.calls
