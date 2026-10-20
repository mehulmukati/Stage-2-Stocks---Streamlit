"""Behavioral contract: all readers agree on dated sources and revisions."""

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

import data
import data_backtest as db
import market_data as md
from backtest_engine import _precompute_all_metrics, rank_universe_at_date
from momentum_ranking import rank_momentum_frame
from scripts.refresh_market_data import (
    check_publication,
    merge_observations,
    refresh_prices,
    revision_needs_full_history,
    validate_publication,
)


def test_screeners_portfolios_charts_share_prices_membership_and_revisions(market, monkeypatch):
    import yfinance as yf

    monkeypatch.setattr(yf, "download", lambda *a, **kw: pytest.fail("A page reader contacted upstream"))
    frame, history, days = market
    screen, date, source = data.resolve_screener_data(for_momentum=True)
    snapshot = md.load_snapshot(days[-1])
    loaded = db.load_ohlcv_for_backtest(snapshot=snapshot)
    quant = db.load_quant_ohlcv_snapshot(snapshot=snapshot)
    assert set(screen.Symbol) == snapshot.symbols() == {"A", "B", "LOWVOL"}
    assert screen.attrs["source_revisions"] == loaded.source_revisions == quant.source_revisions == snapshot.revisions
    assert data._load_constituents(days[-11]) == {"Nifty 50": ["A", "LOWVOL"]}
    assert set(db.load_compositions().SYMBOL) == {"A", "B", "LOWVOL"}
    chart = data._fetch_chart_data_for_target("A", date)
    pd.testing.assert_series_equal(chart.Close, loaded.symbol_data["A"].Close, check_names=False)
    pd.testing.assert_frame_equal(quant.symbol_data["A"], loaded.symbol_data["A"])


def test_identical_eligibility_scores_and_stable_ranks_across_paths(market):
    _, _, days = market
    snapshot = md.load_snapshot(days[-1])
    screen, _, _ = data.resolve_screener_data(for_momentum=True)
    ranked = rank_momentum_frame(screen, "Average of 3/6/9/12 months")
    prices = snapshot.ohlcv()
    scalar, reasons = rank_universe_at_date(
        prices,
        days[-1],
        "Average of 3/6/9/12 months",
        snapshot.symbols(),
        min_history_days=252,
        return_excluded_reasons=True,
    )
    fast = rank_universe_at_date(
        prices,
        days[-1],
        "Average of 3/6/9/12 months",
        snapshot.symbols(),
        min_history_days=252,
        precomputed=_precompute_all_metrics(prices),
    )
    assert scalar == fast == ranked.Symbol.tolist() == ["A", "B"]
    assert reasons["LOWVOL"] == "low_volume"
    assert rank_momentum_frame(screen, "Average of 3/6/9/12 months", minimum_median_volume=0).Symbol.tolist() == [
        "A",
        "B",
        "LOWVOL",
    ]


def test_same_date_price_correction_invalidates_all_reader_caches(market):
    frame, _, days = market
    old, _, _ = data.resolve_screener_data(True)
    old_score = old.set_index("Symbol").loc["A", "Sharpe_3M"]
    before = md.source_revisions()
    frame.loc[(frame.symbol == "A") & (frame.date == days[-1]), ["Open", "High", "Low", "Close"]] *= 1.1
    md.write_source(frame, md.PRICE_PATH, expected_revision=before[0])
    fresh, _, source = data.resolve_screener_data(True)
    assert fresh.attrs["source_revisions"] != before
    assert fresh.set_index("Symbol").loc["A", "Sharpe_3M"] != old_score
    chart = data.fetch_chart_data("A")
    assert chart.Close.iloc[-1] == frame[(frame.symbol == "A") & (frame.date == days[-1])].Close.iloc[0]
    assert db.load_quant_ohlcv_snapshot().source_revisions == fresh.attrs["source_revisions"]


def test_membership_revision_changes_same_date_universe(market):
    _, history, days = market
    before, _, _ = data.resolve_screener_data(True)
    revised = md.append_membership_snapshot(
        history, {"Nifty 50": ["B"]}, days[-1], datetime.now(timezone.utc).isoformat(), "test official notice"
    )
    md.write_source(revised, md.MEMBERSHIP_PATH)
    after, _, _ = data.resolve_screener_data(True)
    assert set(before.Symbol) == {"A", "B", "LOWVOL"}
    assert after.Symbol.tolist() == ["B"]
    assert md.load_snapshot(days[-11]).symbols() == {"A", "LOWVOL"}


def test_publish_rejects_invalid_observation_and_conflicts_without_losing_history(market):
    frame, _, _ = market
    revision = md.source_revisions()[0]
    bad = frame.copy()
    bad.loc[0, "Close"] = -1
    with pytest.raises(ValueError, match="positive"):
        md.write_source(bad, md.PRICE_PATH, expected_revision=revision)
    assert md.source_revisions()[0] == revision
    with pytest.raises(RuntimeError, match="Concurrent"):
        md.write_source(frame, md.PRICE_PATH, expected_revision="obsolete")
    duplicate = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        md.write_source(duplicate, md.PRICE_PATH)


def test_missing_authoritative_file_does_not_fall_back_to_legacy_store(market):
    md.PRICE_PATH.unlink()
    with pytest.raises(FileNotFoundError):
        md.load_snapshot()


def test_adjusted_price_revision_requires_full_symbol_history(market):
    frame, _, days = market
    new = frame[(frame.symbol == "A") & (frame.date >= days[-5])].copy()
    new[["Open", "High", "Low", "Close"]] *= 0.5
    assert revision_needs_full_history(frame, new) == {("equity", "A")}
    merged = merge_observations(frame, new)
    assert len(merged) == len(frame)
    assert merged.date.min() == frame.date.min()


def test_partial_shared_refresh_retains_failed_symbols_and_history(market, monkeypatch):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())

    def fetch(requests, start, end):
        if "A.NS" not in requests:
            return pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"
        new = frame[(frame.symbol == "A") & (frame.date == days[-1])].copy()
        new.date = pd.Timestamp(target)
        return new, None

    result = refresh_prices(target_date=target, fetch=fetch)
    accepted = md.load_snapshot()
    assert accepted.prices.date.min() == frame.date.min()
    assert accepted.ohlcv()["B"].index.max() == days[-1]
    assert accepted.ohlcv()["A"].index.max() == pd.Timestamp(target)
    assert result["missing_target_symbols"] == ["B", "LOWVOL"]
    assert accepted.prices.attrs["errors"]


def test_failed_shared_refresh_does_not_publish(market, monkeypatch):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    before = md.source_revisions()
    with pytest.raises(RuntimeError, match="prior files retained"):
        refresh_prices(fetch=lambda *a: (pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"))
    assert md.source_revisions() == before


def test_publisher_lock_rejects_overlapping_writer(market):
    with md.publisher_lock():
        with pytest.raises(RuntimeError, match="Another publisher"):
            with md.publisher_lock(timeout=0):
                pytest.fail("A concurrent publisher acquired the same lock")


def test_short_history_matches_when_volume_gate_is_disabled(market):
    frame, _, days = market
    short = frame[frame.date >= days[-100]].copy()
    md.write_source(short, md.PRICE_PATH)
    screen, _, _ = data.resolve_screener_data(True)
    snapshot = md.load_snapshot(days[-1])
    settings = dict(min_history_days=63, minimum_median_volume=0)
    ranked = rank_momentum_frame(screen, "3 months", **settings).Symbol.tolist()
    scalar = rank_universe_at_date(snapshot.ohlcv(), days[-1], "3 months", snapshot.symbols(), **settings)
    fast = rank_universe_at_date(
        snapshot.ohlcv(),
        days[-1],
        "3 months",
        snapshot.symbols(),
        precomputed=_precompute_all_metrics(snapshot.ohlcv()),
        **settings
    )
    assert scalar == fast == ranked == ["A", "B", "LOWVOL"]


def test_incomplete_adjusted_history_cannot_replace_existing_prices(market, monkeypatch):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())
    changed = frame[(frame.symbol == "A") & (frame.date >= days[-5])].copy()
    changed[["Open", "High", "Low", "Close"]] *= 0.5
    b = frame[(frame.symbol == "B") & (frame.date == days[-1])].copy()
    b.date = pd.Timestamp(target)

    def fetch(requests, start, end):
        if set(requests) == {"A.NS"}:
            return changed, None  # Correction response is only a tail, not full history.
        if "A.NS" in requests:
            return pd.concat([changed, b], ignore_index=True), None
        return pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"

    report = refresh_prices(target_date=target, fetch=fetch)
    actual = md.load_snapshot().ohlcv()["A"]
    expected = frame[frame.symbol == "A"].set_index("date").Close
    pd.testing.assert_series_equal(actual.Close, expected, check_names=False)
    assert "incomplete" in report["errors"]["A"]


def test_complete_adjustment_rebuild_replaces_history_without_seam(market, monkeypatch):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    a = frame[frame.symbol == "A"].copy()
    a[["Open", "High", "Low", "Close"]] *= 0.5

    def fetch(requests, start, end):
        if set(requests) == {"A.NS"}:
            return a, None
        if "A.NS" in requests:
            return a.tail(5), None
        return pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"

    refresh_prices(target_date=str(days[-1].date()), fetch=fetch)
    actual = md.load_snapshot().ohlcv()["A"].Close
    pd.testing.assert_series_equal(actual, a.set_index("date").Close, check_names=False)


def test_upstream_adapter_retries_old_session_and_keeps_both_observations(monkeypatch):
    from scripts import refresh_market_data as publisher

    calls = []

    def download(*args, **kwargs):
        calls.append(args)
        day = "2026-10-05" if len(calls) == 1 else "2026-10-06"
        return pd.DataFrame(
            dict(Open=[100.0], High=[101.0], Low=[99.0], Close=[100.0], Volume=[1000.0]),
            index=pd.to_datetime([day]),
        )

    monkeypatch.setattr(publisher.yf, "download", download)
    monkeypatch.setattr(publisher.time, "sleep", lambda *a: None)
    result, error = publisher.download({"A.NS": ("A", "equity")}, "2026-10-01", "2026-10-06")
    assert error is None and len(calls) == 2
    assert set(result.date) == set(pd.to_datetime(["2026-10-05", "2026-10-06"]))


@pytest.mark.parametrize("missing_volume", [0.0, 1000.0, np.nan])
def test_rebuild_omitting_placeholder_repairs_prices_but_preserves_required_history(
    market, monkeypatch, missing_volume
):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    omitted = (frame.symbol == "A") & (frame.date == days[10])
    frame.loc[omitted, "Volume"] = missing_volume
    md.write_source(frame, md.PRICE_PATH, {"history_rebuild_required": ["A"]})
    replacement = frame[(frame.symbol == "A") & ~omitted].copy()
    replacement[["Open", "High", "Low", "Close"]] *= 0.5
    b = frame[(frame.symbol == "B") & (frame.date == days[-1])].copy()

    def fetch(requests, start, end):
        if "A.NS" in requests:
            return pd.concat([replacement, b], ignore_index=True), None
        return pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"

    report = refresh_prices(target_date=str(days[-1].date()), fetch=fetch)
    actual = md.load_snapshot().ohlcv()["A"].Close
    expected = frame[frame.symbol == "A"].set_index("date").Close.copy()
    if missing_volume == 0:
        expected.loc[replacement.date] = replacement.Close.to_numpy()
        assert "A" not in report["history_rebuild_required"]
    else:
        assert "A" in report["history_rebuild_required"]
        assert "incomplete" in report["errors"]["A"]
    pd.testing.assert_series_equal(actual, expected, check_names=False)


def test_partial_download_records_lag_instead_of_silently_reporting_success(market, monkeypatch):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())

    def fetch(requests, start, end):
        if "A.NS" not in requests:
            return pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"
        return frame[frame.symbol == "A"].tail(5), "target session unavailable"

    report = refresh_prices(target_date=target, fetch=fetch)
    assert report["publication_status"] == "partial"
    assert report["target_session_symbol_count"] == 0
    assert report["max_price_date"] == str(days[-1].date())
    assert target in report["errors"]["A.NS"]
    assert "latest returned" in report["errors"]["A.NS"]


@pytest.mark.parametrize("neighbor_factor", [1.0, 0.5])
def test_omitted_special_session_is_retained_only_with_unchanged_surrounding_prices(
    market, monkeypatch, neighbor_factor
):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    special = frame[(frame.symbol == "A") & (frame.date == pd.Timestamp("2025-01-31"))].copy()
    special.date = pd.Timestamp("2025-02-01")
    previous = pd.concat([frame, special], ignore_index=True)
    md.write_source(previous, md.PRICE_PATH, {"history_rebuild_required": ["A"]})
    replacement = frame[frame.symbol == "A"].copy()
    replacement[["Open", "High", "Low", "Close"]] *= neighbor_factor
    replacement.loc[replacement.date.ge(days[-5]), ["Open", "High", "Low", "Close"]] *= 0.5
    b = frame[(frame.symbol == "B") & (frame.date == days[-1])].copy()

    def fetch(requests, start, end):
        if "A.NS" in requests:
            return pd.concat([replacement, b], ignore_index=True), None
        return pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"

    report = refresh_prices(target_date=str(days[-1].date()), fetch=fetch)
    actual = md.load_snapshot().ohlcv()["A"].Close
    expected = previous[previous.symbol == "A"].set_index("date").Close.sort_index()
    if neighbor_factor == 1:
        expected.loc[replacement.date] = replacement.Close.to_numpy()
        assert "A" not in report["history_rebuild_required"]
    else:
        assert "A" in report["history_rebuild_required"]
        assert "incomplete" in report["errors"]["A"]
    pd.testing.assert_series_equal(actual, expected, check_names=False)


def test_publication_check_fails_on_universe_lag_without_upstream_requests_or_writes(market, monkeypatch):
    import yfinance as yf

    monkeypatch.setattr(yf, "download", lambda *a, **kw: pytest.fail("Coverage check contacted upstream"))
    _, _, days = market
    revisions = md.source_revisions()
    with pytest.raises(RuntimeError, match="no current constituents"):
        check_publication(str((days[-1] + pd.offsets.BDay()).date()))
    assert md.source_revisions() == revisions


def test_publication_check_requires_broad_coverage_and_fresh_benchmarks(market):
    frame, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())
    quotes = []
    for name in ["A", "B", "LOWVOL", "Nifty 50", "Nifty 100", "Nifty 500"]:
        row = frame.iloc[[-1]].copy().assign(symbol=name, date=pd.Timestamp(target))
        row.series_type = "equity" if name in {"A", "B", "LOWVOL"} else "index_price"
        quotes.append(row)
    md.write_source(merge_observations(frame, pd.concat(quotes, ignore_index=True)), md.PRICE_PATH)
    coverage = check_publication(target)
    assert coverage["target_session_symbol_count"] == 3
    assert coverage["missing_target_symbols"] == []
    assert coverage["missing_target_benchmarks"] == []
    md.write_source(md.load_snapshot().prices.query("symbol != 'Nifty 100'"), md.PRICE_PATH)
    with pytest.raises(RuntimeError, match="Nifty 100"):
        check_publication(target)


@pytest.mark.parametrize("fresh_count", [0, 1])
def test_guarded_refresh_rejects_market_wide_lag_before_writing(market, monkeypatch, fresh_count):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())
    before = md.source_revisions()
    before_bytes = md.PRICE_PATH.read_bytes()

    def fetch(requests, start, end):
        if "A.NS" not in requests:
            return pd.DataFrame(columns=md.PRICE_COLUMNS), "offline"
        rows = frame[frame.symbol.eq("A")].tail(5).copy()
        if fresh_count:
            rows.loc[rows.index[-1], "date"] = pd.Timestamp(target)
        return rows, None

    with pytest.raises(RuntimeError, match="Shared publication is incomplete"):
        refresh_prices(target_date=target, fetch=fetch, require_coverage=True)
    assert md.source_revisions() == before
    assert md.PRICE_PATH.read_bytes() == before_bytes


def test_publication_accepts_isolated_gaps_but_rejects_widespread_lag():
    coverage = dict(current_symbol_count=100, target_session_symbol_count=96, missing_target_benchmarks=[])
    validate_publication(coverage, "2026-10-19")
    coverage["target_session_symbol_count"] = 94
    with pytest.raises(RuntimeError, match="95% required"):
        validate_publication(coverage, "2026-10-19")


def test_quant_reports_missing_candles_without_inventing_values(market):
    frame, _, days = market
    frame.loc[(frame.symbol == "A") & (frame.date == days[-5]), "Open"] = np.nan
    md.write_source(frame, md.PRICE_PATH)
    quant = db.load_quant_ohlcv_snapshot()
    assert quant.incomplete_candle_rows == {"A": 1}
    assert days[-5] not in quant.symbol_data["A"].index
    assert pd.isna(md.load_snapshot().ohlcv()["A"].loc[days[-5], "Open"])


def test_composite_score_uses_identical_numeric_contract_for_row_types():
    from momentum_engine import _calculate_avg_sharpe

    row = dict(Sharpe_3M=4.773, Sharpe_6M=4.402, Sharpe_9M=3.836, Sharpe_1Y=2.762)
    array_row = {key: np.float64(value) for key, value in row.items()}
    method = "Average of 3/6/9/12 months"
    assert _calculate_avg_sharpe(row, method) == _calculate_avg_sharpe(pd.Series(array_row), method)


def test_historical_extra_indices_do_not_expand_default_app_universe(market):
    frame, history, days = market
    extra = frame[frame.symbol == "A"].copy()
    extra.symbol = "EXTRA"
    md.write_source(pd.concat([frame, extra], ignore_index=True), md.PRICE_PATH)
    additional = pd.DataFrame(dict(INDEX_NAME=["NIFTY 500"], TIME_STAMP=[days[0]], SYMBOL=["EXTRA"]))
    md.write_source(pd.concat([history, additional], ignore_index=True), md.MEMBERSHIP_PATH)
    snapshot = md.load_snapshot(days[-1])
    assert snapshot.symbols() == {"A", "B", "LOWVOL"}
    assert snapshot.symbols(["Nifty 500"]) == {"EXTRA"}
    screen, _, _ = data.resolve_screener_data(True)
    assert set(screen.Symbol) == {"A", "B", "LOWVOL"}
    assert set(screen.Index) == {"Nifty 50"}
