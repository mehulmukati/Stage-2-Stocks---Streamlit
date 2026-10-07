"""Compatibility adapters over the two shared market sources; no runtime downloads."""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd

import market_data as shared_market
from config import IST, SCREENER_OHLCV_PARQUET
from data import _load_constituents, get_last_valid_trading_date, load_nse_holidays  # noqa: F401

BACKTEST_DATA_VERSION = 5
_NOOP_EMIT: Callable[[str, str], None] = lambda _lv, _msg: None


def _get_target_key(now: datetime | None = None) -> str:
    """Latest completed NSE session using the established 19:00 IST data cutoff."""
    now = now or datetime.now(IST)
    market_data_ready = (now.hour, now.minute) >= (19, 0)
    start = now.strftime("%Y-%m-%d") if market_data_ready else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return get_last_valid_trading_date(start, load_nse_holidays())


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
OHLCV_PARQUET = SCREENER_OHLCV_PARQUET
BENCH_PARQUET = os.path.join(REPO_ROOT, "data", "benchmarks.parquet")

# Gitignored local delta caches — accumulate yfinance tail rows across restarts.
DELTA_PARQUET = os.path.join(REPO_ROOT, "data", "backtest_delta.parquet")
BENCH_DELTA_PARQUET = os.path.join(REPO_ROOT, "data", "benchmarks_delta.parquet")

BENCHMARK_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty 100": "^CNX100",
    "Nifty 500": "^CRSLDX",
}

# ──────────────────────────────────────────────
# Module-level caches (thread-safe via _lock)
# ──────────────────────────────────────────────
_lock = threading.RLock()

# Tier 1b: long-form DataFrame materialized from parquet on first access.
_baseline_ohlcv: pd.DataFrame | None = None
_baseline_bench: pd.DataFrame | None = None

# Tier 1: merged (baseline + yfinance delta) cache, keyed by trading-day string.
_merged_ohlcv: dict[str, dict[str, pd.DataFrame]] = {}  # {today_key: {symbol: df}}
_merged_bench: dict[str, dict[str, pd.Series]] = {}  # {today_key: {label: series}}

# Single-flight refreshes. Failed refreshes are deliberately not put in
# ``_merged_ohlcv`` so a later button click can retry without a process restart.
_ohlcv_refresh_latches: dict[str, threading.Event] = {}
_ohlcv_refresh_results: dict[str, "OHLCVLoadResult"] = {}
_quant_snapshot_cache: tuple[str, float, "OHLCVLoadResult"] | None = None

_QUANT_PRICE_COLUMNS = ("symbol", "date", "Open", "High", "Low", "Close", "Volume")


@dataclass
class DeltaFetchResult:
    data: pd.DataFrame
    requested_symbols: list[str]
    returned_symbols: list[str] = field(default_factory=list)
    attempts: int = 0
    error: str | None = None


@dataclass
class OHLCVLoadResult:
    """OHLCV data plus an explicit, observable freshness contract.

    Iteration preserves the legacy ``symbol_data, date, source = ...`` API.  The
    legacy date is the observed coverage date, never the requested cache key.
    """

    symbol_data: dict[str, pd.DataFrame]
    target_date: str
    actual_latest_date: str | None
    max_price_date: str | None
    source: str
    refresh_status: str
    refresh_error: str | None = None
    requested_symbols: list[str] = field(default_factory=list)
    updated_symbols: list[str] = field(default_factory=list)
    missing_target_symbols: list[str] = field(default_factory=list)
    stale_symbols: list[str] = field(default_factory=list)
    attempts: int = 0
    earliest_price_date: str | None = None
    source_revisions: tuple[str, str] | None = None

    @property
    def is_fresh(self) -> bool:
        return (
            bool(self.symbol_data)
            and self.actual_latest_date == self.target_date
            and not self.missing_target_symbols
            and not self.stale_symbols
            and self.refresh_status in {"fresh", "not_needed", "memory"}
        )

    @property
    def is_usable_for_signal(self) -> bool:
        """Whether every required symbol has a recent tradable bar.

        Exact target-session coverage is ideal but not required: suspended or
        thinly traded constituents can legitimately have no bar on one session,
        and the ranking engine already enforces the same three-session limit.
        """
        return bool(self.symbol_data) and self.actual_latest_date is not None and not self.stale_symbols

    def __iter__(self):
        yield self.symbol_data
        yield self.actual_latest_date or self.target_date
        yield self.source


@dataclass
class BenchmarkLoadResult:
    series: dict[str, pd.Series]
    target_date: str
    actual_latest_date: str | None
    status: str
    missing: list[str] = field(default_factory=list)


# ──────────────────────────────────────────────
# Compositions (backtest-specific parquet; constituents shared via data.py)
# ──────────────────────────────────────────────
def load_compositions(snapshot=None) -> pd.DataFrame:
    return (snapshot or shared_market.load_snapshot()).membership


def load_quant_ohlcv_snapshot(emit=_NOOP_EMIT, snapshot=None) -> OHLCVLoadResult:
    loaded = load_ohlcv_for_backtest(emit=emit, refresh=False, snapshot=snapshot)
    incomplete = {}
    for symbol, frame in list(loaded.symbol_data.items()):
        valid = frame[["Open", "High", "Low", "Close", "Volume"]].notna().all(axis=1)
        if not valid.all():
            incomplete[symbol] = int((~valid).sum())
            cleaned = frame.loc[valid].copy()
            if cleaned.empty:
                loaded.symbol_data.pop(symbol)
            else:
                loaded.symbol_data[symbol] = cleaned
    loaded.incomplete_candle_rows = incomplete
    if incomplete:
        emit(
            "warning",
            f"Excluded {sum(incomplete.values())} incomplete historical candles across "
            f"{len(incomplete)} symbols; shared closes are preserved.",
        )
    starts = [frame.index.min() for frame in loaded.symbol_data.values() if not frame.empty]
    loaded.earliest_price_date = str(min(starts).date()) if starts else None
    return loaded


def _ensure_baseline_ohlcv(emit=_NOOP_EMIT) -> pd.DataFrame:
    snapshot = shared_market.load_snapshot()
    return snapshot.prices[snapshot.prices.series_type == "equity"].drop(columns="series_type")


def _ensure_baseline_bench(emit=_NOOP_EMIT) -> pd.DataFrame:
    series = shared_market.load_snapshot().series("index_price", list(BENCHMARK_TICKERS))
    frame = pd.DataFrame(series)
    frame.index.name = "date"
    return frame.reset_index()


def _save_ohlcv_delta(new_df, emit=_NOOP_EMIT):
    raise RuntimeError("Page-level delta writes are retired. Use the shared publisher.")


def _save_bench_delta(new_df):
    raise RuntimeError("Page-level benchmark writes are retired. Use the shared publisher.")


def _fetch_ohlcv_delta(*args, **kwargs):
    raise RuntimeError("Private downloads are retired; use the shared publisher")


def _fetch_bench_delta(*args, **kwargs):
    raise RuntimeError("Private downloads are retired; use the shared publisher")


# ──────────────────────────────────────────────
# Public API (matches data.py surface)
# ──────────────────────────────────────────────
def _long_to_symbol_dict(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Convert long-form price rows into per-symbol frames without discarding extra columns."""
    result: dict[str, pd.DataFrame] = {}
    for sym, grp in df.groupby("symbol", sort=False):
        sub = grp.drop(columns="symbol").copy()
        sub = sub.set_index("date").sort_index()
        result[sym] = sub
    return result


def _active_symbols(base: pd.DataFrame, global_max: pd.Timestamp) -> list[str]:
    """Symbols trading recently enough to merit a runtime tail refresh."""
    # Yahoo can carry a delisted symbol forward with a positive Close but
    # zero Volume. Those placeholders must not make it appear active or drag
    # the entire universe's download start back by years.
    traded = base[
        base["Close"].notna()
        & (pd.to_numeric(base["Close"], errors="coerce").fillna(0) > 0)
        & (pd.to_numeric(base["Volume"], errors="coerce").fillna(0) > 0)
    ]
    if traded.empty:
        return []
    maxima = traded.groupby("symbol", sort=False)["date"].max()
    cutoff = min(global_max, maxima.max()) - timedelta(days=14)
    return sorted(maxima[maxima >= cutoff].index.astype(str).tolist())


def _assess_ohlcv_freshness(
    merged: pd.DataFrame,
    target_key: str,
    required_symbols: list[str],
    source: str,
    fetch: DeltaFetchResult | None = None,
) -> OHLCVLoadResult:
    valid = merged[merged["Close"].notna()].copy()
    if "Volume" in valid.columns:
        valid = valid[pd.to_numeric(valid["Volume"], errors="coerce").fillna(0) > 0]
    max_price_date = valid["date"].max() if not valid.empty else None
    maxima = valid.groupby("symbol")["date"].max() if not valid.empty else pd.Series(dtype="datetime64[ns]")
    target = pd.Timestamp(target_key)
    required = sorted(set(required_symbols))
    missing_target = sorted(sym for sym in required if sym not in maxima.index or maxima[sym] < target)

    # A symbol more than three completed NSE sessions behind is explicitly stale.
    holidays = set(load_nse_holidays())
    cutoff = target
    sessions = 0
    while sessions < 3:
        cutoff -= timedelta(days=1)
        if cutoff.weekday() < 5 and cutoff.strftime("%Y-%m-%d") not in holidays:
            sessions += 1
    stale = sorted(sym for sym in required if sym not in maxima.index or maxima[sym] < cutoff)

    if required:
        observed = [maxima[sym] for sym in required if sym in maxima.index]
        actual = min(observed) if len(observed) == len(required) else None
    else:
        actual = max_price_date

    fetch_error = fetch.error if fetch else None
    if not required and max_price_date is None:
        status = "failed"
    elif missing_target:
        status = "failed" if fetch is not None and fetch.data.empty else "partial"
    elif fetch is None:
        status = "not_needed"
    else:
        status = "fresh"

    actual_key = actual.strftime("%Y-%m-%d") if actual is not None else None
    max_key = max_price_date.strftime("%Y-%m-%d") if max_price_date is not None else None
    return OHLCVLoadResult(
        symbol_data=_long_to_symbol_dict(merged),
        target_date=target_key,
        actual_latest_date=actual_key,
        max_price_date=max_key,
        source=source,
        refresh_status=status,
        refresh_error=fetch_error,
        requested_symbols=required,
        updated_symbols=sorted(set(fetch.returned_symbols) & set(required)) if fetch else [],
        missing_target_symbols=missing_target,
        stale_symbols=stale,
        attempts=fetch.attempts if fetch else 0,
    )


def load_ohlcv_for_backtest(emit=_NOOP_EMIT, required_symbols=None, refresh=False, snapshot=None) -> OHLCVLoadResult:
    # refresh is retained for callers but readers never perform upstream requests.
    try:
        snapshot = snapshot or shared_market.load_snapshot()
        target_key = str(snapshot.as_of.date()) if snapshot.as_of is not None else _get_target_key()
        base = snapshot.prices[snapshot.prices.series_type == "equity"].drop(columns="series_type")
        base = base[base.date <= pd.Timestamp(target_key)]
        required = sorted(set(required_symbols if required_symbols is not None else snapshot.symbols(as_of=target_key)))
        result = _assess_ohlcv_freshness(base, target_key, required, "shared")
        result.source_revisions = snapshot.revisions
        result.earliest_price_date = str(base.date.min().date()) if not base.empty else None
        if result.is_fresh:
            result.refresh_status = "fresh"
        else:
            emit(
                "warning",
                "Shared prices are incomplete for the target session. Run the shared refresh workflow.",
            )
        return result
    except (OSError, ValueError, RuntimeError) as exc:
        return OHLCVLoadResult({}, _get_target_key(), None, None, "error", "failed", str(exc))


def load_benchmark_series(with_status=False, refresh=False, snapshot=None):
    snapshot = snapshot or shared_market.load_snapshot()
    target_key = str(snapshot.as_of.date()) if snapshot.as_of is not None else _get_target_key()
    result = snapshot.series("index_price", list(BENCHMARK_TICKERS))
    missing = sorted(
        label
        for label in BENCHMARK_TICKERS
        if label not in result or result[label].index.max() < pd.Timestamp(target_key)
    )
    actual = (
        min((s.index.max() for s in result.values()), default=None) if len(result) == len(BENCHMARK_TICKERS) else None
    )
    loaded = BenchmarkLoadResult(
        result,
        target_key,
        str(actual.date()) if actual is not None else None,
        "fresh" if not missing else "partial",
        missing,
    )
    loaded.source_revisions = snapshot.revisions
    return loaded if with_status else result


def sync_benchmark_data() -> bool:
    """
    No-op — benchmarks live in the parquet and are refreshed via
    `load_benchmark_series` on first access. Kept for API parity with data.py
    so existing workers can be rewired without signature churn.
    """
    return True
