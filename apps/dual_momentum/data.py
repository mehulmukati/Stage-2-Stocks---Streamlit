"""Dual Momentum readers over the shared pricing snapshot.

Actual adjusted ETF quotes, index TRI and a labeled cash proxy are distinct
series in the one price file. Pages never download or maintain private deltas.
TRI is scaled only at the documented ETF overlap when constructing returns.
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import date, datetime, timedelta
from typing import Callable

import numpy as np
import pandas as pd

import market_data as shared_market

from .config import CASH_TICKER, ETF_UNIVERSE, NSE_HOLIDAYS_JSON, REPO_RATE_CSV

logger = logging.getLogger(__name__)

_NOOP_EMIT: Callable[[str, str], None] = lambda _lv, _msg: None

# ---------------------------------------------------------------------------
# Module-level caches
# ---------------------------------------------------------------------------
_lock = threading.Lock()

_baseline_df: pd.DataFrame | None = None  # Tier 1b: TRI parquet in memory
_delta_df: pd.DataFrame | None = None  # Tier 1b: delta parquet in memory
_repo_rate: pd.Series | None = None  # repo rate series (date index)
_hot_cache: dict[str, dict[str, pd.Series]] = {}  # Tier 1: {date_key: {ticker: Series}}
_etf_price_cache: dict[str, pd.Series] = {}  # session-level ETF full-history cache
_loading_events: dict[str, threading.Event] = {}  # prevents duplicate concurrent loads

_IST = None


def _get_ist():
    global _IST
    if _IST is None:
        import pytz

        _IST = pytz.timezone("Asia/Kolkata")
    return _IST


def _today_key() -> str:
    return datetime.now(_get_ist()).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_universe_prices(tickers, emit=_NOOP_EMIT, snapshot=None):
    snapshot = snapshot or shared_market.load_snapshot()
    etf = snapshot.series("etf_adjusted", tickers)
    tri = snapshot.series("index_tri")
    cash = snapshot.series("cash_proxy")
    result = {}
    for ticker in tickers:
        tri_name = ETF_UNIVERSE.get(ticker, {}).get("tri_name")
        actual = etf.get(ticker)
        historical = tri.get(tri_name)
        if ticker == CASH_TICKER and ticker in cash:
            result[ticker] = _ffill_daily(cash[ticker])
        elif actual is not None and historical is not None:
            result[ticker] = _stitch_tri_to_etf(historical, actual)
        elif actual is not None:
            result[ticker] = _ffill_daily(actual)
        elif historical is not None:
            result[ticker] = _ffill_daily(historical)
        else:
            emit("warn", f"{ticker} is missing from shared prices; run the shared refresh.")
        if ticker in result:
            result[ticker].attrs["source_revisions"] = snapshot.revisions
    return result


def load_nse_holidays() -> set[date]:
    """Load NSE holiday dates from the shared JSON file in the repo root."""
    if not os.path.exists(NSE_HOLIDAYS_JSON):
        return set()
    import json

    with open(NSE_HOLIDAYS_JSON) as f:
        raw = json.load(f)
    holidays = set()
    for entry in raw:
        try:
            holidays.add(pd.Timestamp(entry).date())
        except Exception:
            pass
    return holidays


# ---------------------------------------------------------------------------
# TRI loading
# ---------------------------------------------------------------------------


def _load_tri_prices(tickers, emit=_NOOP_EMIT):
    return load_universe_prices(tickers, emit)


def _stitch_tri_to_etf(tri: pd.Series, etf: pd.Series) -> pd.Series:
    """
    Stitch TRI history before ETF launch to ETF prices after.
    On the first overlapping date, compute a scale ratio and apply it to TRI
    so the combined series is return-continuous.
    """
    tri = tri.sort_index()
    etf = etf.sort_index()

    # Align indexes to date only
    tri.index = pd.to_datetime(tri.index).normalize()
    etf.index = pd.to_datetime(etf.index).normalize()

    overlap = tri.index.intersection(etf.index)
    if overlap.empty:
        # No overlap — just concatenate; gap may exist
        combined = pd.concat([tri, etf]).sort_index()
        combined = combined[~combined.index.duplicated(keep="last")]
        return _ffill_daily(combined)

    # Compute scale ratio over up to 5 overlapping days to reduce single-day
    # noise from illiquid ETF bid-ask spreads.
    anchor_dates = overlap[:5]
    ratio = (etf.loc[anchor_dates] / tri.loc[anchor_dates]).mean()
    first_overlap = overlap[0]

    tri_scaled = tri[tri.index < first_overlap] * ratio
    combined = pd.concat([tri_scaled, etf]).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    return _ffill_daily(combined)


def _get_etf_price_series(ticker):
    return shared_market.load_snapshot().series("etf_adjusted", [ticker]).get(ticker)


def _load_liquidcase(emit: Callable[[str, str], None], etf_series=None) -> pd.Series:
    """
    Build a continuous daily NAV series for LIQUIDCASE.NS.

    Strategy:
      1. Load actual ETF prices from yfinance (post-launch, ~2019 onwards).
      2. For dates before first ETF date, backfill using RBI repo rate CSV:
         NAV_t = NAV_{t+1} / (1 + repo_rate_t / 365)
         (working backwards from the first known ETF NAV).
    """
    etf_series = etf_series if etf_series is not None else _get_etf_price_series(CASH_TICKER)
    repo = _load_repo_rate(emit)

    if etf_series is None or etf_series.empty:
        emit("warn", "LiquidCase ETF data unavailable — using repo rate proxy only.")
        return _build_repo_series(repo, end_date=pd.Timestamp.today().normalize())

    etf_series = etf_series.sort_index()
    first_etf_date = etf_series.index[0]
    first_etf_nav = etf_series.iloc[0]

    # Build synthetic history before first_etf_date using repo rate
    if repo is not None and not repo.empty:
        pre_dates = pd.date_range(
            start=repo.index.min(),
            end=first_etf_date - timedelta(days=1),
            freq="D",
        )
        # Get applicable rate for each date (forward-fill repo rate)
        rate_series = repo.reindex(pre_dates, method="ffill").fillna(repo.iloc[0])

        # Work backwards: NAV[t-1] = NAV[t] / (1 + r/365)
        navs = np.ones(len(pre_dates))
        navs[-1] = first_etf_nav / (1 + rate_series.iloc[-1] / 365)
        for i in range(len(pre_dates) - 2, -1, -1):
            navs[i] = navs[i + 1] / (1 + rate_series.iloc[i] / 365)

        synthetic = pd.Series(navs, index=pre_dates)
    else:
        emit("warn", "repo_rate.csv not found — LiquidCase history will start from ETF launch.")
        synthetic = pd.Series(dtype=float)

    combined = pd.concat([synthetic, etf_series]).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    return _ffill_daily(combined)


def _build_repo_series(repo: pd.Series, end_date: pd.Timestamp) -> pd.Series:
    """Compound repo rate from start to end_date with base NAV = 100."""
    if repo is None or repo.empty:
        return pd.Series(dtype=float)
    dates = pd.date_range(repo.index.min(), end_date, freq="D")
    rate_series = repo.reindex(dates, method="ffill").fillna(repo.iloc[0])
    daily_returns = 1 + rate_series / 365
    nav = daily_returns.cumprod() * 100
    return nav


def _load_repo_rate(emit: Callable[[str, str], None]) -> pd.Series | None:
    """
    Load RBI repo rate history from repo_rate.csv.
    Expected columns: Date, Rate (in percent, e.g. 6.5 means 6.5% p.a.)
    """
    global _repo_rate
    if _repo_rate is not None:
        return _repo_rate

    if not os.path.exists(REPO_RATE_CSV):
        emit(
            "warn",
            f"repo_rate.csv not found at {REPO_RATE_CSV}. LiquidCase backfill will use current rate only.",
        )
        return None

    try:
        df = pd.read_csv(REPO_RATE_CSV, parse_dates=["Date"])
        df = df.sort_values("Date").set_index("Date")
        rate_col = [c for c in df.columns if "rate" in c.lower() or "repo" in c.lower()]
        if not rate_col:
            rate_col = [df.columns[0]]
        series = df[rate_col[0]].dropna() / 100  # convert percent to decimal
        series = series[~series.index.duplicated(keep="last")]
        _repo_rate = series
        return series
    except Exception as e:
        emit("warn", f"Failed to load repo_rate.csv: {e}")
        return None


# ---------------------------------------------------------------------------
# Pure yfinance tickers (Gold, Silver, MON100)
# ---------------------------------------------------------------------------


def _load_yf_prices(tickers, emit=_NOOP_EMIT):
    return load_universe_prices(tickers, emit)


# ---------------------------------------------------------------------------
# Parquet baseline management
# ---------------------------------------------------------------------------


def _ensure_tri_baseline(emit=_NOOP_EMIT):
    return pd.DataFrame(shared_market.load_snapshot().series("index_tri"))


def _ensure_tri_delta(emit=_NOOP_EMIT):
    return None


def _fetch_yf_delta(*args, **kwargs):
    raise RuntimeError("Private downloads are retired; use the shared publisher")


def _save_tri_delta(new_rows):
    raise RuntimeError("Private delta writes are retired; use the shared publisher.")


def _ffill_daily(series: pd.Series) -> pd.Series:
    """Reindex to calendar days and forward-fill gaps (weekends, holidays)."""
    if series.empty:
        return series
    series = series.sort_index()
    all_days = pd.date_range(series.index.min(), series.index.max(), freq="D")
    return series.reindex(all_days).ffill()


def get_common_date_range(tickers: list[str]) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Return the intersection of available history across all selected tickers.
    start = latest of all individual starts (most restrictive)
    end   = earliest of all individual ends  (most restrictive)
    Used to warn the user when a short-history ticker limits the backtest window.
    """
    prices = load_universe_prices(tickers)
    starts, ends = [], []
    for t, s in prices.items():
        s = s.dropna()
        if not s.empty:
            starts.append(s.index.min())
            ends.append(s.index.max())
    if not starts:
        return pd.Timestamp("2010-01-01"), pd.Timestamp.today()
    return max(starts), min(ends)


def ticker_history_start(tickers: list[str]) -> dict[str, pd.Timestamp]:
    """Return the earliest available date for each ticker."""
    prices = load_universe_prices(tickers)
    return {t: s.dropna().index.min() for t, s in prices.items() if not s.dropna().empty}
