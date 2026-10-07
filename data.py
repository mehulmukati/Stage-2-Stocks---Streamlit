import functools
import json
import logging
import os
import re
import tempfile
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
import streamlit as st

CHART_DATA_VERSION = 3
SCREENER_DATA_VERSION = 4

# Default no-op emit used when callers don't need progress reporting.
# Signature: (level: str, message: str) -> None
# Levels: "info" | "warning" | "error" | "success"
_NOOP_EMIT: Callable[[str, str], None] = lambda _lv, _msg: None

import market_data as shared_market
from config import IST
from momentum_engine import precompute_metrics
from stage2_engine import check_weinstein_retest, current_stage2_run, score_stage2


# ──────────────────────────────────────────────
# HOLIDAY & TRADING DAY RESOLVER
# ──────────────────────────────────────────────
@functools.lru_cache(maxsize=None)
def load_nse_holidays() -> frozenset:
    """Load NSE market holidays from nse_holidays.json; returns a frozenset of 'YYYY-MM-DD' strings."""
    path = os.path.join(os.path.dirname(__file__), "nse_holidays.json")
    if not os.path.exists(path):
        return frozenset()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    holidays = set()
    # Screeners and Live Signal use NSE cash equities. Do not union holidays
    # from clearing, debt, currency, or settlement segments: those calendars
    # can close on days when the Capital Market segment still trades.
    segments = [data["CM"]] if isinstance(data, dict) and "CM" in data else data.values()
    for segment in segments:
        for entry in segment:
            date_str = entry.get("tradingDate ", entry.get("tradingDate", "")).strip()
            try:
                dt = datetime.strptime(date_str, "%d-%b-%Y")
                holidays.add(dt.strftime("%Y-%m-%d"))
            except ValueError:
                continue
    return frozenset(holidays)


def get_last_valid_trading_date(start_date_str: str, holidays: frozenset) -> str:
    """Walk backwards from start_date_str to find the nearest weekday that is not an NSE holiday."""
    dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    for _ in range(10):
        if dt.weekday() < 5 and dt.strftime("%Y-%m-%d") not in holidays:
            return dt.strftime("%Y-%m-%d")
        dt -= timedelta(days=1)
    return start_date_str


# ──────────────────────────────────────────────
# DATA FRESHNESS CHECKS
# ──────────────────────────────────────────────
_STALENESS_DAYS_CONSTITUENTS = 30
_STALENESS_DAYS_HOLIDAYS = 180  # NSE publishes holiday lists ~annually; refresh twice/year


def check_data_freshness() -> list[tuple[str, str]]:
    """
    Inspect key reference data files and return (level, message) pairs for anything stale or missing.
    level is 'warning' or 'error'.  Callers should render these as st.warning / st.error.
    """
    issues: list[tuple[str, str]] = []
    today = datetime.now(IST).date()
    repo = os.path.dirname(os.path.abspath(__file__))

    try:
        snapshot = shared_market.load_snapshot()
        verified = snapshot.membership.attrs.get("verified_at")
        if not verified:
            issues.append(("warning", "Constituent verification time is unavailable."))
        else:
            age = (today - pd.Timestamp(verified).tz_convert("Asia/Kolkata").date()).days
            if age > _STALENESS_DAYS_CONSTITUENTS:
                issues.append(("warning", f"Constituents were last verified {age} days ago. Run the shared refresh."))
    except (OSError, ValueError, RuntimeError) as exc:
        issues.append(("error", f"Shared market sources unavailable: {exc}"))

    # ── nse_holidays.json ──────────────────────
    hol_path = os.path.join(repo, "nse_holidays.json")
    if not os.path.exists(hol_path):
        issues.append(
            (
                "warning",
                "**nse_holidays.json** not found — rebalance dates cannot be holiday-adjusted. "
                "Download it from NSE.",
            )
        )
    else:
        hols = load_nse_holidays()
        covered_years = {datetime.strptime(d, "%Y-%m-%d").year for d in hols}
        if today.year not in covered_years:
            issues.append(
                (
                    "error",
                    f"**nse_holidays.json** does not include {today.year} holidays — "
                    "rebalance dates may fall on market holidays. Update the file from NSE.",
                )
            )
        else:
            age = (today - datetime.fromtimestamp(os.path.getmtime(hol_path)).date()).days
            if age > _STALENESS_DAYS_HOLIDAYS:
                issues.append(
                    (
                        "warning",
                        f"**nse_holidays.json** is {age} days old — it may be missing "
                        "holidays added or revised later in the year. Refresh from NSE.",
                    )
                )

    return issues


# ──────────────────────────────────────────────
# CONSTITUENTS
# ──────────────────────────────────────────────
def _load_constituents(as_of_date=None) -> dict:
    return shared_market.load_snapshot(as_of_date or _get_target_key()).constituents()


def _write_parquet_atomic(df: pd.DataFrame, path: str) -> None:
    """Write df to path via a unique temp file + os.replace (atomic on POSIX and Windows)."""
    dir_ = os.path.dirname(path) or "."
    os.makedirs(dir_, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=dir_, suffix=".tmp", delete=False) as f:
        tmp = f.name
    try:
        df.to_parquet(tmp, compression="snappy", index=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# Module-level baseline: long-form {symbol, date, Open, High, Low, Close, Volume}.
# Loaded once per process from screener_ohlcv.parquet; replaced in-place after each sync.
_screener_baseline: pd.DataFrame | None = None


def _load_screener_baseline() -> pd.DataFrame:
    snapshot = shared_market.load_snapshot()
    return snapshot.prices[snapshot.prices.series_type == "equity"].drop(columns="series_type")


def _long_to_symbol_dict(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Convert long-form OHLCV DataFrame to {symbol: DataFrame} with date index."""
    result: dict[str, pd.DataFrame] = {}
    for sym, grp in df.groupby("symbol"):
        sub = grp.drop(columns="symbol").copy()
        sub["date"] = pd.to_datetime(sub["date"])
        sub = sub.set_index("date").sort_index()
        if "Volume" in sub.columns:
            sub["Volume"] = sub["Volume"].astype("Int64")
        result[sym] = sub
    return result


def _load_score_cache(path, target_date):
    return None


def _load_latest_score_cache(path):
    return None, None


def _save_score_cache(path, target_date, df):
    return None


def _records_to_symbol_data(records: list[dict]) -> dict[str, pd.DataFrame]:
    """
    Convert a list of OHLCV record dicts (lowercase keys) to the
    {symbol: DataFrame(Open,High,Low,Close,Volume)} format used by _ohlcv_cache.
    """
    if not records:
        return {}
    buckets: dict[str, list] = defaultdict(list)
    for r in records:
        buckets[r["symbol"]].append(r)
    result: dict[str, pd.DataFrame] = {}
    for sym, rows in buckets.items():
        df = pd.DataFrame(rows).drop(columns="symbol")
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        df = df.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
        )
        df["Volume"] = df["Volume"].astype("Int64")
        result[sym] = df
    return result


def _parse_yfinance_download(raw: pd.DataFrame, tickers: list[str]) -> list[dict]:
    """Parse a yfinance multi-ticker download into a flat list of OHLCV record dicts."""

    def _f(v):
        try:
            f = float(v)
            return None if pd.isna(f) else f
        except (TypeError, ValueError):
            return None

    available = raw.columns.get_level_values(0).unique().tolist() if isinstance(raw.columns, pd.MultiIndex) else tickers
    records = []
    for t in tickers:
        sym = t.replace(".NS", "")
        try:
            if t not in available:
                continue
            sub = raw[t].dropna(how="all") if len(tickers) > 1 else raw.dropna(how="all")
            sub.columns = [c[0] if isinstance(c, tuple) else c for c in sub.columns]
            for dt, row in sub.iterrows():
                if pd.isna(row.get("Close")):
                    continue
                records.append(
                    {
                        "symbol": sym,
                        "date": dt.date(),
                        "open": _f(row.get("Open")),
                        "high": _f(row.get("High")),
                        "low": _f(row.get("Low")),
                        "close": float(row["Close"]),
                        "volume": int(row.get("Volume") or 0),
                    }
                )
        except Exception as exc:
            logging.warning("yfinance parse error for %s: %s", sym, exc)
            continue
    return records


def _screener_refresh_health(
    data: pd.DataFrame,
    symbols: list[str],
    target_date: str,
    min_target_coverage: float = 0.95,
) -> tuple[bool, list[str], list[str], float]:
    """Validate freshness using only the requested current universe."""
    requested = sorted(set(symbols))
    if data.empty or not requested or "date" not in data or "symbol" not in data:
        return False, requested, requested, 0.0

    subset = data[data["symbol"].isin(requested)].copy()
    if subset.empty:
        return False, requested, requested, 0.0
    subset["date"] = pd.to_datetime(subset["date"])
    maxima = subset.groupby("symbol")["date"].max()
    target = pd.Timestamp(target_date)
    missing_target = sorted(sym for sym in requested if sym not in maxima.index or maxima[sym] < target)
    coverage = (len(requested) - len(missing_target)) / len(requested)

    holidays = load_nse_holidays()
    cutoff = target
    for _ in range(3):
        previous_day = (cutoff - timedelta(days=1)).strftime("%Y-%m-%d")
        cutoff = pd.Timestamp(get_last_valid_trading_date(previous_day, holidays))
    stale = sorted(sym for sym in requested if sym not in maxima.index or maxima[sym] < cutoff)
    return coverage >= min_target_coverage and not stale, missing_target, stale, coverage


def _sync_ohlcv_to_parquet(all_symbols, force_download=False, target_date=None, emit=_NOOP_EMIT):
    # Compatibility API: all explicit refreshes go through the shared publisher.
    from scripts.refresh_market_data import refresh_prices

    refresh_prices(target_date=target_date, force_full=force_download, emit=emit)
    return True


def _load_and_score(constituents, for_momentum, emit=_NOOP_EMIT, as_of_date=None, snapshot=None):
    from backtest_engine import latest_tradable_date, trading_session_age

    snapshot = snapshot or shared_market.load_snapshot(as_of_date or _get_target_key())
    target = pd.Timestamp(as_of_date or _get_target_key())
    all_symbols = {s for values in constituents.values() for s in values}
    data = snapshot.ohlcv(all_symbols, as_of=target)
    # Exact NSE calendar used for the same three-session rule as the portfolio engine.
    dates = pd.bdate_range(snapshot.prices.date.min(), target)
    holidays = load_nse_holidays()
    calendar = dates[~dates.strftime("%Y-%m-%d").isin(holidays)]
    results = []
    for symbol, sub in data.items():
        if not for_momentum and len(sub) < 250:
            continue
        if for_momentum:
            metrics = precompute_metrics(sub).iloc[-1].to_dict()
            last = latest_tradable_date(sub, target)
            metrics["Stale Sessions"] = trading_session_age(last, target, calendar) if last is not None else 999
            metrics["Tradable"] = last is not None
            metrics["Price Date"] = str(sub.index[-1].date())
        else:
            metrics = score_stage2(sub)
            if metrics is None:
                continue
            metrics["Price Date"] = str(sub.index[-1].date())
            metrics["Retest"] = check_weinstein_retest(sub)
            metrics.update(current_stage2_run(sub))
        metrics["Symbol"] = symbol
        metrics["Index"] = next((i for i, syms in constituents.items() if symbol in syms), "Unknown")
        results.append(metrics)
    result = pd.DataFrame(results)
    result.attrs["source_revisions"] = snapshot.revisions
    result.attrs["as_of_date"] = str(target.date())
    return result


_MIN_SCORING_ROWS = 252
_OHLCV_WINDOW_DAYS = 550


def get_universe_coverage() -> dict:
    """Return per-index and per-symbol coverage stats against the scoring threshold."""
    constituents = _load_constituents()
    if not constituents:
        return {}

    baseline = _load_screener_baseline()
    cutoff = pd.Timestamp.min
    if not baseline.empty:
        windowed = baseline[pd.to_datetime(baseline["date"]) >= cutoff]
        row_counts: dict[str, int] = windowed.groupby("symbol").size().to_dict()
    else:
        row_counts = {}

    all_symbols = list(dict.fromkeys(s for syms in constituents.values() for s in syms))
    total = len(all_symbols)
    missing_all: list[dict] = []

    by_index: dict[str, dict] = {}
    for index_name, syms in constituents.items():
        missing: list[dict] = []
        for sym in syms:
            rows = row_counts.get(sym, 0)
            if rows < _MIN_SCORING_ROWS:
                missing.append(
                    {
                        "Symbol": sym,
                        "Index": index_name,
                        "Trading Days": rows,
                        "Days Until Eligible": max(0, _MIN_SCORING_ROWS - rows),
                        "Weeks Until Eligible": max(0, -(-(_MIN_SCORING_ROWS - rows) // 5)),  # ceiling div
                    }
                )
        by_index[index_name] = {
            "total": len(syms),
            "scored": len(syms) - len(missing),
            "missing": sorted(missing, key=lambda x: x["Trading Days"]),
        }
        missing_all.extend(missing)

    scored = total - len(missing_all)
    missing_all.sort(key=lambda x: x["Trading Days"])

    return {
        "summary": {"total": total, "scored": scored, "missing_count": len(missing_all)},
        "by_index": by_index,
        "all_missing": missing_all,
    }


# ──────────────────────────────────────────────
# SINGLE-SYMBOL CHART DATA
# ──────────────────────────────────────────────
_VALID_TICKER_RE = re.compile(r"^[A-Z0-9&\-]{1,20}$")


def _normalise_chart_download(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize a single-symbol yfinance response to the chart OHLCV shape."""
    if raw is None or raw.empty:
        return pd.DataFrame()
    clean = raw.copy()
    clean.columns = [column[0] if isinstance(column, tuple) else column for column in clean.columns]
    available = [column for column in ("Open", "High", "Low", "Close", "Volume") if column in clean.columns]
    if not {"High", "Low", "Close"}.issubset(available):
        return pd.DataFrame()
    clean = clean[available]
    clean.index = pd.DatetimeIndex(pd.to_datetime(clean.index, errors="coerce"))
    clean = clean[~clean.index.isna()]
    if clean.index.tz is not None:
        clean.index = clean.index.tz_localize(None)
    return clean.sort_index()


def _fetch_chart_data_for_target(symbol: str, target_date: str) -> pd.DataFrame:
    clean = symbol.strip().upper()
    if not _VALID_TICKER_RE.match(clean):
        return pd.DataFrame()
    return shared_market.load_snapshot(target_date).ohlcv([clean]).get(clean, pd.DataFrame())


@st.cache_data(ttl=3600)
def _fetch_chart_data_cached(symbol: str, target_date: str, source_revisions=None) -> pd.DataFrame:
    return _fetch_chart_data_for_target(symbol, target_date)


def fetch_chart_data(symbol: str) -> pd.DataFrame:
    """Return chart OHLCV through the latest completed NSE session."""
    return _fetch_chart_data_cached(symbol, _get_target_key(), shared_market.source_revisions())


# ──────────────────────────────────────────────
# 3-TIER CACHE  (Memory → Parquet → Internet)
# ──────────────────────────────────────────────
# _cache_lock guards every read and write of _score_cache, _ohlcv_cache, and
# _ohlcv_sync_attempted.  Workers in background threads snapshot per-kind dicts
# under the lock and then read fields off the snapshot without holding it.
_cache_lock = threading.RLock()

# Scored results cache — stores the output of the screener engines.
# Both screeners are keyed by trading date only — no intraday TTL (both use EOD data).
_score_cache: dict[str, dict] = {
    "stage2": {"date": None, "data": None},
    "momentum": {"date": None, "data": None},
}

# Raw OHLCV store — populated by _sync_ohlcv_to_parquet() and _load_and_score();
# consumed as the fastest read path, avoiding repeated parquet or yfinance I/O.
# Format: {symbol: DataFrame(Open,High,Low,Close,Volume)} with DatetimeIndex.
_ohlcv_cache: dict[str, pd.DataFrame] = {}

# Dates for which an OHLCV sync has already been attempted this session.
# Prevents both screeners from independently hitting yfinance for the same date.
_ohlcv_sync_attempted: set[str] = set()

# Single-flight latch: the first background thread to sync a given target_date
# creates an Event here and does the work; subsequent threads wait on it.
_sync_latch_lock = threading.Lock()
_sync_latches: dict[str, threading.Event] = {}


def _get_target_key() -> str:
    """Return the last valid trading date string (cache key), with 19:00 IST after-market cutoff."""
    now = datetime.now(IST)
    start = now.strftime("%Y-%m-%d") if now.hour >= 19 else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return get_last_valid_trading_date(start, load_nse_holidays())


def resolve_screener_data(for_momentum=False, emit=_NOOP_EMIT, as_of_date=None):
    target_key = str(pd.Timestamp(as_of_date).date()) if as_of_date is not None else _get_target_key()
    snapshot = shared_market.load_snapshot(target_key)
    kind = "momentum" if for_momentum else "stage2"
    key = (target_key, snapshot.revisions)
    with _cache_lock:
        hit = _score_cache[kind]
        if hit.get("key") == key and hit.get("data") is not None:
            return hit["data"], target_key, "memory"
    constituents = snapshot.constituents()
    result = _load_and_score(constituents, for_momentum, emit, target_key, snapshot)
    with _cache_lock:
        _score_cache[kind] = {"key": key, "date": target_key, "data": result}
    return result, target_key, "shared"
