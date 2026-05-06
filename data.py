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
import yfinance as yf

# Default no-op emit used when callers don't need progress reporting.
# Signature: (level: str, message: str) -> None
# Levels: "info" | "warning" | "error" | "success"
_NOOP_EMIT: Callable[[str, str], None] = lambda _lv, _msg: None

from config import (
    HISTORY_DAYS,
    HISTORY_PERIOD,
    IST,
    MOMENTUM_CACHE_PARQUET,
    SCREENER_OHLCV_PARQUET,
    STAGE2_CACHE_PARQUET,
)
from momentum_engine import score_momentum
from stage2_engine import check_weinstein_retest, score_stage2


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
    for segment in data.values():
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
_STALENESS_DAYS_COMPOSITIONS = 30


def check_data_freshness() -> list[tuple[str, str]]:
    """
    Inspect key reference data files and return (level, message) pairs for anything stale or missing.
    level is 'warning' or 'error'.  Callers should render these as st.warning / st.error.
    """
    issues: list[tuple[str, str]] = []
    today = datetime.now(IST).date()
    repo = os.path.dirname(os.path.abspath(__file__))

    # ── constituents.json ──────────────────────
    const_path = os.path.join(repo, "constituents.json")
    if not os.path.exists(const_path):
        issues.append(
            (
                "error",
                "**constituents.json** not found — index universe is unknown. "
                "Download the latest file from NSE and place it in the repo root.",
            )
        )
    else:
        age = (today - datetime.fromtimestamp(os.path.getmtime(const_path)).date()).days
        if age > _STALENESS_DAYS_CONSTITUENTS:
            updated = datetime.fromtimestamp(os.path.getmtime(const_path)).strftime("%d %b %Y")
            issues.append(
                (
                    "warning",
                    f"**constituents.json** is {age} days old (last updated {updated}). "
                    "Index membership may be stale — refresh from NSE.",
                )
            )

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

    # ── compositions.parquet ──────────────────
    comp_path = os.path.join(repo, "data", "compositions.parquet")
    if not os.path.exists(comp_path):
        issues.append(
            (
                "warning",
                "**data/compositions.parquet** not found — survivorship-bias correction is disabled. "
                "Run `scripts/refresh_backtest_parquet.py` to generate it.",
            )
        )
    else:
        try:
            df = pd.read_parquet(comp_path, columns=["TIME_STAMP"])
            max_ts = pd.to_datetime(df["TIME_STAMP"]).max().date()
            lag = (today - max_ts).days
            if lag > _STALENESS_DAYS_COMPOSITIONS:
                issues.append(
                    (
                        "warning",
                        f"**compositions.parquet** last covers {max_ts.strftime('%d %b %Y')} "
                        f"({lag} days ago) — the constituent filter may be stale. "
                        "Run `scripts/refresh_backtest_parquet.py` to update.",
                    )
                )
        except Exception:
            pass

    return issues


# ──────────────────────────────────────────────
# CONSTITUENTS
# ──────────────────────────────────────────────
@functools.lru_cache(maxsize=None)
def _load_constituents() -> dict:
    """Load index-to-symbols mapping from constituents.json; returns {} if missing."""
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        return {}
    with open(const_path, "r") as f:
        return json.load(f)


# ──────────────────────────────────────────────
# PARQUET HELPERS
# ──────────────────────────────────────────────
# Serialises all writes to screener parquet files within this process.
# Cross-process writes (e.g. refresh script) are safe via atomic rename.
_parquet_write_lock = threading.Lock()


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
    """Return the cached baseline DataFrame, loading from parquet on first call."""
    global _screener_baseline
    with _cache_lock:
        if _screener_baseline is not None:
            return _screener_baseline
        # Hold the lock through the read so only one thread pays the I/O cost
        # and no reader can observe a half-initialised baseline.
        if not os.path.exists(SCREENER_OHLCV_PARQUET):
            return pd.DataFrame()
        df = pd.read_parquet(SCREENER_OHLCV_PARQUET)
        df["date"] = pd.to_datetime(df["date"])
        _screener_baseline = df
        return df


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


def _load_score_cache(path: str, target_date: str) -> pd.DataFrame | None:
    """Read a score-cache parquet and return rows matching target_date, or None."""
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    if "cache_date" not in df.columns or df.empty:
        return None
    match = df[df["cache_date"].astype(str) == target_date].drop(columns="cache_date")
    return match.reset_index(drop=True) if not match.empty else None


def _load_latest_score_cache(path: str) -> tuple[pd.DataFrame | None, str | None]:
    """Return (df, date_str) for the most recent entry in a score-cache parquet."""
    if not os.path.exists(path):
        return None, None
    df = pd.read_parquet(path)
    if "cache_date" not in df.columns or df.empty:
        return None, None
    latest = str(df["cache_date"].max())
    match = df[df["cache_date"].astype(str) == latest].drop(columns="cache_date")
    return (match.reset_index(drop=True) if not match.empty else None), latest


_SCORE_CACHE_MAX_DATES = 5  # rolling window kept in each score-cache parquet


def _save_score_cache(path: str, target_date: str, df: pd.DataFrame) -> None:
    """Persist scored results for target_date to a score-cache parquet.

    Keeps the last _SCORE_CACHE_MAX_DATES trading days so stale-parquet fallback
    (_load_latest_score_cache) can serve a recent result even after a weekend
    or holiday when the target date hasn't been scored yet.
    """
    out = df.copy()
    out["cache_date"] = target_date
    with _parquet_write_lock:
        if os.path.exists(path):
            try:
                existing = pd.read_parquet(path)
                # Drop any prior entry for today (idempotent re-score) then append.
                existing = existing[existing["cache_date"].astype(str) != target_date]
                all_dates = sorted(existing["cache_date"].astype(str).unique())
                # Evict oldest entries beyond the rolling window.
                if len(all_dates) >= _SCORE_CACHE_MAX_DATES:
                    keep = set(all_dates[-(_SCORE_CACHE_MAX_DATES - 1) :])
                    existing = existing[existing["cache_date"].astype(str).isin(keep)]
                out = pd.concat([existing, out], ignore_index=True)
            except Exception:
                pass  # corrupted cache — overwrite cleanly with today's data
        _write_parquet_atomic(out, path)


# ──────────────────────────────────────────────
# OHLCV SYNC
# ──────────────────────────────────────────────
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


def _sync_ohlcv_to_parquet(
    all_symbols: list[str],
    target_date: str | None = None,
    emit: Callable[[str, str], None] = _NOOP_EMIT,
    force_download: bool = False,
) -> bool:
    """
    Incremental sync: fetch only missing dates from yfinance and merge into the
    screener parquet baseline.  Returns True if OHLCV data is available.

    Single-flight guarantee: the first thread to sync a given target_date does the
    work; concurrent threads block on a threading.Event until the leader finishes.

    When `force_download=True`, bypass the attempted-set guard and the
    "baseline already fresh" shortcut — do a real yfinance fetch and populate
    `_ohlcv_cache`.
    """
    global _screener_baseline

    # Already synced this process run — skip immediately (unless forced).
    if target_date and not force_download:
        with _cache_lock:
            if target_date in _ohlcv_sync_attempted:
                return True

    # Single-flight latch: leader does the work, waiters block.
    latch_evt: threading.Event | None = None
    if target_date and not force_download:
        with _sync_latch_lock:
            if target_date in _sync_latches:
                latch_evt = _sync_latches[target_date]
                is_leader = False
            else:
                latch_evt = threading.Event()
                _sync_latches[target_date] = latch_evt
                is_leader = True
        if not is_leader:
            emit("info", "⏳ OHLCV sync already in progress — waiting…")
            latch_evt.wait(timeout=300)
            return True

    try:
        tickers = [f"{s}.NS" for s in all_symbols]

        if force_download:
            global_max = None
            conservative_min = None
            global_min = None
        else:
            baseline = _load_screener_baseline()
            if not baseline.empty and "date" in baseline.columns:
                dates = pd.to_datetime(baseline["date"])
                global_max = dates.max().strftime("%Y-%m-%d")
                global_min = dates.min().strftime("%Y-%m-%d")
                sym_maxes = baseline.groupby("symbol")["date"].max()
                conservative_min = pd.to_datetime(sym_maxes).min().strftime("%Y-%m-%d")
            else:
                global_max = None
                conservative_min = None
                global_min = None

        earliest_needed = (datetime.now(IST) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")

        needs_backfill = global_min is None or global_min > earliest_needed

        if global_max is None or needs_backfill:
            spinner_msg = f"🌐 Downloading {HISTORY_PERIOD} history for {len(tickers)} stocks" + (
                " (backfilling missing history)…" if needs_backfill and global_max else "…"
            )
            fetch_kwargs = {"period": HISTORY_PERIOD}
        else:
            if target_date and global_max >= target_date:
                with _cache_lock:
                    _ohlcv_sync_attempted.add(target_date)
                return True
            assert conservative_min is not None  # set alongside global_max; both None only when global_max is None
            fetch_from = (datetime.strptime(conservative_min, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
            today = datetime.now(IST).strftime("%Y-%m-%d")
            spinner_msg = f"🔄 Incremental update: fetching data since {fetch_from}…"
            fetch_kwargs = {"start": fetch_from, "end": today}

        try:
            emit("info", spinner_msg)
            raw = yf.download(
                tickers,
                group_by="ticker",
                threads=True,
                progress=False,
                auto_adjust=True,
                **fetch_kwargs,
            )
        except Exception as e:
            emit("error", f"Yahoo Finance error: {e}")
            return False

        if raw is None or raw.empty:
            return False

        records = _parse_yfinance_download(raw, tickers)

        if records:
            # Build a DataFrame from new records (uppercase column names to match baseline)
            new_df = pd.DataFrame(records)
            new_df["date"] = pd.to_datetime(new_df["date"])
            new_df = new_df.rename(
                columns={
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "volume": "Volume",
                }
            )
            for col in ["Open", "High", "Low", "Close"]:
                if col in new_df.columns:
                    new_df[col] = new_df[col].astype("float32")
            new_df["Volume"] = new_df["Volume"].astype("int64")

            # Merge with existing baseline, deduplicate, sort
            existing = _load_screener_baseline()
            if not existing.empty:
                existing = existing.copy()
                existing["date"] = pd.to_datetime(existing["date"])
                merged = pd.concat([existing, new_df], ignore_index=True)
                merged = merged.drop_duplicates(subset=["symbol", "date"], keep="last")
            else:
                merged = new_df
            merged = merged.sort_values(["symbol", "date"]).reset_index(drop=True)

            emit("info", f"💾 Saving {len(merged):,} rows to screener parquet…")
            try:
                with _parquet_write_lock:
                    _write_parquet_atomic(merged, SCREENER_OHLCV_PARQUET)
                with _cache_lock:
                    _screener_baseline = merged
                    # Clear the symbol dict so _load_and_score rebuilds from the full
                    # merged baseline rather than using only the short delta records.
                    # An incremental fetch only covers the tail (days since last sync);
                    # leaving those short per-symbol DataFrames in _ohlcv_cache would
                    # cause every stock to fail the ≥250-row scoring guard.
                    _ohlcv_cache.clear()
            except Exception as _exc:
                emit("warning", f"⚠️ Parquet write failed — data cached in memory only: {_exc}")
                # On write failure keep the short delta in memory as a best-effort fallback;
                # scoring will degrade gracefully (stocks with < 250 rows return None).
                with _cache_lock:
                    _ohlcv_cache.update(_records_to_symbol_data(records))

        if target_date:
            with _cache_lock:
                _ohlcv_sync_attempted.add(target_date)
        return True

    finally:
        if latch_evt is not None and target_date is not None:
            latch_evt.set()
            with _sync_latch_lock:
                _sync_latches.pop(target_date, None)


def _load_and_score(
    constituents: dict,
    for_momentum: bool,
    emit: Callable[[str, str], None] = _NOOP_EMIT,
) -> pd.DataFrame:
    """Load recent OHLCV from memory (preferred) or parquet, run the scorer, return sorted DataFrame."""
    # 550 calendar days ≈ 392 trading days — enough for MA200 + MA_RISING_LOOKBACK + 52w-high.
    period_days = 550
    symbol_data: dict[str, pd.DataFrame] | None = None

    # Prefer in-memory cache — avoids re-reading from disk on every request.
    with _cache_lock:
        if _ohlcv_cache:
            symbol_data = dict(_ohlcv_cache)
    if symbol_data:
        emit("info", f"📦 Using in-memory OHLCV cache ({len(symbol_data)} symbols)")
    else:
        emit("info", "📊 Loading price history from parquet…")
        try:
            baseline = _load_screener_baseline()
            if not baseline.empty:
                cutoff = pd.Timestamp.now() - pd.Timedelta(days=period_days)
                filtered = baseline[pd.to_datetime(baseline["date"]) >= cutoff]
                symbol_data = _long_to_symbol_dict(filtered)
                if symbol_data:
                    with _cache_lock:
                        _ohlcv_cache.update(symbol_data)
        except Exception as _exc:
            emit("warning", f"⚠️ Parquet read failed — using in-memory OHLCV cache: {_exc}")

    if not symbol_data:
        with _cache_lock:
            symbol_data = dict(_ohlcv_cache)

    # Last resort: force fresh yfinance download
    if not symbol_data:
        emit("warning", "⚠️ Parquet unavailable and memory empty — forcing fresh Yahoo Finance download…")
        all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))
        _sync_ohlcv_to_parquet(all_symbols, emit=emit, force_download=True)
        with _cache_lock:
            symbol_data = dict(_ohlcv_cache)

    if not symbol_data:
        emit("error", "❌ No OHLCV data available — parquet missing and Yahoo Finance download failed")
        return pd.DataFrame()

    emit("info", f"⚙️ Scoring {len(symbol_data)} symbols…")

    results = []
    for sym, sub in symbol_data.items():
        try:
            res = score_momentum(sub) if for_momentum else score_stage2(sub)
            if res:
                res["Symbol"] = sym
                res["Index"] = next(
                    (idx for idx, syms in constituents.items() if sym in syms),
                    "Unknown",
                )
                if not for_momentum:
                    res["Retest"] = check_weinstein_retest(sub)
                results.append(res)
        except Exception as exc:
            logging.warning("scoring failed for %s: %s", sym, exc)
            continue

    df = pd.DataFrame(results)
    if df.empty:
        return df
    return df.sort_values("Score" if not for_momentum else "Sharpe_1Y", ascending=False, na_position="last")


# ──────────────────────────────────────────────
# UNIVERSE COVERAGE ANALYSIS
# ──────────────────────────────────────────────
_MIN_SCORING_ROWS = 250  # matches score_stage2 / score_momentum threshold
_OHLCV_WINDOW_DAYS = 550  # matches _load_and_score cutoff


@st.cache_data(ttl=3600)
def get_universe_coverage() -> dict:
    """Return per-index and per-symbol coverage stats against the scoring threshold."""
    constituents = _load_constituents()
    if not constituents:
        return {}

    baseline = _load_screener_baseline()
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=_OHLCV_WINDOW_DAYS)
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


@st.cache_data(ttl=3600)
def fetch_chart_data(symbol: str) -> pd.DataFrame:
    """Return OHLCV DataFrame for one symbol (up to 2y); tries parquet baseline, falls back to yfinance."""
    clean = symbol.strip().upper()
    if not _VALID_TICKER_RE.match(clean):
        logging.warning("fetch_chart_data: invalid ticker format %r — returning empty", symbol)
        return pd.DataFrame()
    baseline = _load_screener_baseline()
    if not baseline.empty:
        sym_data = baseline[baseline["symbol"] == clean]
        if not sym_data.empty:
            sub = sym_data.drop(columns="symbol").copy()
            sub["date"] = pd.to_datetime(sub["date"])
            return sub.set_index("date").sort_index()
    try:
        raw = yf.download(
            f"{clean}.NS",
            period="2y",
            auto_adjust=True,
            progress=False,
        )
        if not raw.empty:
            raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
            return raw[["Open", "High", "Low", "Close", "Volume"]]
    except Exception:
        pass
    return pd.DataFrame()


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


def resolve_screener_data(
    for_momentum: bool = False,
    emit: Callable[[str, str], None] = _NOOP_EMIT,
):
    """
    3-tier resolution for both screeners:
      Tier 1 — in-memory (same process, keyed by trading date)
      Tier 2 — local parquet file (persists across restarts; consulted on cold start only)
      Tier 3 — yfinance internet fetch (only when parquet is stale or absent)
    Returns (df, date_str, source) where source is 'memory' | 'db' | 'internet' | 'error'.
    """
    target_key = _get_target_key()
    constituents = _load_constituents()
    if not constituents:
        emit("error", "❌ constituents.json missing — check the data directory")
        return pd.DataFrame(), target_key, "error"
    all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))

    if for_momentum:
        with _cache_lock:
            mc = _score_cache["momentum"]

        # Tier 1: memory
        if mc["data"] is not None and mc["date"] == target_key:
            return mc["data"], target_key, "memory"

        # Tier 2: parquet cache
        try:
            cached_df = _load_score_cache(MOMENTUM_CACHE_PARQUET, target_key)
        except Exception:
            cached_df = None
        if cached_df is not None:
            with _cache_lock:
                _score_cache["momentum"] = {"date": target_key, "data": cached_df}
            return cached_df, target_key, "db"

        # Tier 3: score fresh from OHLCV
        _sync_ohlcv_to_parquet(all_symbols, target_date=target_key, emit=emit)
        df = _load_and_score(constituents, for_momentum=True, emit=emit)
        if not df.empty:
            try:
                _save_score_cache(MOMENTUM_CACHE_PARQUET, target_key, df)
            except Exception as _exc:
                emit("warning", f"⚠️ Failed to save momentum cache: {_exc}")
            with _cache_lock:
                _score_cache["momentum"] = {"date": target_key, "data": df}
        return df, target_key, "internet" if not df.empty else "error"

    else:
        with _cache_lock:
            mc = _score_cache["stage2"]

        # Tier 1: memory
        if mc["data"] is not None and mc["date"] == target_key:
            return mc["data"], target_key, "memory"

        # Tier 2: parquet cache
        try:
            cached_df = _load_score_cache(STAGE2_CACHE_PARQUET, target_key)
        except Exception:
            cached_df = None
        if cached_df is not None:
            with _cache_lock:
                _score_cache["stage2"] = {"date": target_key, "data": cached_df}
            return cached_df, target_key, "db"

        # Tier 3: sync OHLCV, score, persist
        synced = _sync_ohlcv_to_parquet(all_symbols, target_date=target_key, emit=emit)
        if synced:
            df = _load_and_score(constituents, for_momentum=False, emit=emit)
            if not df.empty:
                try:
                    _save_score_cache(STAGE2_CACHE_PARQUET, target_key, df)
                except Exception as _exc:
                    emit("warning", f"⚠️ Failed to save Stage 2 cache: {_exc}")
                with _cache_lock:
                    _score_cache["stage2"] = {"date": target_key, "data": df}
                return df, target_key, "internet"

        # Last resort: serve the most recent available score cache from parquet
        try:
            fallback_df, fallback_date = _load_latest_score_cache(STAGE2_CACHE_PARQUET)
        except Exception:
            fallback_df, fallback_date = None, None
        if fallback_df is not None:
            return fallback_df, fallback_date, "db"

        return pd.DataFrame(), target_key, "error"
