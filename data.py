import functools
import json
import logging
import os
import threading
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
import streamlit as st
import yfinance as yf

# Default no-op emit used when callers don't need progress reporting.
# Signature: (level: str, message: str) -> None
# Levels: "info" | "warning" | "error" | "success"
_NOOP_EMIT: Callable[[str, str], None] = lambda _lv, _msg: None

import db
from config import _MOMENTUM_TTL, HISTORY_DAYS, HISTORY_PERIOD, IST
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
# OHLCV SYNC
# ──────────────────────────────────────────────
def _records_to_symbol_data(records: list[dict]) -> dict[str, pd.DataFrame]:
    """
    Convert a list of OHLCV record dicts (as written by _sync_ohlcv_to_db) to the
    {symbol: DataFrame} format returned by db.load_ohlcv_all().  Used to populate
    the in-memory fallback store when the database is unavailable.
    """
    if not records:
        return {}
    from collections import defaultdict
    buckets: dict[str, list] = defaultdict(list)
    for r in records:
        buckets[r["symbol"]].append(r)
    result: dict[str, pd.DataFrame] = {}
    for sym, rows in buckets.items():
        df = pd.DataFrame(rows).drop(columns="symbol")
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        df = df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        })
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

    available = (
        raw.columns.get_level_values(0).unique().tolist()
        if isinstance(raw.columns, pd.MultiIndex)
        else tickers
    )
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
                records.append({
                    "symbol": sym,
                    "date": dt.date(),
                    "open": _f(row.get("Open")),
                    "high": _f(row.get("High")),
                    "low": _f(row.get("Low")),
                    "close": float(row["Close"]),
                    "volume": int(row.get("Volume") or 0),
                })
        except Exception:
            continue
    return records


def _sync_ohlcv_to_db(
    all_symbols: list[str],
    target_date: str = None,
    emit: Callable[[str, str], None] = _NOOP_EMIT,
    force_download: bool = False,
) -> bool:
    """
    Incremental sync: fetch only missing dates from yfinance and upsert to DB.
    Returns True if data is available in DB (either already fresh or after fetching).

    Single-flight guarantee: the first thread to sync a given target_date does the
    work; concurrent threads block on a threading.Event until the leader finishes,
    then return True without re-fetching.

    When `force_download=True`, bypass the attempted-set guard AND the
    "DB already has fresh data" shortcut — do a real yfinance fetch and populate
    `_ohlcv_cache`.  Used as a last-resort fallback when DB reads succeed for small
    queries but fail/time-out for the backtest's 10-year query on flaky hosts.
    """
    # Already synced this process run — skip immediately (unless forced).
    if target_date and not force_download:
        with _cache_lock:
            if target_date in _ohlcv_sync_attempted:
                return True

    # Single-flight latch: leader creates the Event; waiters block on it.
    # Skipped when forcing a re-download (the prior latch is already resolved).
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
    # target_date is None or force_download → always leader, no latch needed.

    try:
        tickers = [f"{s}.NS" for s in all_symbols]
        if force_download:
            # Skip DB date checks — go straight to full yfinance download
            global_max = None
            conservative_min = None
            global_min = None
        else:
            try:
                global_max, conservative_min = db.get_latest_ohlcv_date()
                global_min = db.get_earliest_ohlcv_date()
            except Exception as _db_exc:
                emit("warning", f"⚠️ DB unavailable — fetching full history from Yahoo Finance: {_db_exc}")
                global_max = None
                conservative_min = None
                global_min = None

        earliest_needed = (
            datetime.now(IST) - timedelta(days=HISTORY_DAYS)
        ).strftime("%Y-%m-%d")

        needs_backfill = global_min is None or global_min > earliest_needed

        if global_max is None or needs_backfill:
            spinner_msg = (
                f"🌐 Downloading {HISTORY_PERIOD} history for {len(tickers)} stocks"
                + (" (backfilling missing history)…" if needs_backfill and global_max else "…")
            )
            fetch_kwargs = {"period": HISTORY_PERIOD}
        else:
            if target_date and global_max >= target_date:
                with _cache_lock:
                    _ohlcv_sync_attempted.add(target_date)
                return True
            fetch_from = (
                datetime.strptime(conservative_min, "%Y-%m-%d") - timedelta(days=10)
            ).strftime("%Y-%m-%d")
            today = datetime.now(IST).strftime("%Y-%m-%d")
            spinner_msg = f"🔄 Incremental update: fetching data since {fetch_from}..."
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
            # Always populate the in-memory fallback store so callers can use
            # _ohlcv_cache when the DB is unavailable for reads.
            with _cache_lock:
                _ohlcv_cache.update(_records_to_symbol_data(records))
            emit("info", f"💾 Saving {len(records):,} rows to database…")
            try:
                db.upsert_ohlcv(records, emit=emit)
            except Exception as _db_exc:
                emit("warning", f"⚠️ DB write failed — data cached in memory only: {_db_exc}")
                # _ohlcv_cache was populated above; _load_and_score will prefer it.
                # The DB remains stale until the next cold start triggers a re-sync.

        if target_date:
            with _cache_lock:
                _ohlcv_sync_attempted.add(target_date)
        return True

    finally:
        if latch_evt is not None:
            latch_evt.set()
            with _sync_latch_lock:
                _sync_latches.pop(target_date, None)


# sync_benchmark_data and load_benchmark_series removed — moved to data_backtest.py.
# The backtest app is now fully parquet-backed; the screener app (app.py) does
# not use benchmark series at all.


def _load_and_score(
    constituents: dict,
    for_momentum: bool,
    emit: Callable[[str, str], None] = _NOOP_EMIT,
) -> pd.DataFrame:
    """Load recent OHLCV from memory (preferred) or DB, run the scorer, return a sorted DataFrame."""
    # 550 calendar days ≈ 392 trading days — enough for MA200 (200td) + MA_RISING_LOOKBACK (50td)
    # + 52w-high (252td) with margin.  Both screeners use the same window so one DB read
    # populates _ohlcv_cache for the other.  Stage 2 previously requested 750 days which exceeded
    # the 2y table size and returned the whole table even with an index; 550 fits within it.
    period_days = 550
    symbol_data: dict[str, pd.DataFrame] | None = None

    # Prefer in-memory cache when populated — avoids a full-table-scan query
    # against Supabase (the date column is TEXT, so `date::date >= NOW() - ...`
    # cannot use an index and routinely hits the 60s statement timeout).
    with _cache_lock:
        if _ohlcv_cache:
            symbol_data = dict(_ohlcv_cache)
    if symbol_data:
        emit("info", f"📦 Using in-memory OHLCV cache ({len(symbol_data)} symbols)")
    else:
        emit("info", "📊 Loading price history from database…")
        try:
            symbol_data = db.load_ohlcv_all(period_days=period_days)
            if symbol_data:
                with _cache_lock:
                    _ohlcv_cache.update(symbol_data)
        except Exception as _db_exc:
            emit("warning", f"⚠️ DB read failed — using in-memory OHLCV cache: {_db_exc}")

    if not symbol_data:
        # Fallback A: use whatever _sync_ohlcv_to_db already downloaded to memory
        with _cache_lock:
            symbol_data = dict(_ohlcv_cache)

    # Fallback B (last resort): force fresh yfinance download
    if not symbol_data:
        emit("warning", "⚠️ DB unreachable and memory empty — forcing fresh Yahoo Finance download…")
        all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))
        _sync_ohlcv_to_db(all_symbols, emit=emit, force_download=True)
        with _cache_lock:
            symbol_data = dict(_ohlcv_cache)

    if not symbol_data:
        emit("error", "❌ No OHLCV data available — DB is unreachable and Yahoo Finance download failed")
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
# SINGLE-SYMBOL CHART DATA
# ──────────────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_chart_data(symbol: str) -> pd.DataFrame:
    """Return 2y OHLCV DataFrame for one symbol; tries DB first, falls back to yfinance."""
    try:
        df = db.load_ohlcv_symbol(symbol.upper(), period_days=750)
    except Exception:
        df = pd.DataFrame()
    if not df.empty:
        return df
    try:
        raw = yf.download(
            f"{symbol.upper()}.NS",
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
# 3-TIER CACHE  (Memory → DB → Internet)
# ──────────────────────────────────────────────
# _cache_lock guards every read and write of _score_cache, _ohlcv_cache, and
# _ohlcv_sync_attempted.  Workers in background threads snapshot per-kind dicts
# under the lock and then read fields off the snapshot without holding it.
_cache_lock = threading.RLock()

# Scored results cache — stores the output of the screener engines.
# stage2 is keyed by trading date; momentum adds a TTL timestamp for intraday refresh.
_score_cache: dict[str, dict] = {
    "stage2":   {"date": None, "data": None},
    "momentum": {"date": None, "data": None, "ts": None},
}

# Raw OHLCV store — populated by _sync_ohlcv_to_db() whenever yfinance data is
# downloaded; consumed by _load_and_score() when db.load_ohlcv_all() is unavailable.
# Format matches db.load_ohlcv_all(): {symbol: DataFrame(Open,High,Low,Close,Volume)}.
_ohlcv_cache: dict[str, pd.DataFrame] = {}

# Dates for which an OHLCV sync has already been attempted this session.
# Prevents both screeners from hitting yfinance independently when they share
# the same underlying OHLCV store.
_ohlcv_sync_attempted: set[str] = set()

# Single-flight latch: the first background thread to sync a given target_date
# creates an Event here and does the work; subsequent threads wait on it.
_sync_latch_lock = threading.Lock()
_sync_latches: dict[str, threading.Event] = {}


def _get_target_key() -> str:
    """Return the last valid trading date string to use as the cache key (after-market cutoff at 19:00 IST)."""
    now = datetime.now(IST)
    start = (
        now.strftime("%Y-%m-%d")
        if now.hour >= 19
        else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    )
    return get_last_valid_trading_date(start, load_nse_holidays())


# load_ohlcv_for_backtest and load_compositions removed — moved to data_backtest.py.
# The backtest app reads from parquet; the screener app only needs resolve_screener_data.


def resolve_screener_data(
    for_momentum: bool = False,
    emit: Callable[[str, str], None] = _NOOP_EMIT,
):
    """
    3-tier resolution for both screeners:
      Tier 1 — in-memory (same process, keyed by trading date; momentum adds TTL)
      Tier 2 — PostgreSQL (persists across restarts; consulted on cold start only)
      Tier 3 — yfinance internet fetch (only when DB is stale or absent)
    Returns (df, date_str, source) where source is 'memory' | 'db' | 'internet' | 'error'.
    """
    target_key = _get_target_key()
    constituents = _load_constituents()
    if not constituents:
        emit("error", "❌ constituents.json missing — check the data directory")
        return pd.DataFrame(), target_key, "error"
    all_symbols = list(
        dict.fromkeys([s for syms in constituents.values() for s in syms])
    )

    if for_momentum:
        with _cache_lock:
            mc = _score_cache["momentum"]
        now = datetime.now()

        # Tier 1: memory with TTL — serve if data is fresh enough within this session.
        if (
            mc["data"] is not None
            and mc["date"] == target_key
            and mc["ts"]
            and (now - mc["ts"]).total_seconds() < _MOMENTUM_TTL
        ):
            return mc["data"], target_key, "memory"

        # Tier 2: DB cache — consulted only on cold start (no in-memory data for today).
        # When TTL expires mid-session, skip straight to Tier 3 to get fresh scores.
        if mc["data"] is None or mc["date"] != target_key:
            try:
                cached_df = db.load_momentum_cache(target_key)
            except Exception:
                cached_df = None
            if cached_df is not None:
                with _cache_lock:
                    _score_cache["momentum"] = {"date": target_key, "data": cached_df, "ts": now}
                return cached_df, target_key, "db"

        # Tier 3: score fresh from OHLCV.
        _sync_ohlcv_to_db(all_symbols, target_date=target_key, emit=emit)
        df = _load_and_score(constituents, for_momentum=True, emit=emit)
        if not df.empty:
            try:
                db.save_momentum_cache(target_key, df)
            except Exception as _exc:
                emit("warning", f"⚠️ Failed to save momentum cache to DB: {_exc}")
            with _cache_lock:
                _score_cache["momentum"] = {"date": target_key, "data": df, "ts": now}
        return df, target_key, "internet" if not df.empty else "error"

    else:
        with _cache_lock:
            mc = _score_cache["stage2"]

        # Tier 1: memory — serve if date matches.
        if mc["data"] is not None and mc["date"] == target_key:
            return mc["data"], target_key, "memory"

        # Tier 2: DB cache — load pre-scored results for today's trading date.
        try:
            cached_df = db.load_stage2_cache(target_key)
        except Exception:
            cached_df = None  # DB unavailable; skip to re-score
        if cached_df is not None:
            with _cache_lock:
                _score_cache["stage2"] = {"date": target_key, "data": cached_df}
            return cached_df, target_key, "db"

        # Tier 3: sync OHLCV, score, persist.
        synced = _sync_ohlcv_to_db(all_symbols, target_date=target_key, emit=emit)
        if synced:
            df = _load_and_score(constituents, for_momentum=False, emit=emit)
            if not df.empty:
                try:
                    db.save_stage2_cache(target_key, df)
                except Exception as _exc:
                    emit("warning", f"⚠️ Failed to save Stage 2 cache to DB: {_exc}")
                with _cache_lock:
                    _score_cache["stage2"] = {"date": target_key, "data": df}
                return df, target_key, "internet"

        # Last resort: serve the most recent available cache entry from DB.
        try:
            fallback_df, fallback_date = db.load_latest_stage2_cache()
        except Exception:
            fallback_df, fallback_date = None, None
        if fallback_df is not None:
            return fallback_df, fallback_date, "db"

        return pd.DataFrame(), target_key, "error"
