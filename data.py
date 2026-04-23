import json
import os
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import yfinance as yf

import db
from config import _MOMENTUM_TTL, HISTORY_DAYS, HISTORY_PERIOD, IST
from momentum_engine import score_momentum
from stage2_engine import check_weinstein_retest, score_stage2


# ──────────────────────────────────────────────
# HOLIDAY & TRADING DAY RESOLVER
# ──────────────────────────────────────────────
def load_nse_holidays() -> set:
    """Load NSE market holidays from nse_holidays.json; returns a set of 'YYYY-MM-DD' strings."""
    path = os.path.join(os.path.dirname(__file__), "nse_holidays.json")
    if not os.path.exists(path):
        return set()
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
    return holidays


def get_last_valid_trading_date(start_date_str: str, holidays: set) -> str:
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
def _load_constituents() -> dict:
    """Load index-to-symbols mapping from constituents.json; shows an error and returns {} if missing."""
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        st.error("❌ `constituents.json` missing.")
        return {}
    with open(const_path, "r") as f:
        return json.load(f)


# ──────────────────────────────────────────────
# OHLCV SYNC
# ──────────────────────────────────────────────
def _sync_ohlcv_to_db(all_symbols: list[str], target_date: str = None) -> bool:
    """
    Incremental sync: fetch only missing dates from yfinance and upsert to DB.
    Returns True if data is available in DB (either already fresh or after fetching).
    A per-session flag ensures we never hit yfinance more than once per target_date,
    even if yfinance doesn't yet carry today's data (global_max < target_date).
    """
    # Already attempted this session for this date — don't re-fetch
    if target_date and target_date in _ohlcv_sync_attempted:
        return True

    tickers = [f"{s}.NS" for s in all_symbols]
    global_max, conservative_min = db.get_latest_ohlcv_date()
    global_min = db.get_earliest_ohlcv_date()

    # Earliest date the DB should cover given HISTORY_PERIOD
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
            if target_date:
                _ohlcv_sync_attempted.add(target_date)
            return True
        fetch_from = (
            datetime.strptime(conservative_min, "%Y-%m-%d") - timedelta(days=10)
        ).strftime("%Y-%m-%d")
        today = datetime.now(IST).strftime("%Y-%m-%d")
        spinner_msg = f"🔄 Incremental update: fetching data since {fetch_from}..."
        fetch_kwargs = {"start": fetch_from, "end": today}

    try:
        with st.spinner(spinner_msg):
            raw = yf.download(
                tickers,
                group_by="ticker",
                threads=True,
                progress=False,
                auto_adjust=True,
                **fetch_kwargs,
            )
    except Exception as e:
        st.error(f"Yahoo Finance Error: {e}")
        return False

    if raw is None or raw.empty:
        return False

    records = []
    available = (
        raw.columns.get_level_values(0).unique().tolist()
        if isinstance(raw.columns, pd.MultiIndex)
        else tickers
    )

    for t in tickers:
        sym = t.replace(".NS", "")
        try:
            if t not in available:
                continue
            sub = (
                raw[t].dropna(how="all") if len(tickers) > 1 else raw.dropna(how="all")
            )
            sub.columns = [c[0] if isinstance(c, tuple) else c for c in sub.columns]
            for dt, row in sub.iterrows():
                if pd.isna(row.get("Close")):
                    continue
                def _f(v):
                    try:
                        f = float(v)
                        return None if pd.isna(f) else f
                    except (TypeError, ValueError):
                        return None
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
        except Exception:
            continue

    if records:
        with st.spinner(f"💾 Saving {len(records):,} rows to database..."):
            db.upsert_ohlcv(records)

    # Mark this date as synced for the remainder of the session so that
    # the second screener (or backtest) doesn't trigger another yfinance fetch.
    if target_date:
        _ohlcv_sync_attempted.add(target_date)
    return True


# ──────────────────────────────────────────────
# BENCHMARK SYNC
# ──────────────────────────────────────────────
BENCHMARK_TICKERS = {
    "NIFTY50": "^NSEI",
    "NIFTY500": "^CRSLDX",
}


def sync_benchmark_data() -> bool:
    """Fetch Nifty 50 and Nifty 500 index close prices from yfinance and upsert to index_ohlcv table."""
    records = []
    for label, ticker in BENCHMARK_TICKERS.items():
        latest = db.get_latest_index_date(label)
        if latest is None:
            fetch_kwargs = {"period": "10y"}
        else:
            fetch_from = (
                datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=3)
            ).strftime("%Y-%m-%d")
            today = datetime.now(IST).strftime("%Y-%m-%d")
            if latest >= today:
                continue
            fetch_kwargs = {"start": fetch_from, "end": today}

        try:
            raw = yf.download(ticker, auto_adjust=True, progress=False, **fetch_kwargs)
            if raw is None or raw.empty:
                continue
            raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
            for dt, row in raw.iterrows():
                if pd.isna(row.get("Close")):
                    continue
                records.append({"symbol": label, "date": dt.date(), "close": float(row["Close"])})
        except Exception:
            continue

    if records:
        db.upsert_index_ohlcv(records)
    return True


def load_benchmark_series() -> dict[str, pd.Series]:
    """Return close price Series for each benchmark index, keyed by label."""
    return {label: db.load_index_ohlcv(label) for label in BENCHMARK_TICKERS}


def _score_from_db(constituents: dict, for_momentum: bool) -> pd.DataFrame:
    """Load recent OHLCV from DB, run the appropriate scorer on each symbol, and return a sorted DataFrame."""
    period_days = 550 if for_momentum else 750
    with st.spinner("📊 Loading history from database and scoring..."):
        symbol_data = db.load_ohlcv_all(period_days=period_days)

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
        except Exception:
            continue

    df = pd.DataFrame(results)
    if df.empty:
        return df
    return df.sort_values("Score" if not for_momentum else "Close", ascending=False)


# ──────────────────────────────────────────────
# SINGLE-SYMBOL CHART DATA
# ──────────────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_chart_data(symbol: str) -> pd.DataFrame:
    """Return 2y OHLCV DataFrame for one symbol; tries DB first, falls back to yfinance."""
    df = db.load_ohlcv_symbol(symbol.upper(), period_days=750)
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
_mem_cache: dict[str, dict] = {
    "stage2":   {"date": None, "data": None},
    "momentum": {"date": None, "data": None, "ts": None},
    "backtest": {"date": None, "data": None, "ts": None},
}

# Dates for which an OHLCV sync has already been attempted this session.
# Prevents both screeners (and backtest) from hitting yfinance independently
# when they share the same underlying OHLCV store.
_ohlcv_sync_attempted: set[str] = set()


def _get_target_key() -> str:
    """Return the last valid trading date string to use as the cache key (after-market cutoff at 19:00 IST)."""
    now = datetime.now(IST)
    start = (
        now.strftime("%Y-%m-%d")
        if now.hour >= 19
        else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    )
    return get_last_valid_trading_date(start, load_nse_holidays())


def load_ohlcv_for_backtest() -> tuple[dict, str, str]:
    """
    3-tier load of the full OHLCV history for backtesting (~5 years).
      Tier 1 — in-memory dict keyed by last trading date (TTL: 1 hour)
      Tier 2 — PostgreSQL (persists across restarts; read without re-fetching if fresh)
      Tier 3 — yfinance incremental sync when DB is stale
    Returns (symbol_data, target_date, source) where source is 'memory' | 'db' | 'internet'.
    """
    target_key = _get_target_key()
    constituents = _load_constituents()
    all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))

    bc = _mem_cache["backtest"]
    now = datetime.now()
    if (
        bc["data"] is not None
        and bc["date"] == target_key
        and bc["ts"]
        and (now - bc["ts"]).total_seconds() < _MOMENTUM_TTL
    ):
        return bc["data"], target_key, "memory"

    # Tier 3: sync from yfinance if DB is stale
    _sync_ohlcv_to_db(all_symbols, target_date=target_key)

    # Tier 2: load from DB
    with st.spinner(f"📊 Loading {HISTORY_PERIOD} OHLCV history from database…"):
        symbol_data = db.load_ohlcv_all(period_days=HISTORY_DAYS)

    if symbol_data:
        _mem_cache["backtest"] = {"date": target_key, "data": symbol_data, "ts": now}

    source = "memory" if bc["data"] is not None and bc["date"] == target_key else "db"
    return symbol_data, target_key, source


def load_compositions() -> pd.DataFrame:
    """
    Load historical index compositions from data/compositions.csv.
    Returns a DataFrame with columns [INDEX_NAME, TIME_STAMP, SYMBOL] for use
    in survivorship-bias-aware backtesting.  Returns an empty DataFrame if the
    file is missing.
    """
    path = os.path.join(os.path.dirname(__file__), "data", "compositions.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["TIME_STAMP"])
    return df[["INDEX_NAME", "TIME_STAMP", "SYMBOL"]].dropna(subset=["SYMBOL"])


def resolve_screener_data(
    rsi_filter: bool, for_momentum: bool = False, universe: str = None
):
    """
    3-tier resolution for both screeners:
      Tier 1 — in-memory (same process, keyed by trading date / TTL)
      Tier 2 — SQLite/PostgreSQL (persists across restarts)
      Tier 3 — yfinance internet fetch (only when DB is stale)
    Returns (df, date_str, source) where source is 'memory' | 'db' | 'internet' | 'error'.
    """
    target_key = _get_target_key()
    constituents = _load_constituents()
    if not constituents:
        return pd.DataFrame(), target_key, "error"
    all_symbols = list(
        dict.fromkeys([s for syms in constituents.values() for s in syms])
    )

    if for_momentum:
        mc = _mem_cache["momentum"]
        now = datetime.now()

        if (
            mc["data"] is not None
            and mc["date"] == target_key
            and mc["ts"]
            and (now - mc["ts"]).total_seconds() < _MOMENTUM_TTL
        ):
            return mc["data"], target_key, "memory"

        _sync_ohlcv_to_db(all_symbols, target_date=target_key)
        df = _score_from_db(constituents, for_momentum=True)

        source = "db" if (mc["data"] is None or mc["date"] != target_key) else "memory"
        if not df.empty:
            _mem_cache["momentum"] = {"date": target_key, "data": df, "ts": now}
        return df, target_key, source

    else:
        mc = _mem_cache["stage2"]

        if mc["data"] is not None and mc["date"] == target_key:
            return mc["data"], target_key, "memory"

        cached_df = db.load_stage2_cache(target_key)
        if cached_df is not None:
            _mem_cache["stage2"] = {"date": target_key, "data": cached_df}
            return cached_df, target_key, "db"

        synced = _sync_ohlcv_to_db(all_symbols, target_date=target_key)
        if synced:
            df = _score_from_db(constituents, for_momentum=False)
            if not df.empty:
                db.save_stage2_cache(target_key, df)
                _mem_cache["stage2"] = {"date": target_key, "data": df}
                return df, target_key, "internet"

        fallback_df, fallback_date = db.load_latest_stage2_cache()
        if fallback_df is not None:
            return fallback_df, fallback_date, "db"

        return pd.DataFrame(), target_key, "error"
