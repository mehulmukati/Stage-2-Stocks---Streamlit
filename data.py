import os
import json
import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

import db
from config import IST, HISTORY_PERIOD, _MOMENTUM_TTL
from stage2_engine import score_stage2
from momentum_engine import score_momentum


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
    """
    tickers = [f"{s}.NS" for s in all_symbols]
    global_max, conservative_min = db.get_latest_ohlcv_date()

    if global_max is None:
        spinner_msg = f"🌐 First run: downloading full history for {len(tickers)} stocks..."
        fetch_kwargs = {"period": HISTORY_PERIOD}
    else:
        if target_date and global_max >= target_date:
            return True
        fetch_from = (datetime.strptime(conservative_min, "%Y-%m-%d") - timedelta(days=3)).strftime("%Y-%m-%d")
        today = datetime.now(IST).strftime("%Y-%m-%d")
        spinner_msg = f"🔄 Incremental update: fetching data since {fetch_from}..."
        fetch_kwargs = {"start": fetch_from, "end": today}

    try:
        with st.spinner(spinner_msg):
            raw = yf.download(
                tickers, group_by="ticker",
                threads=True, progress=False, auto_adjust=True,
                **fetch_kwargs
            )
    except Exception as e:
        st.error(f"Yahoo Finance Error: {e}")
        return False

    if raw is None or raw.empty:
        return False

    records = []
    available = raw.columns.get_level_values(0).unique().tolist() if isinstance(raw.columns, pd.MultiIndex) else tickers

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
                    "open":   float(row.get("Open", 0) or 0),
                    "high":   float(row.get("High", 0) or 0),
                    "low":    float(row.get("Low", 0) or 0),
                    "close":  float(row["Close"]),
                    "volume": int(row.get("Volume", 0) or 0),
                })
        except Exception:
            continue

    if records:
        with st.spinner(f"💾 Saving {len(records):,} rows to database..."):
            db.upsert_ohlcv(records)
    return True


def _score_from_db(constituents: dict, for_momentum: bool, rsi_filter: bool) -> pd.DataFrame:
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
                res["Index"] = next((idx for idx, syms in constituents.items() if sym in syms), "Unknown")
                results.append(res)
        except Exception:
            continue

    df = pd.DataFrame(results)
    if df.empty:
        return df
    if not for_momentum and rsi_filter:
        df = df[(df["RSI"] >= 50) & (df["RSI"] <= 70)]
    return df.sort_values("Score" if not for_momentum else "Close", ascending=False)


# ──────────────────────────────────────────────
# 3-TIER CACHE  (Memory → DB → Internet)
# ──────────────────────────────────────────────
_mem_cache: dict[str, dict] = {
    "stage2":   {"date": None, "data": None},
    "momentum": {"date": None, "data": None, "ts": None},
}


def _get_target_key() -> str:
    """Return the last valid trading date string to use as the cache key (after-market cutoff at 19:00 IST)."""
    now = datetime.now(IST)
    start = now.strftime("%Y-%m-%d") if now.hour >= 19 else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return get_last_valid_trading_date(start, load_nse_holidays())


def resolve_screener_data(rsi_filter: bool, for_momentum: bool = False, universe: str = None):
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
    all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))

    if for_momentum:
        mc = _mem_cache["momentum"]
        now = datetime.now()

        if (mc["data"] is not None and mc["date"] == target_key and
                mc["ts"] and (now - mc["ts"]).total_seconds() < _MOMENTUM_TTL):
            return mc["data"], target_key, "memory"

        _sync_ohlcv_to_db(all_symbols, target_date=target_key)
        df = _score_from_db(constituents, for_momentum=True, rsi_filter=False)

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
            df = _score_from_db(constituents, for_momentum=False, rsi_filter=rsi_filter)
            if not df.empty:
                db.save_stage2_cache(target_key, df)
                _mem_cache["stage2"] = {"date": target_key, "data": df}
                return df, target_key, "internet"

        fallback_df, fallback_date = db.load_latest_stage2_cache()
        if fallback_df is not None:
            return fallback_df, fallback_date, "db"

        return pd.DataFrame(), target_key, "error"
