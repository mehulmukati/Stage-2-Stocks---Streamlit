#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Stage 2 Breakout Screener — Nifty Total Market (750)     ║
║   7-Point Weinstein Scoring | Full-Universe Daily Cache    ║
║   DATA: constituents.json | HOLIDAYS: nse_holidays.json    ║
╚══════════════════════════════════════════════════════════════╝
"""
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore")

load_dotenv()
import db

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")
HISTORY_PERIOD = "2y"
HISTORY_PERIOD_MOMENTUM = "18mo"  # Changed from "1y" to ensure we get >= 250 trading days for momentum calculations
MIN_VOLUME = 100_000
VOL_AVG_PERIOD = 10          # Configurable: 10, 20, or 50 days (Change here)
HH_HL_LOOKBACK = 50          # Change here if needed
MA_RISING_LOOKBACK = 50      # Change here if needed

# ──────────────────────────────────────────────
# CIRCUIT LEVELS
# ──────────────────────────────────────────────
CIRCUIT_LEVELS = [5.0, 10.0, 20.0]	# removed 2.0
CIRCUIT_TOLERANCE = 0.1  # ±0.1% tolerance

# ──────────────────────────────────────────────
# HOLIDAY & TRADING DAY RESOLVER
# ──────────────────────────────────────────────
def load_nse_holidays() -> set:
    path = os.path.join(os.path.dirname(__file__), "nse_holidays.json")
    if not os.path.exists(path): return set()
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
    """Loops backward from start_date until a valid weekday (non-holiday) is found."""
    dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    for _ in range(10):  # Safety cap: max 10 days back
        if dt.weekday() < 5 and dt.strftime("%Y-%m-%d") not in holidays:
            return dt.strftime("%Y-%m-%d")
        dt -= timedelta(days=1)
    return start_date_str  # Fallback

# ──────────────────────────────────────────────
# DB INIT (runs once at startup)
# ──────────────────────────────────────────────
@st.cache_resource
def _init_db():
    db.init_db()

_init_db()

# ──────────────────────────────────────────────
# PURE PYTHON SCORING ENGINE
# ──────────────────────────────────────────────
def _rsi_wilder(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def score_stage2(df: pd.DataFrame) -> dict | None:
    if len(df) < 250: return None
    c, h, l, v = df["Close"], df["High"], df["Low"], df["Volume"]
    ma50 = c.rolling(50).mean()
    ma150 = c.rolling(150).mean()
    ma200 = c.rolling(200).mean()

    # Use configurable VOL_AVG_PERIOD for average volume
    avg_vol = v.rolling(VOL_AVG_PERIOD).mean()

    rsi = _rsi_wilder(c)

    c1, h1, l1, v1 = c.iloc[-1], h.iloc[-1], l.iloc[-1], v.iloc[-1]
    m50, m150, m200 = ma50.iloc[-1], ma150.iloc[-1], ma200.iloc[-1]
    r = rsi.iloc[-1]

    # Volume Ratio uses the same configurable average
    vr = v1 / avg_vol.iloc[-1] if avg_vol.iloc[-1] > 0 else 0

    if np.isnan([m50, m150, m200, vr, r]).any(): return None

    score = 0
    if vr >= 2.0: score += 1
    if h1 >= h.rolling(HH_HL_LOOKBACK).max().shift(1).iloc[-1]: score += 1
    if l1 >= l.rolling(HH_HL_LOOKBACK).min().shift(1).iloc[-1]: score += 1
    if c1 > m50 and ma50.iloc[-1] > ma50.iloc[-MA_RISING_LOOKBACK]: score += 1
    if c1 > m200 and ma200.iloc[-1] > ma200.iloc[-MA_RISING_LOOKBACK]: score += 1
    if c1 > m150: score += 1
    if m50 > m150 > m200: score += 1

    if score >= 6: stage = "🟢 Strong Stage 2"
    elif score >= 4: stage = "🟡 Likely Stage 2"
    elif score >= 2: stage = "🟠 Early/Weak Stage 2"
    else: stage = "⚪ Not Stage 2"

    return {
        "Score": score, "Stage": stage,
        # Illiquid check now uses Average Volume over VOL_AVG_PERIOD
        "Illiquid": avg_vol.iloc[-1] < MIN_VOLUME,
        "Close": round(c1, 2), "Volume": int(v1), "Vol_Ratio": round(vr, 2),
        "RSI": round(r, 1), "MA50": round(m50, 2), "MA150": round(m150, 2),
        "MA200": round(m200, 2), "MA_Stack": m50 > m150 > m200,
        "Avg_Vol": int(np.floor(avg_vol.iloc[-1]))
    }


# ──────────────────────────────────────────────
# MOMENTUM SCREENER FUNCTIONS
# ──────────────────────────────────────────────
def _count_circuits(df: pd.DataFrame) -> int:
    """Count number of days where price change equals common circuit levels ± tolerance."""
    if len(df) < 2:
        return 0
    pct_change = df["Close"].pct_change() * 100
    circuit_count = 0
    for level in CIRCUIT_LEVELS:
        upper = (pct_change >= level - CIRCUIT_TOLERANCE) & (pct_change <= level + CIRCUIT_TOLERANCE)
        lower = (pct_change <= -level - CIRCUIT_TOLERANCE) & (pct_change >= -level + CIRCUIT_TOLERANCE)
        circuit_count += (upper | lower).sum()
    return int(circuit_count)


def _calculate_sharpe(df: pd.DataFrame, period_days: int) -> float | None:
    """Calculate Sharpe ratio (RoC/SD) for a given period based on exact number of trading days."""
    if len(df) < period_days:
        return None
    subset = df.tail(period_days)
    daily_returns = subset["Close"].pct_change().dropna()
    if len(daily_returns) == 0 or daily_returns.std() == 0:
        return None
    # Annualized RoC and SD
    total_return = (subset["Close"].iloc[-1] / subset["Close"].iloc[0]) - 1
    trading_days_in_year = 252
    annualized_roc = ((1 + total_return) ** (trading_days_in_year / len(daily_returns))) - 1
    annualized_sd = daily_returns.std() * np.sqrt(trading_days_in_year)
    if annualized_sd == 0:
        return None
    return annualized_roc / annualized_sd


def _calculate_positive_days_pct(df: pd.DataFrame, months: int) -> float | None:
    """Calculate percentage of positive days (close > prev close) in given months."""
    days_approx = int(months * 21)  # ~21 trading days per month
    if len(df) < days_approx:
        return None
    subset = df.tail(days_approx)
    positive_days = (subset["Close"].diff() > 0).sum()
    total_days = len(subset) - 1
    if total_days == 0:
        return None
    return (positive_days / total_days) * 100


def score_momentum(df: pd.DataFrame) -> dict | None:
    """Calculate momentum metrics for a stock."""
    if len(df) < 250:
        return None
    
    c = df["Close"]
    v = df["Volume"]
    h = df["High"]
    
    # Basic metrics
    close = c.iloc[-1]
    high_52w = h.rolling(252).max().iloc[-1]
    dma100 = c.rolling(100).mean().iloc[-1]
    dma200 = c.rolling(200).mean().iloc[-1]

    # Volume median (1 year)
    vol_median = v.rolling(252).median().iloc[-1]

    # 1Y Change
    if len(c) >= 252:
        one_yr_change = ((c.iloc[-1] / c.iloc[-252]) - 1) * 100
    else:
        one_yr_change = None

    # % from 52W High
    pct_from_52w_high = ((close - high_52w) / high_52w) * 100 if high_52w else None

    # Circuit count (1 year)
    circuit_count = _count_circuits(df)

    # Sharpe ratios for different periods
    sharpe_3m = _calculate_sharpe(df, 63)   # ~3 months
    sharpe_6m = _calculate_sharpe(df, 126)  # ~6 months
    sharpe_9m = _calculate_sharpe(df, 189)  # ~9 months
    sharpe_1y = _calculate_sharpe(df, 252)  # ~12 months

    # Volatility (SD) - annualized
    daily_returns = c.pct_change().dropna()
    volatility = daily_returns.std() * np.sqrt(252) if len(daily_returns) > 0 else None

    # Positive days percentages
    pos_days_3m = _calculate_positive_days_pct(df, 3)
    pos_days_6m = _calculate_positive_days_pct(df, 6)
    pos_days_12m = _calculate_positive_days_pct(df, 12)
    
    return {
        "Close": round(close, 2),
        "52w_High": round(high_52w, 2) if high_52w else None,
        "DMA100": round(dma100, 2),
        "DMA200": round(dma200, 2),
        "Vol_Median": int(vol_median) if vol_median else None,
        "1Y_Change": round(one_yr_change, 2) if one_yr_change else None,
        "Pct_From_52W_High": round(pct_from_52w_high, 2) if pct_from_52w_high else None,
        "Circuit_Count": circuit_count,
        "Sharpe_3M": round(sharpe_3m, 3) if sharpe_3m else None,
        "Sharpe_6M": round(sharpe_6m, 3) if sharpe_6m else None,
        "Sharpe_9M": round(sharpe_9m, 3) if sharpe_9m else None,
        "Sharpe_1Y": round(sharpe_1y, 3) if sharpe_1y else None,
        "Volatility": round(volatility * 100, 1) if volatility else None,  # Convert to percentage
        "Pos_Days_3M": round(pos_days_3m, 0) if pos_days_3m else None,
        "Pos_Days_6M": round(pos_days_6m, 0) if pos_days_6m else None,
        "Pos_Days_12M": round(pos_days_12m, 0) if pos_days_12m else None,
    }

# ──────────────────────────────────────────────
# FETCH & CACHE ORCHESTRATOR (FULL UNIVERSE)
# ──────────────────────────────────────────────
def _get_universe_symbols(universe: str, constituents: dict) -> list:
    """Get list of symbols for a given universe selection."""
    base_indices = {
        "Nifty 50": ["Nifty 50"],
        "Nifty Next 50": ["Nifty Next 50"],
        "Nifty Midcap 150": ["Nifty Midcap 150"],
        "Nifty Smallcap 250": ["Nifty Smallcap 250"],
        "Nifty Microcap 250": ["Nifty Microcap 250"],
        "Nifty LargeMidcap 250": ["Nifty 50", "Nifty Next 50", "Nifty Midcap 150"],
        "Nifty MidSmallcap 400": ["Nifty Midcap 150", "Nifty Smallcap 250"],
        "Nifty Total Market": ["Nifty 50", "Nifty Next 50", "Nifty Midcap 150", "Nifty Smallcap 250", "Nifty Microcap 250"],
    }
    indices_to_include = base_indices.get(universe, [universe])
    symbols = []
    for idx in indices_to_include:
        if idx in constituents:
            symbols.extend(constituents[idx])
    return list(dict.fromkeys(symbols))  # Remove duplicates while preserving order


def _load_constituents() -> dict:
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        st.error("❌ `constituents.json` missing.")
        return {}
    with open(const_path, "r") as f:
        return json.load(f)


def _sync_ohlcv_to_db(all_symbols: list[str], target_date: str = None) -> bool:
    """
    Incremental sync: fetch only missing dates from yfinance and upsert to DB.
    Returns True if data is available in DB (either already fresh or after fetching).
    target_date: last valid NSE trading day — skip fetch if DB is already up to this date.
    """
    tickers = [f"{s}.NS" for s in all_symbols]
    global_max, conservative_min = db.get_latest_ohlcv_date()

    if global_max is None:
        # First run — always fetch the longer period so both screeners have full history
        spinner_msg = f"🌐 First run: downloading full history for {len(tickers)} stocks..."
        fetch_kwargs = {"period": HISTORY_PERIOD}  # 2y — superset of momentum's 18mo
    else:
        # Skip fetch if the most recently synced date already covers the target trading day
        if target_date and global_max >= target_date:
            return True
        # Use conservative_min as fetch start to fill any per-symbol gaps
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

    # Parse raw into upsert records
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
    """Load OHLCV from DB, run scoring, return results DataFrame."""
    period_days = 550 if for_momentum else 750  # enough for 200-day MA + buffer
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
# Tier 1 — module-level in-memory store, keyed by trading date
_mem_cache: dict[str, dict] = {
    "stage2":   {"date": None, "data": None},
    "momentum": {"date": None, "data": None, "ts": None},
}
_MOMENTUM_TTL = 3600  # seconds before in-memory momentum data is considered stale


def _get_target_key() -> str:
    """Return the last valid NSE trading date string (respects 7 PM IST cutoff)."""
    now = datetime.now(IST)
    start = now.strftime("%Y-%m-%d") if now.hour >= 19 else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return get_last_valid_trading_date(start, load_nse_holidays())


def resolve_screener_data(rsi_filter: bool, for_momentum: bool = False, universe: str = None):
    """
    3-tier resolution for both screeners:
      Tier 1 — in-memory (same process, keyed by trading date / TTL)
      Tier 2 — SQLite/PostgreSQL (persists across restarts)
      Tier 3 — yfinance internet fetch (only when DB is stale)
    Returns (df, date_str, source) where source is 'memory' | 'db' | 'internet'.
    """
    target_key = _get_target_key()
    constituents = _load_constituents()
    if not constituents:
        return pd.DataFrame(), target_key, "error"
    all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))

    if for_momentum:
        mc = _mem_cache["momentum"]
        now = datetime.now()

        # Tier 1 — memory: same trading day AND within TTL
        if (mc["data"] is not None and mc["date"] == target_key and
                mc["ts"] and (now - mc["ts"]).total_seconds() < _MOMENTUM_TTL):
            return mc["data"], target_key, "memory"

        # Tier 2 — DB: OHLCV already fresh → score without internet
        # Tier 3 — Internet: OHLCV stale → fetch → upsert → score
        _sync_ohlcv_to_db(all_symbols, target_date=target_key)
        df = _score_from_db(constituents, for_momentum=True, rsi_filter=False)

        source = "db" if (mc["data"] is None or mc["date"] != target_key) else "memory"
        if not df.empty:
            _mem_cache["momentum"] = {"date": target_key, "data": df, "ts": now}
        return df, target_key, source

    else:
        mc = _mem_cache["stage2"]

        # Tier 1 — memory: same trading date
        if mc["data"] is not None and mc["date"] == target_key:
            return mc["data"], target_key, "memory"

        # Tier 2 — DB scored-results cache
        cached_df = db.load_stage2_cache(target_key)
        if cached_df is not None:
            _mem_cache["stage2"] = {"date": target_key, "data": cached_df}
            return cached_df, target_key, "db"

        # Tier 3 — Internet: sync OHLCV → score → persist
        synced = _sync_ohlcv_to_db(all_symbols, target_date=target_key)
        if synced:
            df = _score_from_db(constituents, for_momentum=False, rsi_filter=rsi_filter)
            if not df.empty:
                db.save_stage2_cache(target_key, df)
                _mem_cache["stage2"] = {"date": target_key, "data": df}
                return df, target_key, "internet"

        # Fallback — most recent DB cache (market closed / fetch failed)
        fallback_df, fallback_date = db.load_latest_stage2_cache()
        if fallback_df is not None:
            return fallback_df, fallback_date, "db"

        return pd.DataFrame(), target_key, "error"


def get_momentum_full_universe_data():
    """Thin wrapper — momentum UI calls this; 3-tier logic lives in resolve_screener_data."""
    df, _, _ = resolve_screener_data(rsi_filter=False, for_momentum=True)
    return df

# ──────────────────────────────────────────────
# STREAMLIT UI
# ──────────────────────────────────────────────
st.set_page_config(page_title="Stock Screeners | Nifty 750", page_icon="📈", layout="wide")
st.markdown("""
<style>
.sb-head { font-weight: 700; margin-bottom: 0.5rem; font-size: 0.95rem; }
.hero { text-align: center; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
.sub-hero { text-align: center; color: #64748b; margin-top: -8px; }
/* Make tabs full width at top */
.stTabs [data-baseweb="tab-list"] { gap: 2px; width: 100%; justify-content: stretch; }
.stTabs [data-baseweb="tab"] { flex-grow: 1; width: 100%; max-width: none; }
.stTabs { width: 100%; }
/* Sidebar container styling */
.screener-sidebar { 
    background-color: #f8f9fa; 
    padding: 1rem; 
    border-radius: 8px;
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)


def stage2_screener_ui():
    """UI for Stage 2 Screener"""
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">📊 Nifty Total Market Stage 2 Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">EOD Analysis · 7-Point Weinstein Score · {now_ist}</p>', unsafe_allow_html=True)

    # ── CONTROL PANEL (Batched) - Separate sidebar for this tab ──
    col_sidebar, col_main = st.columns([1, 4])
    
    with col_sidebar:
        st.markdown('<div class="screener-sidebar">', unsafe_allow_html=True)
        st.markdown('<p class="sb-head">🔍 Filters</p>', unsafe_allow_html=True)
        rsi_toggle = st.toggle("Filter: RSI between 50–70", value=False, key="stage2_rsi_toggle")
        show_illiquid = st.toggle("Show Illiquid Stocks (Avg Vol < 1L)", value=False, key="stage2_show_illiquid")

        st.markdown("---")
        st.markdown('<p class="sb-head">📦 Select Indices</p>', unsafe_allow_html=True)

        const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
        idx_options = list(json.load(open(const_path, "r")).keys()) if os.path.exists(const_path) else []

        cols = st.columns(2)
        selected_indices = []
        for i, idx in enumerate(idx_options):
            default_checked = idx in ["Nifty 50", "Nifty Next 50", "Nifty Midcap 150", "Nifty Smallcap 250", "Nifty Microcap 250"]
            if cols[i % 2].checkbox(idx, value=default_checked, key=f"stage2_idx_{idx}"):
                selected_indices.append(idx)

        run_btn = st.button("🚀 Apply Filters & Show", type="primary", use_container_width=True, key="stage2_run_btn")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_main:
        if "stage2_run_triggered" not in st.session_state and run_btn:
            st.session_state["stage2_run_triggered"] = True

        if not st.session_state.get("stage2_run_triggered"):
            st.info("👈 Select indices/filters and click **Apply Filters & Show** to begin.")
            return

        # ── RESOLVE DATA  (Memory → DB → Internet) ──
        df, cache_date, source = resolve_screener_data(rsi_toggle, for_momentum=False)

        if df.empty:
            st.warning(f"📅 No data available for **{cache_date}**. Yahoo Finance may be syncing. Try again in 30 mins.")
            return

        if source == "memory":
            st.success(f"⚡ Served from memory cache for **{cache_date}**.")
        elif source == "db":
            st.info(f"💾 Loaded from local database for **{cache_date}**.")
        elif source == "internet":
            st.success(f"🌐 Fetched fresh EOD data and saved to database for **{cache_date}**.")

        # ── APPLY UI FILTERS LOCALLY (Instant) ──
        display_df = df.copy()
        if selected_indices:
            display_df = display_df[display_df["Index"].isin(selected_indices)]
        if rsi_toggle:
            display_df = display_df[(display_df["RSI"] >= 50) & (display_df["RSI"] <= 70)]
        if not show_illiquid:
            display_df = display_df[~display_df["Illiquid"]]

        if display_df.empty:
            st.warning("No stocks match the selected filters. Adjust criteria or show illiquid stocks.")
            return

        # Text-based ILLIQ indicator
        display_df["Symbol"] = display_df.apply(
            lambda r: f"{r['Symbol']} 🚩 ILLIQ" if r['Illiquid'] else r['Symbol'], axis=1
        )

        # EXPLICIT COLUMN ORDER: Ticker, Source, Classification, Score, Close, Vol, Avg Vol, Vol Ratio, RSI
        display_cols = ["Symbol", "Index", "Stage", "Score", "Close", "Volume", "Avg_Vol", "Vol_Ratio", "RSI"]
        display_df = display_df[display_cols]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Cache Date", cache_date)
        c2.metric("Total Universe", len(df))
        c3.metric("Matches (Filters)", len(display_df))
        c4.metric("Strong Stage 2", len(display_df[display_df["Score"] >= 6]))

        # Row Coloring Logic
        def color_rows(row):
            bg_map = {
                "🟢 Strong Stage 2": "#ecfdf5",
                "🟡 Likely Stage 2": "#fefce8",
                "🟠 Early/Weak Stage 2": "#fef2f2",
                "⚪ Not Stage 2": "#f9fafb"
            }
            return [f'background-color: {bg_map.get(row["Stage"], "#ffffff")}'] * len(row)

        styled_df = display_df.style.apply(color_rows, axis=1)

        # Render Table - Fixed deprecation
        st.dataframe(
            styled_df,
            width="stretch",
            hide_index=True,
            column_config={
                "Symbol": st.column_config.TextColumn("Ticker", width="medium"),
                "Index": st.column_config.TextColumn("Source", width="medium"),
                "Stage": st.column_config.TextColumn("Classification", width="medium"),
                "Score": st.column_config.NumberColumn("Score", format="%d/7", width="small"),
                "Close": st.column_config.NumberColumn("Close (₹)", format="%.2f", width="small"),
                "Volume": st.column_config.NumberColumn("Volume", format="%,d", width="small"),
                "Avg_Vol": st.column_config.NumberColumn("Avg Vol (10d)", format="%,d", width="small"),
                "Vol_Ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2f x", width="small"),
                "RSI": st.column_config.NumberColumn("RSI(14)", format="%.1f", width="small")
            },
            height=650
        )

        # Export CSV
        csv = display_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download Screener Results", csv,
            file_name=f"stage2_screener_{datetime.now(IST).strftime('%Y%m%d')}.csv",
            mime="text/csv",
            width="stretch"
        )


def _calculate_avg_sharpe(row, method: str) -> float | None:
    """Calculate average Sharpe ratio based on sorting method."""
    sharpes = []
    if method in ["1 year", "1Y"]:
        return row.get("Sharpe_1Y")
    elif method in ["3 months", "3M"]:
        return row.get("Sharpe_3M")
    elif method in ["6 months", "6M"]:
        return row.get("Sharpe_6M")
    elif method in ["9 months", "9M"]:
        return row.get("Sharpe_9M")
    elif method == "Average of 3/6/9/12 months":
        for k in ["Sharpe_3M", "Sharpe_6M", "Sharpe_9M", "Sharpe_1Y"]:
            v = row.get(k)
            if v is not None:
                sharpes.append(v)
        return sum(sharpes) / len(sharpes) if sharpes else None
    elif method == "Average of 3/6 months":
        for k in ["Sharpe_3M", "Sharpe_6M"]:
            v = row.get(k)
            if v is not None:
                sharpes.append(v)
        return sum(sharpes) / len(sharpes) if sharpes else None
    return None


def momentum_screener_ui():
    """UI for Momentum Screener"""
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">🚀 Momentum Stock Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">Sharpe Ratio Based Momentum Analysis · {now_ist}</p>', unsafe_allow_html=True)

    # ── CONTROL PANEL (Batched) - Separate sidebar for this tab ──
    col_sidebar, col_main = st.columns([1, 4])
    
    with col_sidebar:
        st.markdown('<div class="screener-sidebar">', unsafe_allow_html=True)
        st.markdown('<p class="sb-head">🔍 Filters</p>', unsafe_allow_html=True)

        # Universe Selection
        universe_options = [
            "Nifty 50", "Nifty Next 50", "Nifty Midcap 150", "Nifty Smallcap 250", "Nifty Microcap 250",
            "Nifty LargeMidcap 250", "Nifty MidSmallcap 400", "Nifty Total Market"
        ]
        selected_universe = st.selectbox("Choosing Universe", options=universe_options, index=7, key="mom_universe")

        # Minimum Annual Return
        min_annual_return = st.number_input("Minimum Annual Return (%)", min_value=0.0, max_value=1000.0, value=0.0, step=0.01, format="%.2f", key="mom_min_annual_return")

        # DMA Filters
        col1, col2 = st.columns(2)
        with col1:
            close_above_100dma = st.checkbox("Close > 100 DMA", value=False, key="mom_close_above_100dma")
        with col2:
            close_above_200dma = st.checkbox("Close > 200 DMA", value=False, key="mom_close_above_200dma")

        # 52W High Filter
        pct_from_52w_high = st.number_input("Last Close / 52w High (within %)", min_value=0, max_value=100, value=25, step=1, key="mom_pct_from_52w_high")

        # Max Circuits
        max_circuits = st.number_input("Max Circuits (past 1 year)", min_value=0, max_value=100, value=18, step=1, key="mom_max_circuits")

        # Positive Days
        st.markdown('<p class="sb-head" style="margin-top: 1rem;">Positive Days (%)</p>', unsafe_allow_html=True)
        col3, col4, col5 = st.columns(3)
        with col3:
            pos_days_3m = st.number_input("3 Months", min_value=0, max_value=100, value=0, step=1, key="mom_pos_days_3m")
        with col4:
            pos_days_6m = st.number_input("6 Months", min_value=0, max_value=100, value=0, step=1, key="mom_pos_days_6m")
        with col5:
            pos_days_12m = st.number_input("12 Months", min_value=0, max_value=100, value=0, step=1, key="mom_pos_days_12m")

        # Sorting Method
        sort_options = [
            "1 year", "3 months", "6 months", "9 months",
            "Average of 3/6/9/12 months", "Average of 3/6 months"
        ]
        sort_method = st.selectbox("Sorting Method (Sharpe Ratio)", options=sort_options, index=4, key="mom_sort_method")

        run_btn = st.button("🚀 Run Momentum Screener", type="primary", use_container_width=True, key="mom_run_btn")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_main:
        # Only fetch data when the Run button is clicked
        if not run_btn:
            st.info("👈 Set your filters and click **Run Momentum Screener** to begin.")
            return
        
        # ── RESOLVE DATA  (Memory → DB → Internet) ──
        full_df, cache_date, source = resolve_screener_data(rsi_filter=False, for_momentum=True)

        if full_df.empty:
            st.warning("📅 No data available. This could be due to:\n\n1. Yahoo Finance API returning no data\n2. Market holiday/weekend\n3. Invalid symbols in constituents.json\n\nTry again in a few minutes or check your internet connection.")
            return

        if source == "memory":
            st.success(f"⚡ Served from memory cache for **{cache_date}** · {len(full_df)} stocks")
        elif source == "db":
            st.info(f"💾 Loaded from local database for **{cache_date}** · {len(full_df)} stocks")
        elif source == "internet":
            st.success(f"🌐 Fetched fresh EOD data and saved to database for **{cache_date}** · {len(full_df)} stocks")

        # ── APPLY UNIVERSE FILTER ──
        display_df = full_df[full_df["Index"].isin([selected_universe])] if selected_universe != "Nifty Total Market" else full_df.copy()

        # ── APPLY FILTERS ──
        # Minimum Annual Return filter (using 1Y_Change as proxy for annual return)
        if min_annual_return > 0:
            display_df = display_df[display_df["1Y_Change"].notna() & (display_df["1Y_Change"] >= min_annual_return)]

        # Close > 100 DMA
        if close_above_100dma:
            display_df = display_df[display_df["Close"] > display_df["DMA100"]]

        # Close > 200 DMA
        if close_above_200dma:
            display_df = display_df[display_df["Close"] > display_df["DMA200"]]

        # 52W High Filter: last close should be within X% of 52W high
        # e.g., if 25% entered, include stocks where close > 0.75 * 52W high
        threshold_multiplier = (100 - pct_from_52w_high) / 100
        display_df = display_df[display_df["Close"] >= (threshold_multiplier * display_df["52w_High"])]

        # Max Circuits
        display_df = display_df[display_df["Circuit_Count"] <= max_circuits]

        # Positive Days filters
        if pos_days_3m > 0:
            display_df = display_df[display_df["Pos_Days_3M"].notna() & (display_df["Pos_Days_3M"] >= pos_days_3m)]
        if pos_days_6m > 0:
            display_df = display_df[display_df["Pos_Days_6M"].notna() & (display_df["Pos_Days_6M"] >= pos_days_6m)]
        if pos_days_12m > 0:
            display_df = display_df[display_df["Pos_Days_12M"].notna() & (display_df["Pos_Days_12M"] >= pos_days_12m)]

        if display_df.empty:
            st.warning("No stocks match the selected filters. Adjust criteria and try again.")
            return

        # ── CALCULATE SHARPE FOR SORTING ──
        display_df["Avg_Sharpe"] = display_df.apply(lambda row: _calculate_avg_sharpe(row, sort_method), axis=1)
        display_df = display_df[display_df["Avg_Sharpe"].notna()]

        if display_df.empty:
            st.warning("No stocks have valid Sharpe ratios for the selected sorting method.")
            return

        # Sort by Sharpe descending
        display_df = display_df.sort_values("Avg_Sharpe", ascending=False)

        # ── PREPARE OUTPUT COLUMNS ──
        output_cols = [
            "Symbol", "Index", "Close", "Avg_Sharpe", "Volatility", "52w_High",
            "Vol_Median", "1Y_Change", "Pct_From_52W_High", "Circuit_Count"
        ]
        display_df = display_df[output_cols]

        # Rename for display
        display_df = display_df.rename(columns={
            "Avg_Sharpe": "Sharpe",
            "Vol_Median": "Median Vol",
            "1Y_Change": "1Y Change",
            "Pct_From_52W_High": "% from 52wH",
            "Circuit_Count": "Circuit Close"
        })

        # ── METRICS ──
        c1, c2, c3 = st.columns(3)
        c1.metric("Universe", selected_universe)
        c2.metric("Total in Universe", len(full_df))
        c3.metric("Matches (Filters)", len(display_df))

        # ── RENDER TABLE ──
        st.dataframe(
            display_df,
            width="stretch",
            hide_index=True,
            column_config={
                "Symbol": st.column_config.TextColumn("Symbol", width="medium"),
                "Index": st.column_config.TextColumn("Index", width="medium"),
                "Close": st.column_config.NumberColumn("Close (₹)", format="%.2f", width="small"),
                "Sharpe": st.column_config.NumberColumn("Sharpe", format="%.3f", width="small"),
                "Volatility": st.column_config.NumberColumn("Volatility (%)", format="%.1f%%", width="small"),
                "52w_High": st.column_config.NumberColumn("52w High", format="%.2f", width="small"),
                "Median Vol": st.column_config.NumberColumn("Median Vol", format="%,d", width="small"),
                "1Y Change": st.column_config.NumberColumn("1Y Change", format="%.2f%%", width="small"),
                "% from 52wH": st.column_config.NumberColumn("% from 52wH", format="%.2f%%", width="small"),
                "Circuit Close": st.column_config.NumberColumn("Circuit Close", format="%d", width="small")
            },
            height=650
        )

        # Export CSV
        csv = display_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download Momentum Screener Results", csv,
            file_name=f"momentum_screener_{datetime.now(IST).strftime('%Y%m%d')}.csv",
            mime="text/csv",
            width="stretch"
        )


def main():
    # Create tabs for Stage 2 and Momentum screeners
    tab1, tab2 = st.tabs(["📊 Stage 2 Screener", "🚀 Momentum Screener"])

    with tab1:
        stage2_screener_ui()

    with tab2:
        momentum_screener_ui()


if __name__ == "__main__":
    main()