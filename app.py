#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Stage 2 Breakout Screener — Nifty Total Market (750)     ║
║   7-Point Weinstein Scoring | Smart EOD Cache & Fetch Logic║
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
import warnings
warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")
HISTORY_PERIOD = "2y"
MIN_VOLUME = 100_000
VOL_AVG_PERIOD = 10          # Average volume period: 10/20/50 days
HH_HL_LOOKBACK = 50          # Change here if needed
MA_RISING_LOOKBACK = 50      # Change here if needed
RESULT_CACHE_DIR = "daily_cache"
os.makedirs(RESULT_CACHE_DIR, exist_ok=True)

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
# CACHE UTILS
# ──────────────────────────────────────────────
def load_json_cache(key: str) -> pd.DataFrame | None:
    path = os.path.join(RESULT_CACHE_DIR, f"{key}.json")
    try:
        if os.path.exists(path):
            df = pd.read_json(path, orient="records")
            return df if not df.empty else None
    except Exception:
        return None
    return None

def save_json_cache(df: pd.DataFrame, key: str):
    path = os.path.join(RESULT_CACHE_DIR, f"{key}.json")
    df.to_json(path, orient="records")

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
    avg_vol = v.rolling(VOL_AVG_PERIOD).mean()
    rsi = _rsi_wilder(c)

    c1, h1, l1, v1 = c.iloc[-1], h.iloc[-1], l.iloc[-1], v.iloc[-1]
    m50, m150, m200 = ma50.iloc[-1], ma150.iloc[-1], ma200.iloc[-1]
    r = rsi.iloc[-1]
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
        "Illiquid": avg_vol.iloc[-1] < MIN_VOLUME,
        "Close": round(c1, 2), "Volume": int(v1), "Vol_Ratio": round(vr, 2),
        "RSI": round(r, 1), "MA50": round(m50, 2), "MA150": round(m150, 2), 
        "MA200": round(m200, 2), "MA_Stack": m50 > m150 > m200
    }

# ──────────────────────────────────────────────
# FETCH & CACHE ORCHESTRATOR
# ──────────────────────────────────────────────
def fetch_and_score_universe(selected_indices: list[str], rsi_filter: bool) -> tuple[pd.DataFrame, int]:
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        st.error("❌ `constituents.json` missing.")
        return pd.DataFrame(), 0
    with open(const_path, "r") as f:
        constituents = json.load(f)

    symbols = []
    for idx in selected_indices:
        symbols.extend(constituents.get(idx, []))
    symbols = list(dict.fromkeys(symbols))
    tickers = [f"{s}.NS" for s in symbols]

    try:
        with st.spinner("🌐 Fetching EOD data..."):
            raw = yf.download(tickers, period=HISTORY_PERIOD, group_by="ticker", 
                              threads=True, progress=False, auto_adjust=True)
    except Exception as e:
        st.error(f"Yahoo Finance Error: {e}")
        return pd.DataFrame(), 0

    results = []
    for t in tickers:
        sym = t.replace(".NS", "")
        try:
            sub = raw[t].dropna(how="all") if len(tickers) > 1 else raw.dropna(how="all")
            sub.columns = [c[0] if isinstance(c, tuple) else c for c in sub.columns]
            res = score_stage2(sub)
            if res:
                res["Symbol"] = sym
                res["Index"] = next((idx for idx in selected_indices if sym in constituents.get(idx, [])), "Unknown")
                results.append(res)
        except: continue

    df = pd.DataFrame(results)
    if df.empty: return pd.DataFrame(), 0
    if rsi_filter: df = df[(df["RSI"] >= 50) & (df["RSI"] <= 70)]
    return df.sort_values("Score", ascending=False), len(df)

def resolve_screener_data(selected_indices: list[str], rsi_filter: bool):
    """Implements exact logic: Time-based target → Find valid trading day → Cache/Fetch."""
    now = datetime.now(IST)
    
    # 1. Determine starting date based on 7 PM cutoff
    start_date = now.strftime("%Y-%m-%d") if now.hour >= 19 else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 2. Find last valid trading day (handles weekends/holidays)
    holidays = load_nse_holidays()
    target_key = get_last_valid_trading_date(start_date, holidays)
    
    # 3. Check cache first
    df = load_json_cache(target_key)
    if df is not None:
        return df, target_key, True  # (data, date, is_cached)
        
    # 4. Cache miss → Fetch
    df, valid_count = fetch_and_score_universe(selected_indices, rsi_filter)
    if not df.empty:
        save_json_cache(df, target_key)
        return df, target_key, False
        
    # 5. Fetch failed or returned empty → Look for any older cache
    try:
        files = sorted([f.replace(".json", "") for f in os.listdir(RESULT_CACHE_DIR) if f.endswith(".json")], reverse=True)
        for f in files:
            if f <= target_key:
                df = load_json_cache(f)
                if df is not None:
                    return df, f, True
    except Exception:
        pass
        
    return pd.DataFrame(), target_key, True  # Fallback to empty if absolutely nothing exists

# ──────────────────────────────────────────────
# STREAMLIT UI
# ──────────────────────────────────────────────
st.set_page_config(page_title="Stage 2 Screener | Nifty 750", page_icon="📈", layout="wide")
st.markdown("""
<style>
.sb-head { font-weight: 700; margin-bottom: 0.5rem; font-size: 0.95rem; }
.hero { text-align: center; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
.sub-hero { text-align: center; color: #64748b; margin-top: -8px; }
.illiq-tag { color: #dc2626; font-size: 0.75em; font-weight: 700; margin-left: 4px; }
</style>
""", unsafe_allow_html=True)

def main():
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">📊 Nifty Total Market Stage 2 Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">EOD Analysis · 7-Point Weinstein Score · {now_ist}</p>', unsafe_allow_html=True)

    # ── CONTROL PANEL (Batched) ──
    with st.sidebar.form("controls", clear_on_submit=False):
        st.markdown('<p class="sb-head">🔍 Filters</p>', unsafe_allow_html=True)
        rsi_toggle = st.toggle("Filter: RSI between 50–70", value=False)
        show_illiquid = st.toggle("Show Illiquid Stocks (Vol < 1L)", value=False)
        
        st.markdown("---")
        st.markdown('<p class="sb-head">📦 Select Indices</p>', unsafe_allow_html=True)
        
        const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
        idx_options = list(json.load(open(const_path, "r")).keys()) if os.path.exists(const_path) else []
        
        # Checkbox layout
        selected_indices = []
        cols = st.columns(2)
        for i, idx in enumerate(idx_options):
            default_checked = idx in ["Nifty 50", "Nifty Next 50", "Nifty Midcap 150", "Nifty Smallcap 250", "Nifty Microcap 250"]
            if cols[i % 2].checkbox(idx, value=default_checked):
                selected_indices.append(idx)
        
        run_btn = st.form_submit_button("🚀 Apply Filters & Show", type="primary", width="stretch")

    if "run_triggered" not in st.session_state and run_btn:
        st.session_state["run_triggered"] = True

    if not st.session_state.get("run_triggered"):
        st.info("👈 Select indices/filters and click **Apply Filters & Show** to begin.")
        return

    # ── RESOLVE DATA (Cache/Fetch Logic) ──
    df, cache_date, is_cached = resolve_screener_data(selected_indices, rsi_toggle)
    
    if df.empty:
        st.warning(f"📅 No data available for **{cache_date}**. Yahoo Finance may be syncing. Try again in 30 mins.")
        return
        
    if not is_cached:
        st.success(f"✅ Fetched & cached fresh EOD data for **{cache_date}**.")
    elif cache_date != (datetime.now(IST) - timedelta(days=1 if datetime.now(IST).hour < 19 else 0)).strftime("%Y-%m-%d"):
        st.info(f"ℹ️ Market closed or data pending. Showing latest available cache from **{cache_date}**.")

    # ── APPLY FILTERS & PREPARE DISPLAY ──
    display_df = df.copy()
    if selected_indices: display_df = display_df[display_df["Index"].isin(selected_indices)]
    if rsi_toggle: display_df = display_df[(display_df["RSI"] >= 50) & (display_df["RSI"] <= 70)]
    if not show_illiquid: display_df = display_df[~display_df["Illiquid"]]

    if display_df.empty:
        st.warning("No stocks match the selected filters. Adjust criteria or show illiquid stocks.")
        return

    # Inline ILLIQ tag next to ticker
    display_df["Symbol"] = display_df.apply(
        lambda r: f"{r['Symbol']} 🚩 ILLIQ" if r['Illiquid'] else r['Symbol'], axis=1
    )

    # EXPLICIT COLUMN ORDER: Ticker, Source, Classification, Score, Close, Vol, Vol Ratio, RSI
    display_cols = ["Symbol", "Index", "Stage", "Score", "Close", "Volume", "Vol_Ratio", "RSI"]
    display_df = display_df[display_cols]

    # Drop all helper columns before styling to prevent rendering conflicts
    display_df = display_df.drop(columns=[c for c in display_df.columns if c not in display_cols], errors="ignore")

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

    # Render Table - Fixed deprecation & blank row issue
    st.dataframe(
        styled_df,
        width="stretch",  # Replaced use_container_width=True
        hide_index=True, 
        column_config={
            "Symbol": st.column_config.TextColumn("Ticker", width="medium"),
            "Index": st.column_config.TextColumn("Source", width="small"),
            "Stage": st.column_config.TextColumn("Classification", width="medium"),
            "Score": st.column_config.NumberColumn("Score", format="%d/7", width="small"),
            "Close": st.column_config.NumberColumn("Close (₹)", format="%.2f", width="small"),
            "Volume": st.column_config.NumberColumn("Volume", format="%,d", width="medium"),
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
        width="stretch"  # Replaced use_container_width=True
    )

if __name__ == "__main__":
    main()