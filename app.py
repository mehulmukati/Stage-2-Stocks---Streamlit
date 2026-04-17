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
import warnings
warnings.filterwarnings("ignore")
# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")
HISTORY_PERIOD = "2y"
$ sed -n '25,40p' /workspace/app.py
sed -n '25,40p' /workspace/app.py
HISTORY_PERIOD_MOMENTUM = "1y"
MIN_VOLUME = 100_000
VOL_AVG_PERIOD = 10          # Configurable: 10, 20, or 50 days (Change here)
HH_HL_LOOKBACK = 50          # Change here if needed
MA_RISING_LOOKBACK = 50      # Change here if needed
RESULT_CACHE_DIR = "daily_cache"
os.makedirs(RESULT_CACHE_DIR, exist_ok=True)
# ──────────────────────────────────────────────
# CIRCUIT LEVELS
# ──────────────────────────────────────────────
CIRCUIT_LEVELS = [2.0, 5.0, 10.0, 20.0]
CIRCUIT_TOLERANCE = 0.1  # ±0.1% tolerance
# ──────────────────────────────────────────────
# HOLIDAY & TRADING DAY RESOLVER
$ sed -n '140,200p' /workspace/app.py
sed -n '140,200p' /workspace/app.py
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
$ sed -n '700,732p' /workspace/app.py
sed -n '700,732p' /workspace/app.py
"Sharpe": st.column_config.NumberColumn("Sharpe", format="%.3f", width="small"),
            "Volatility": st.column_config.NumberColumn("Volatility (SD)", format="%.4f", width="small"),
            "52w_High": st.column_config.NumberColumn("52w High", format="%.2f", width="small"),
            "Median Vol": st.column_config.NumberColumn("Median Vol", format="%,d", width="medium"),
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
$ sed -n '200,260p' /workspace/app.py
sed -n '200,260p' /workspace/app.py
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
        "Volatility": round(volatility, 4) if volatility else None,
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
$ sed -n '260,330p' /workspace/app.py
sed -n '260,330p' /workspace/app.py
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
def fetch_full_universe(rsi_filter: bool, for_momentum: bool = False) -> tuple[pd.DataFrame, int]:
    """Downloads ALL indices, scores them, and returns the complete DF."""
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        st.error("❌ `constituents.json` missing.")
        return pd.DataFrame(), 0
    with open(const_path, "r") as f:
        constituents = json.load(f)
    # Flatten ALL symbols from ALL indices
    all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))
    tickers = [f"{s}.NS" for s in all_symbols]
    period = HISTORY_PERIOD_MOMENTUM if for_momentum else HISTORY_PERIOD
    try:
        with st.spinner("🌐 Fetching EOD data for full Nifty 750 universe..."):
            raw = yf.download(tickers, period=period, group_by="ticker",
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
            if for_momentum:
                res = score_momentum(sub)
            else:
                res = score_stage2(sub)
            if res:
                res["Symbol"] = sym
                res["Index"] = next((idx for idx, syms in constituents.items() if sym in syms), "Unknown")
                results.append(res)
        except: continue
    df = pd.DataFrame(results)
    if df.empty: return pd.DataFrame(), 0
    if not for_momentum and rsi_filter:
        df = df[(df["RSI"] >= 50) & (df["RSI"] <= 70)]
    return df.sort_values("Score" if not for_momentum else "Close", ascending=False), len(df)
def fetch_momentum_universe(universe: str) -> tuple[pd.DataFrame, int]:
    """Fetches data for selected universe and calculates momentum metrics."""
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        st.error("❌ `constituents.json` missing.")
        return pd.DataFrame(), 0
    with open(const_path, "r") as f:
        constituents = json.load(f)
$ sed -n '330,420p' /workspace/app.py
sed -n '330,420p' /workspace/app.py
constituents = json.load(f)
    symbols = _get_universe_symbols(universe, constituents)
    tickers = [f"{s}.NS" for s in symbols]
    try:
        with st.spinner(f"🌐 Fetching EOD data for {universe}..."):
            raw = yf.download(tickers, period=HISTORY_PERIOD_MOMENTUM, group_by="ticker",
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
            res = score_momentum(sub)
            if res:
                res["Symbol"] = sym
                res["Index"] = next((idx for idx, syms in constituents.items() if sym in syms), "Unknown")
                results.append(res)
        except: continue
    df = pd.DataFrame(results)
    if df.empty: return pd.DataFrame(), 0
    return df, len(df)
def resolve_screener_data(rsi_filter: bool, for_momentum: bool = False):
    """Implements exact logic: Time-based target → Find valid trading day → Cache/Fetch."""
    now = datetime.now(IST)
    # 1. Determine starting date based on 7 PM cutoff
    start_date = now.strftime("%Y-%m-%d") if now.hour >= 19 else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    # 2. Find last valid trading day (handles weekends/holidays)
    holidays = load_nse_holidays()
    target_key = get_last_valid_trading_date(start_date, holidays)
    # For momentum screener, don't use cache (different filters per run)
    if for_momentum:
        df, valid_count = fetch_full_universe(rsi_filter=False, for_momentum=True)
        return df, target_key, False
    # 3. Check cache first (only for Stage 2 screener)
    df = load_json_cache(target_key)
    if df is not None:
        return df, target_key, True  # (data, date, is_cached)
    # 4. Cache miss → Fetch FULL UNIVERSE
    df, valid_count = fetch_full_universe(rsi_filter, for_momentum=False)
    if not df.empty:
        save_json_cache(df, target_key)
        return df, target_key, False
    # 5. Fetch failed → Look for any older cache
    try:
        files = sorted([f.replace(".json", "") for f in os.listdir(RESULT_CACHE_DIR) if f.endswith(".json")], reverse=True)
        for f in files:
            if f <= target_key:
                df = load_json_cache(f)
                if df is not None:
                    return df, f, True
    except Exception:
        pass
    return pd.DataFrame(), target_key, True  # Fallback to empty
# ──────────────────────────────────────────────
# STREAMLIT UI
# ──────────────────────────────────────────────
st.set_page_config(page_title="Stock Screeners | Nifty 750", page_icon="📈", layout="wide")
st.markdown("""
<style>
.sb-head { font-weight: 700; margin-bottom: 0.5rem; font-size: 0.95rem; }
.hero { text-align: center; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
.sub-hero { text-align: center; color: #64748b; margin-top: -8px; }
</style>
""", unsafe_allow_html=True)
def stage2_screener_ui():
    """UI for Stage 2 Screener"""
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">📊 Nifty Total Market Stage 2 Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">EOD Analysis · 7-Point Weinstein Score · {now_ist}</p>', unsafe_allow_html=True)
    # ── CONTROL PANEL (Batched) ──
$ sed -n '520,600p' /workspace/app.py
sed -n '520,600p' /workspace/app.py
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
    # ── CONTROL PANEL (Batched) ──
    with st.sidebar.form("momentum_controls", clear_on_submit=False):
        st.markdown('<p class="sb-head">🔍 Filters</p>', unsafe_allow_html=True)
        # Universe Selection
        universe_options = [
            "Nifty 50", "Nifty Next 50", "Nifty Midcap 150", "Nifty Smallcap 250", "Nifty Microcap 250",
            "Nifty LargeMidcap 250", "Nifty MidSmallcap 400", "Nifty Total Market"
        ]
        selected_universe = st.selectbox("Choosing Universe", options=universe_options, index=7)
        # Minimum Annual Return
        min_annual_return = st.number_input("Minimum Annual Return (%)", min_value=0.0, max_value=1000.0, value=0.0, step=0.01, format="%.2f")
        # DMA Filters
        col1, col2 = st.columns(2)
        with col1:
            close_above_100dma = st.checkbox("Close > 100 DMA", value=False)
        with col2:
            close_above_200dma = st.checkbox("Close > 200 DMA", value=False)
        # 52W High Filter
        pct_from_52w_high = st.number_input("Last Close / 52w High (within %)", min_value=0, max_value=100, value=25, step=1)
        # Max Circuits
        max_circuits = st.number_input("Max Circuits (past 1 year)", min_value=0, max_value=100, value=5, step=1)
        # Positive Days
        st.markdown('<p class="sb-head" style="margin-top: 1rem;">Positive Days (%)</p>', unsafe_allow_html=True)
        col3, col4, col5 = st.columns(3)
        with col3:
            pos_days_3m = st.number_input("3 Months", min_value=0, max_value=100, value=0, step=1)
        with col4:
            pos_days_6m = st.number_input("6 Months", min_value=0, max_value=100, value=0, step=1)
        with col5:
            pos_days_12m = st.number_input("12 Months", min_value=0, max_value=100, value=0, step=1)
        # Sorting Method
        sort_options = [
            "1 year", "3 months", "6 months", "9 months",
$ sed -n '600,700p' /workspace/app.py
sed -n '600,700p' /workspace/app.py
"1 year", "3 months", "6 months", "9 months",
            "Average of 3/6/9/12 months", "Average of 3/6 months"
        ]
        sort_method = st.selectbox("Sorting Method (Sharpe Ratio)", options=sort_options, index=4)
        run_btn = st.form_submit_button("🚀 Run Momentum Screener", type="primary", width="stretch")
    if "momentum_run_triggered" not in st.session_state and run_btn:
        st.session_state["momentum_run_triggered"] = True
    if not st.session_state.get("momentum_run_triggered"):
        st.info("👈 Set your filters and click **Run Momentum Screener** to begin.")
        return
    # ── FETCH & PROCESS DATA ──
    df, total_count = fetch_momentum_universe(selected_universe)
    if df.empty:
        st.warning("📅 No data available. Yahoo Finance may be syncing. Try again in 30 mins.")
        return
    st.success(f"✅ Fetched data for {total_count} stocks in {selected_universe}")
    # ── APPLY FILTERS ──
    display_df = df.copy()
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
    c2.metric("Total in Universe", total_count)
    c3.metric("Matches (Filters)", len(display_df))
    # ── RENDER TABLE ──
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Symbol": st.column_config.TextColumn("Symbol", width="medium"),
            "Index": st.column_config.TextColumn("Index", width="small"),
            "Close": st.column_config.NumberColumn("Close (₹)", format="%.2f", width="small"),
            "Sharpe": st.column_config.NumberColumn("Sharpe", format="%.3f", widt