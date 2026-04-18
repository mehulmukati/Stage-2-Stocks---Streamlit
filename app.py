#!/usr/bin/env python3
import os
import json
import warnings
import streamlit as st
from datetime import datetime

warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv()

import db
from config import IST
from data import resolve_screener_data
from momentum_engine import _calculate_avg_sharpe

# ── DB INIT (once at startup) ──
@st.cache_resource
def _init_db():
    db.init_db()

_init_db()

# ── PAGE CONFIG & CSS ──
st.set_page_config(page_title="Stock Screeners | Nifty 750", page_icon="📈", layout="wide")
st.markdown("""
<style>
.hero { text-align: center; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
.sub-hero { text-align: center; color: #64748b; margin-top: -8px; }
.stTabs [data-baseweb="tab-list"] { gap: 2px; width: 100%; justify-content: stretch; }
.stTabs [data-baseweb="tab"] { flex-grow: 1; width: 100%; max-width: none; }
.stTabs { width: 100%; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# STAGE 2 SCREENER UI
# ──────────────────────────────────────────────
def stage2_screener_ui(selected_indices: list[str]):
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">📊 Nifty Total Market Stage 2 Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">EOD Analysis · 7-Point Weinstein Score · {now_ist}</p>', unsafe_allow_html=True)

    fc1, fc2, fc3 = st.columns([2, 2, 1])
    with fc1:
        rsi_toggle = st.toggle("RSI between 50–70", value=False, key="stage2_rsi_toggle")
    with fc2:
        show_illiquid = st.toggle("Show Illiquid (Avg Vol < 1L)", value=False, key="stage2_show_illiquid")
    with fc3:
        run_btn = st.button("🚀 Run", type="primary", use_container_width=True, key="stage2_run_btn")

    st.divider()

    if run_btn:
        st.session_state["stage2_run_triggered"] = True

    if not st.session_state.get("stage2_run_triggered"):
        st.info("👈 Select indices in the sidebar, set filters above, then click **Run**.")
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

    # ── APPLY UI FILTERS LOCALLY ──
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

    display_df["Symbol"] = display_df.apply(
        lambda r: f"{r['Symbol']} 🚩 ILLIQ" if r['Illiquid'] else r['Symbol'], axis=1
    )

    display_cols = ["Symbol", "Index", "Stage", "Score", "Close", "Volume", "Avg_Vol", "Vol_Ratio", "RSI"]
    display_df = display_df[display_cols]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cache Date", cache_date)
    c2.metric("Total Universe", len(df))
    c3.metric("Matches (Filters)", len(display_df))
    c4.metric("Strong Stage 2", len(display_df[display_df["Score"] >= 6]))

    def color_rows(row):
        bg_map = {
            "🟢 Strong Stage 2": "#ecfdf5",
            "🟡 Likely Stage 2": "#fefce8",
            "🟠 Early/Weak Stage 2": "#fef2f2",
            "⚪ Not Stage 2": "#f9fafb"
        }
        return [f'background-color: {bg_map.get(row["Stage"], "#ffffff")}'] * len(row)

    styled_df = display_df.style.apply(color_rows, axis=1)

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

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Screener Results", csv,
        file_name=f"stage2_screener_{datetime.now(IST).strftime('%Y%m%d')}.csv",
        mime="text/csv",
        width="stretch"
    )


# ──────────────────────────────────────────────
# MOMENTUM SCREENER UI
# ──────────────────────────────────────────────
def momentum_screener_ui(selected_indices: list[str], idx_options: list[str]):
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">🚀 Momentum Stock Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">Sharpe Ratio Based Momentum Analysis · {now_ist}</p>', unsafe_allow_html=True)

    fr1c1, fr1c2, fr1c3, fr1c4 = st.columns([2, 2, 2, 2])
    with fr1c1:
        min_annual_return = st.number_input("Min Annual Return (%)", min_value=0.0, max_value=1000.0, value=0.0, step=0.01, format="%.2f", key="mom_min_annual_return")
    with fr1c2:
        pct_from_52w_high = st.number_input("Within % of 52w High", min_value=0, max_value=100, value=25, step=1, key="mom_pct_from_52w_high")
    with fr1c3:
        max_circuits = st.number_input("Max Circuits (1yr)", min_value=0, max_value=100, value=18, step=1, key="mom_max_circuits")
    with fr1c4:
        sort_options = ["1 year", "3 months", "6 months", "9 months", "Average of 3/6/9/12 months", "Average of 3/6 months"]
        sort_method = st.selectbox("Sort by Sharpe", options=sort_options, index=4, key="mom_sort_method")

    fr2c1, fr2c2, fr2c3, fr2c4, fr2c5 = st.columns([2, 2, 2, 2, 1])
    with fr2c1:
        close_above_100dma = st.checkbox("Close > 100 DMA", value=False, key="mom_close_above_100dma")
        close_above_200dma = st.checkbox("Close > 200 DMA", value=False, key="mom_close_above_200dma")
    with fr2c2:
        pos_days_3m = st.number_input("Pos Days 3M (%)", min_value=0, max_value=100, value=0, step=1, key="mom_pos_days_3m")
    with fr2c3:
        pos_days_6m = st.number_input("Pos Days 6M (%)", min_value=0, max_value=100, value=0, step=1, key="mom_pos_days_6m")
    with fr2c4:
        pos_days_12m = st.number_input("Pos Days 12M (%)", min_value=0, max_value=100, value=0, step=1, key="mom_pos_days_12m")
    with fr2c5:
        run_btn = st.button("🚀 Run", type="primary", use_container_width=True, key="mom_run_btn")

    st.divider()

    if run_btn:
        st.session_state["mom_run_triggered"] = True

    if not st.session_state.get("mom_run_triggered"):
        st.info("👈 Select indices in the sidebar, set filters above, then click **Run**.")
        return

    # ── RESOLVE DATA  (Memory → DB → Internet) ──
    full_df, cache_date, source = resolve_screener_data(rsi_filter=False, for_momentum=True)

    if full_df.empty:
        st.warning("📅 No data available. Try again in a few minutes or check your internet connection.")
        return

    if source == "memory":
        st.success(f"⚡ Served from memory cache for **{cache_date}** · {len(full_df)} stocks")
    elif source == "db":
        st.info(f"💾 Loaded from local database for **{cache_date}** · {len(full_df)} stocks")
    elif source == "internet":
        st.success(f"🌐 Fetched fresh EOD data and saved to database for **{cache_date}** · {len(full_df)} stocks")

    # ── APPLY INDEX FILTER (in-memory) ──
    display_df = full_df[full_df["Index"].isin(selected_indices)].copy() if selected_indices else full_df.copy()

    # ── APPLY FILTERS ──
    if min_annual_return > 0:
        display_df = display_df[display_df["1Y_Change"].notna() & (display_df["1Y_Change"] >= min_annual_return)]

    if close_above_100dma:
        display_df = display_df[display_df["Close"] > display_df["DMA100"]]

    if close_above_200dma:
        display_df = display_df[display_df["Close"] > display_df["DMA200"]]

    threshold_multiplier = (100 - pct_from_52w_high) / 100
    display_df = display_df[display_df["Close"] >= (threshold_multiplier * display_df["52w_High"])]

    display_df = display_df[display_df["Circuit_Count"] <= max_circuits]

    if pos_days_3m > 0:
        display_df = display_df[display_df["Pos_Days_3M"].notna() & (display_df["Pos_Days_3M"] >= pos_days_3m)]
    if pos_days_6m > 0:
        display_df = display_df[display_df["Pos_Days_6M"].notna() & (display_df["Pos_Days_6M"] >= pos_days_6m)]
    if pos_days_12m > 0:
        display_df = display_df[display_df["Pos_Days_12M"].notna() & (display_df["Pos_Days_12M"] >= pos_days_12m)]

    if display_df.empty:
        st.warning("No stocks match the selected filters. Adjust criteria and try again.")
        return

    # ── SORT BY SHARPE ──
    display_df["Avg_Sharpe"] = display_df.apply(lambda row: _calculate_avg_sharpe(row, sort_method), axis=1)
    display_df = display_df[display_df["Avg_Sharpe"].notna()]

    if display_df.empty:
        st.warning("No stocks have valid Sharpe ratios for the selected sorting method.")
        return

    display_df = display_df.sort_values("Avg_Sharpe", ascending=False)

    output_cols = [
        "Symbol", "Index", "Close", "Avg_Sharpe", "Volatility", "52w_High",
        "Vol_Median", "1Y_Change", "Pct_From_52W_High", "Circuit_Count"
    ]
    display_df = display_df[output_cols]

    display_df = display_df.rename(columns={
        "Avg_Sharpe": "Sharpe",
        "Vol_Median": "Median Vol",
        "1Y_Change": "1Y Change",
        "Pct_From_52W_High": "% from 52wH",
        "Circuit_Count": "Circuit Close"
    })

    c1, c2, c3 = st.columns(3)
    universe_label = "All Indices" if len(selected_indices) == len(idx_options) else ", ".join(selected_indices) if selected_indices else "None"
    c1.metric("Universe", universe_label)
    c2.metric("Total in Universe", len(full_df))
    c3.metric("Matches (Filters)", len(display_df))

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

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Momentum Screener Results", csv,
        file_name=f"momentum_screener_{datetime.now(IST).strftime('%Y%m%d')}.csv",
        mime="text/csv",
        width="stretch"
    )


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    idx_options = list(json.load(open(const_path, "r")).keys()) if os.path.exists(const_path) else []

    with st.sidebar:
        st.markdown("### 📦 Select Indices")
        st.caption("Applies to both screeners")
        cols = st.columns(2)
        selected_indices = []
        for i, idx in enumerate(idx_options):
            if cols[i % 2].checkbox(idx, value=True, key=f"shared_idx_{idx}"):
                selected_indices.append(idx)
        st.markdown("---")
        st.caption("💡 **Composite combinations:**\n\n• 50 + Next50 + Mid150 = LargeMidCap 250\n\n• Mid150 + Small250 = MidSmallCap 400\n\n• All five = Total Market")

    tab1, tab2 = st.tabs(["📊 Stage 2 Screener", "🚀 Momentum Screener"])

    with tab1:
        stage2_screener_ui(selected_indices)

    with tab2:
        momentum_screener_ui(selected_indices, idx_options)


if __name__ == "__main__":
    main()
