#!/usr/bin/env python3
"""
Screener app — Stage 2, Momentum, Phase Chart, Ichimoku. Parquet-backed, no external DB.
Backtest lives in app_backtest.py (separate parquet baseline).
"""

import difflib
import importlib
import inspect
import json
import logging
import os
import threading
import warnings
from datetime import datetime

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

import charts as chart_builders
import data as data_access
import ichimoku_engine as ichimoku_calculations
import ichimoku_summary as ichimoku_descriptions
import workers as worker_functions
from app_backtest import _sidebar_backtest, render_backtest_tabs
from app_live_signal import _sidebar_live_signal, live_signal_results
from config import IST, SCREENER_OHLCV_PARQUET
from jobs import JobStatus, registry
from momentum_engine import _calculate_avg_sharpe
from stage2_engine import compute_rolling_stage2 as _compute_rolling_stage2
from ui_helpers import _get_user_token, _poll_job

# Streamlit reruns app.py in the same process and can retain pre-change modules.
# Reload only when a cached module predates the current Ichimoku interfaces.
if (
    getattr(chart_builders, "ICHIMOKU_CHART_VERSION", 0) < 7
    or not hasattr(chart_builders, "ichimoku_chart_figure")
    or "timeframe" not in inspect.signature(chart_builders.ichimoku_chart_figure).parameters
):
    chart_builders = importlib.reload(chart_builders)
if (
    getattr(ichimoku_calculations, "ICHIMOKU_ENGINE_VERSION", 0) < 2
    or "timeframe" not in inspect.signature(ichimoku_calculations.compute_ichimoku).parameters
):
    ichimoku_calculations = importlib.reload(ichimoku_calculations)
if getattr(ichimoku_descriptions, "ICHIMOKU_SUMMARY_VERSION", 0) < 2 or not hasattr(ichimoku_descriptions, "_periods"):
    ichimoku_descriptions = importlib.reload(ichimoku_descriptions)
_data_access_reloaded = False
if getattr(data_access, "CHART_DATA_VERSION", 0) < 2 or getattr(data_access, "SCREENER_DATA_VERSION", 0) < 2:
    data_access = importlib.reload(data_access)
    _data_access_reloaded = True
if _data_access_reloaded or getattr(worker_functions, "SCREENER_WORKER_VERSION", 0) < 2:
    worker_functions = importlib.reload(worker_functions)

ichimoku_chart_figure = chart_builders.ichimoku_chart_figure
phase_chart_figure = chart_builders.phase_chart_figure
_compute_ichimoku = ichimoku_calculations.compute_ichimoku
latest_ichimoku_state = ichimoku_calculations.latest_ichimoku_state
build_ichimoku_summary = ichimoku_descriptions.build_ichimoku_summary
_load_constituents = data_access._load_constituents
_score_cache = data_access._score_cache
fetch_chart_data = data_access.fetch_chart_data
get_universe_coverage = data_access.get_universe_coverage
momentum_worker = worker_functions.momentum_worker
stage2_worker = worker_functions.stage2_worker


@st.cache_data(ttl=3600)
def compute_rolling_stage2(df):
    return _compute_rolling_stage2(df)


@st.cache_data(ttl=3600)
def compute_ichimoku(df, timeframe: str = "Daily"):
    return _compute_ichimoku(df, timeframe=timeframe)


_state_lock = threading.RLock()
_last_chart_ticker: str = ""


# ── PARQUET BASELINE CHECK (once at startup) ──
@st.cache_resource
def _check_baseline() -> bool:
    """Return True if screener_ohlcv.parquet exists; warn once if missing."""
    return os.path.exists(SCREENER_OHLCV_PARQUET)


_baseline_ok = _check_baseline()

# ── PAGE CONFIG & CSS ──
st.set_page_config(page_title="Stock Screeners | Nifty 750", page_icon="📈", layout="wide")
# Backtest lives at app_backtest.py — link to it from the screener if desired.
st.markdown(
    """
<style>
.hero { text-align: center; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
.sub-hero { text-align: center; opacity: 0.6; margin-top: -8px; }
</style>
""",
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────
# PHASE CHART
# ──────────────────────────────────────────────


def get_closest_symbol_match(ticker: str, threshold: float = 0.6) -> str | None:
    constituents = _load_constituents()
    all_symbols = list(dict.fromkeys([s for syms in constituents.values() for s in syms]))
    if not all_symbols:
        return None
    matches = difflib.get_close_matches(ticker.upper(), all_symbols, n=1, cutoff=threshold)
    return matches[0] if matches else None


def render_phase_chart(ticker: str, use_log_scale: bool = True):
    with st.spinner(f"Loading data for {ticker}…"):
        df = fetch_chart_data(ticker)

    if df.empty:
        closest_match = get_closest_symbol_match(ticker)
        if closest_match:
            st.info(f"ℹ️ Symbol **{ticker}** not found. Did you mean **{closest_match}**? Loading that instead...")
            with st.spinner(f"Loading data for {closest_match}…"):
                df = fetch_chart_data(closest_match)
            if df.empty:
                st.error(f"❌ No data available for **{closest_match}**. Please try another symbol.")
                return
            ticker = closest_match
        else:
            st.error(f"❌ Symbol **{ticker}** not found in available stocks. Please check the symbol and try again.")
            return

    rolled = compute_rolling_stage2(df)
    st.plotly_chart(phase_chart_figure(rolled, ticker, use_log_scale), width="stretch")
    st.caption(
        "🟢 Strong Stage 2 (score ≥ 6) · "
        "🟡 Likely Stage 2 (4–5) · "
        "🟠 Early/Weak Stage 2 (2–3) · "
        "White = Not Stage 2 (<2)"
    )


def render_ichimoku_chart(
    ticker: str,
    timeframe: str = "Daily",
    use_log_scale: bool = True,
    show_chikou: bool = True,
    show_crossovers: bool = True,
):
    with st.spinner(f"Loading data for {ticker}…"):
        df = fetch_chart_data(ticker)

    if df.empty:
        closest_match = get_closest_symbol_match(ticker)
        if closest_match:
            st.info(f"ℹ️ Symbol **{ticker}** not found. Did you mean **{closest_match}**? Loading that instead...")
            with st.spinner(f"Loading data for {closest_match}…"):
                df = fetch_chart_data(closest_match)
            if df.empty:
                st.error(f"❌ No data available for **{closest_match}**. Please try another symbol.")
                return
            ticker = closest_match
        else:
            st.error(f"❌ Symbol **{ticker}** not found in available stocks. Please check the symbol and try again.")
            return

    try:
        calculated = compute_ichimoku(df, timeframe)
    except ValueError as exc:
        st.error(f"❌ Ichimoku chart cannot be calculated: {exc}")
        return
    if calculated.empty:
        st.warning(f"No valid OHLC history is available for **{ticker}**.")
        return

    state = latest_ichimoku_state(calculated, ticker, timeframe)
    metric_cols = [*st.columns(2), *st.columns(2)]
    price_position = str(state.get("price_position", "unavailable")).title()
    distance = state.get("distance_pct")
    distance_text = f"{float(distance):.1f}% from cloud" if distance is not None else None
    price_help_parts = [f"Cloud visible at the latest price date: {state.get('displayed_cloud', 'unavailable')}."]
    if distance_text:
        price_help_parts.append(distance_text + ".")
    metric_cols[0].metric(
        "Price vs Cloud",
        price_position,
        help=" ".join(price_help_parts),
    )
    metric_cols[1].metric("TK Alignment", str(state.get("tk_relation", "unavailable")).title())
    latest_cross = state.get("last_cross")
    if latest_cross:
        cross_value = str(latest_cross["strength"]).title()
        age = int(latest_cross["age_sessions"])
        if timeframe == "Weekly":
            age_unit = "week" if age == 1 else "weeks"
        else:
            age_unit = "trading session" if age == 1 else "trading sessions"
        cross_help = f"{str(latest_cross['direction']).title()} crossover · {age} {age_unit} ago."
    else:
        cross_value, cross_help = "None", "No valid cross in loaded history."
    metric_cols[2].metric("Latest TK Cross", cross_value, help=cross_help)
    metric_cols[3].metric("Projected Cloud", str(state.get("projected_cloud", "unavailable")).title())

    st.plotly_chart(
        ichimoku_chart_figure(
            calculated,
            ticker,
            use_log_scale,
            show_chikou,
            show_crossovers,
            timeframe,
        ),
        width="stretch",
    )
    st.caption(
        "🟢 Bullish cloud (Senkou A ≥ B) · 🔴 Bearish cloud (Senkou A < B) · "
        "▲ Bullish Tenkan–Kijun cross · ▼ Bearish Tenkan–Kijun cross"
    )
    with st.container(border=True):
        st.markdown("#### Ichimoku Summary")
        st.write(build_ichimoku_summary(state))
        st.caption("Rule-based technical description; not investment advice.")


# ──────────────────────────────────────────────
# SHARED HELPERS
# ──────────────────────────────────────────────


def render_ichimoku_basics() -> None:
    """Explain the standard Ichimoku components and a simple reading order."""
    st.markdown("## Ichimoku basics")
    st.write(
        "Ichimoku Kinko Hyo combines trend, momentum, and support/resistance in one view. "
        "This chart uses the standard **9 / 26 / 52** settings."
    )

    st.markdown("### The five lines")
    st.markdown(
        """
| Component | What it measures | A simple way to read it |
|---|---|---|
| **Tenkan-sen (Conversion Line)** | Midpoint of the 9-period high and low | Fast measure of price balance |
| **Kijun-sen (Base Line)** | Midpoint of the 26-period high and low | Slower trend reference |
| **Senkou Span A** | Midpoint of Tenkan and Kijun, plotted 26 periods ahead | Faster edge of the cloud |
| **Senkou Span B** | Midpoint of the 52-period high and low, plotted 26 periods ahead | Slower edge of the cloud |
| **Chikou Span (Lagging Span)** | Current close plotted 26 periods back | Compares current and earlier price action |
"""
    )

    st.markdown("### Read the chart in three steps")
    step_cols = st.columns(3)
    with step_cols[0]:
        with st.container(border=True):
            st.markdown("#### 1. Price vs cloud")
            st.write(
                "Price above the cloud suggests a bullish trend, below it suggests a bearish trend, "
                "and inside it suggests transition or uncertainty."
            )
    with step_cols[1]:
        with st.container(border=True):
            st.markdown("#### 2. Tenkan vs Kijun")
            st.write(
                "Tenkan above Kijun is bullish alignment; Tenkan below Kijun is bearish alignment. "
                "A cross marks a change in that alignment."
            )
    with step_cols[2]:
        with st.container(border=True):
            st.markdown("#### 3. Forward cloud")
            st.write(
                "A green cloud has Span A above Span B; a red cloud has Span A below Span B. "
                "A thicker cloud can act as a broader support or resistance zone."
            )

    st.info(
        "The cloud is shifted forward to show potential support and resistance structure. "
        "It is an indicator projection—not a prediction of future price."
    )
    st.caption(
        "Signals are generally more meaningful when price, Tenkan/Kijun alignment, and the cloud agree. "
        "Use Ichimoku with risk management and other analysis; it is not investment advice."
    )


def _render_source_banner(source: str, cache_date: str, count: int = None) -> None:
    suffix = f" · {count} stocks" if count is not None else ""
    if source == "memory":
        st.success(f"⚡ Served from memory cache for **{cache_date}**{suffix}.")
    elif source == "db":
        st.info(f"💾 Loaded from local database for **{cache_date}**{suffix}.")
    elif source == "internet":
        st.success(f"🌐 Fetched fresh EOD data and saved to database for **{cache_date}**{suffix}.")
    elif source == "partial":
        st.warning(
            f"⚠️ Calculated for target **{cache_date}** using the latest price available per stock{suffix}. "
            "Check **Price Date** before acting; this partial result was not saved as a fresh cache."
        )
    elif source == "fallback":
        st.warning(f"⚠️ Fresh refresh unavailable. Showing the last verified cache from **{cache_date}**{suffix}.")


def _invalidate_legacy_score_result(kind: str, cached: dict) -> bool:
    """Discard pre-price-date results retained in Streamlit/process memory."""
    frame = cached.get("df")
    if isinstance(frame, pd.DataFrame) and "Price Date" in frame.columns:
        return False
    st.session_state.pop(f"{kind}_cached_result", None)
    _score_cache[kind] = {"date": None, "data": None}
    st.warning(
        "⚠️ An older unverifiable screener result was cleared. "
        "The background worker binding has been refreshed; click **Run** once more to calculate a date-safe result."
    )
    return True


# ──────────────────────────────────────────────
# RESULTS — COVERAGE
# ──────────────────────────────────────────────


def coverage_results():
    st.markdown('<p class="hero">📋 Universe Coverage</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-hero">Why some index constituents are absent from the screener</p>',
        unsafe_allow_html=True,
    )

    cov = get_universe_coverage()
    if not cov:
        st.error("Could not load coverage data — constituents.json or parquet may be missing.")
        return

    s = cov["summary"]

    st.markdown(
        """
Both screeners (**Stage 2** and **Momentum**) require a stock to have at least **250 trading days**
of price history within the last 550 calendar days before it can be scored. Stocks that fall below
this threshold — typically recent IPOs or newly-added index constituents — are silently excluded
from the universe count. They will appear automatically once they accumulate enough data.
"""
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Constituents", s["total"])
    c2.metric("Currently Scored", s["scored"])
    c3.metric("Not Yet Eligible", s["missing_count"])

    st.divider()

    # Per-index breakdown table
    st.subheader("By Index")
    index_rows = []
    for idx_name, d in cov["by_index"].items():
        pct = round(d["scored"] / d["total"] * 100, 1) if d["total"] else 0
        index_rows.append(
            {
                "Index": idx_name,
                "Constituents": d["total"],
                "Scored": d["scored"],
                "Missing": len(d["missing"]),
                "Coverage %": pct,
            }
        )
    st.dataframe(pd.DataFrame(index_rows), hide_index=True, width="stretch")

    st.divider()

    # Full missing-symbol table
    st.subheader(f"Excluded Symbols ({s['missing_count']} total)")
    st.caption(
        "Sorted by trading days available (ascending — closest to qualifying first). "
        "Refreshes automatically when the parquet is updated."
    )

    if cov["all_missing"]:
        missing_df = pd.DataFrame(cov["all_missing"])
        st.dataframe(
            missing_df,
            hide_index=True,
            width="stretch",
            column_config={
                "Symbol": st.column_config.TextColumn("Symbol", width="small"),
                "Index": st.column_config.TextColumn("Index", width="medium"),
                "Trading Days": st.column_config.NumberColumn("Trading Days", format="%d", width="small"),
                "Days Until Eligible": st.column_config.NumberColumn("Days Until Eligible", format="%d", width="small"),
                "Weeks Until Eligible": st.column_config.NumberColumn(
                    "Weeks Until Eligible", format="%d wks", width="small"
                ),
            },
            height=min(50 + len(cov["all_missing"]) * 35, 800),
        )
    else:
        st.success("All constituents have sufficient data to be scored.")


# ──────────────────────────────────────────────
# RESULTS — STAGE 2
# ──────────────────────────────────────────────


def stage2_results(selected_indices: list[str], rsi_toggle: bool, show_illiquid: bool):
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">📊 Stage 2 Breakout Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">EOD Analysis · 8-Point Weinstein Score · {now_ist}</p>', unsafe_allow_html=True)
    st.divider()

    if _poll_job("stage2", stage2_worker):
        return

    cached = st.session_state.get("stage2_cached_result")
    if cached is None:
        proc = _score_cache["stage2"]
        if proc["data"] is not None and proc["date"] is not None:
            cached = {"df": proc["data"], "cache_date": proc["date"], "source": "memory"}
            st.session_state["stage2_cached_result"] = cached

    if cached is None:
        st.info("Set filters in the sidebar and click **Run**.")
        return

    if _invalidate_legacy_score_result("stage2", cached):
        return

    df, cache_date, source = cached["df"], cached["cache_date"], cached["source"]
    _render_source_banner(source, cache_date)

    display_df = df.copy()
    if selected_indices:
        display_df = display_df[display_df["Index"].isin(selected_indices)]
    if rsi_toggle:
        display_df = display_df[(display_df["RSI"] >= 50) & (display_df["RSI"] <= 70)]
    if not show_illiquid:
        display_df = display_df[~display_df["Illiquid"]]

    if display_df.empty:
        st.warning("No stocks match the selected filters. Adjust criteria or enable illiquid stocks.")
        return

    def _decorate_symbol(r):
        sym = r["Symbol"]
        if r.get("Illiquid", False):
            sym += " 🚩 ILLIQ"
        if r.get("Retest", False):
            sym += " 🔄 RT"
        return sym

    display_df["Symbol"] = display_df.apply(_decorate_symbol, axis=1)
    display_df = display_df[
        ["Symbol", "Index", "Stage", "Score", "Close", "Price Date", "Volume", "Avg_Vol", "Vol_Ratio", "RSI"]
    ]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cache Date", cache_date)
    c2.metric(
        "Total Universe",
        len(df),
        help="Stocks with ≥250 days of price history. See the 📋 Coverage tab for the full list of excluded symbols.",
    )
    c3.metric("Matches", len(display_df))
    c4.metric("Strong Stage 2", len(display_df[display_df["Score"] >= 6]))

    def color_rows(row):
        bg_map = {
            "🟢 Strong Stage 2": "rgba(34, 197, 94, 0.18)",
            "🟡 Likely Stage 2": "rgba(234, 179, 8, 0.18)",
            "🟠 Early/Weak Stage 2": "rgba(249, 115, 22, 0.15)",
            "⚪ Not Stage 2": "rgba(0, 0, 0, 0)",
        }
        return [f'background-color: {bg_map.get(row["Stage"], "rgba(0,0,0,0)")}'] * len(row)

    st.dataframe(
        display_df.style.apply(color_rows, axis=1),
        width="stretch",
        hide_index=True,
        column_config={
            "Symbol": st.column_config.TextColumn("Ticker", width="medium"),
            "Index": st.column_config.TextColumn("Source", width="medium"),
            "Stage": st.column_config.TextColumn("Classification", width="medium"),
            "Score": st.column_config.NumberColumn("Score", format="%d/8", width="small"),
            "Close": st.column_config.NumberColumn("Close (₹)", format="%.2f", width="small"),
            "Price Date": st.column_config.TextColumn("Price Date", width="small"),
            "Volume": st.column_config.NumberColumn("Volume", format="%,d", width="small"),
            "Avg_Vol": st.column_config.NumberColumn("Avg Vol (10d)", format="%,d", width="small"),
            "Vol_Ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2f x", width="small"),
            "RSI": st.column_config.NumberColumn("RSI(14)", format="%.1f", width="small"),
        },
        height=650,
    )

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Results",
        csv,
        file_name=f"stage2_screener_{datetime.now(IST).strftime('%Y%m%d')}.csv",
        mime="text/csv",
        width="stretch",
    )


# ──────────────────────────────────────────────
# RESULTS — MOMENTUM
# ──────────────────────────────────────────────


def momentum_results(selected_indices: list[str], idx_options: list[str], filters: dict):
    now_ist = datetime.now(IST).strftime("%d %b %Y · %I:%M %p IST")
    st.markdown('<p class="hero">🚀 Momentum Stock Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-hero">Sharpe Ratio Based Momentum Analysis · {now_ist}</p>', unsafe_allow_html=True)
    st.divider()

    if _poll_job("momentum", momentum_worker):
        return

    cached = st.session_state.get("momentum_cached_result")
    if cached is None:
        proc = _score_cache["momentum"]
        if proc["data"] is not None and proc["date"] is not None:
            cached = {"df": proc["data"], "cache_date": proc["date"], "source": "memory"}
            st.session_state["momentum_cached_result"] = cached

    if cached is None:
        st.info("Set filters in the sidebar and click **Run**.")
        return

    if _invalidate_legacy_score_result("momentum", cached):
        return

    full_df, cache_date, source = cached["df"], cached["cache_date"], cached["source"]
    _render_source_banner(source, cache_date, count=len(full_df))

    display_df = full_df[full_df["Index"].isin(selected_indices)].copy() if selected_indices else full_df.copy()

    if filters["min_annual_return"] > 0:
        display_df = display_df[
            display_df["1Y_Change"].notna() & (display_df["1Y_Change"] >= filters["min_annual_return"])
        ]
    if filters["close_above_100dma"]:
        display_df = display_df[display_df["DMA100"].notna() & (display_df["Close"] > display_df["DMA100"])]
    if filters["close_above_200dma"]:
        display_df = display_df[display_df["DMA200"].notna() & (display_df["Close"] > display_df["DMA200"])]

    threshold = (100 - filters["pct_from_52w_high"]) / 100
    display_df = display_df[display_df["Close"] >= (threshold * display_df["52w_High"])]
    display_df = display_df[display_df["Circuit_Count"] <= filters["max_circuits"]]

    for col, key in [("Pos_Days_3M", "pos_days_3m"), ("Pos_Days_6M", "pos_days_6m"), ("Pos_Days_12M", "pos_days_12m")]:
        if filters[key] > 0:
            display_df = display_df[display_df[col].notna() & (display_df[col] >= filters[key])]

    if display_df.empty:
        st.warning("No stocks match the selected filters. Adjust criteria and try again.")
        return

    display_df["Avg_Sharpe"] = display_df.apply(lambda row: _calculate_avg_sharpe(row, filters["sort_method"]), axis=1)
    display_df = display_df[display_df["Avg_Sharpe"].notna()]

    if display_df.empty:
        st.warning("No stocks have valid Sharpe ratios for the selected sorting method.")
        return

    display_df = display_df.sort_values("Avg_Sharpe", ascending=False)
    display_df = display_df[
        [
            "Symbol",
            "Index",
            "Close",
            "Price Date",
            "Avg_Sharpe",
            "Volatility",
            "52w_High",
            "Vol_Median",
            "1Y_Change",
            "Pct_From_52W_High",
            "Circuit_Count",
        ]
    ]
    display_df = display_df.rename(
        columns={
            "Avg_Sharpe": "Sharpe",
            "Vol_Median": "Median Vol",
            "1Y_Change": "1Y Change",
            "Pct_From_52W_High": "% from 52wH",
            "Circuit_Count": "Circuit Close",
        }
    )
    display_df.insert(0, "Rank", range(1, len(display_df) + 1))

    c1, c2, c3 = st.columns(3)
    universe_label = (
        "All Indices"
        if len(selected_indices) == len(idx_options)
        else (", ".join(selected_indices) if selected_indices else "None")
    )
    c1.metric("Universe", universe_label)
    c2.metric(
        "Total in Universe",
        len(full_df),
        help="Stocks with ≥250 days of price history. See the 📋 Coverage tab for the full list of excluded symbols.",
    )
    c3.metric("Matches", len(display_df))

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Rank": st.column_config.NumberColumn("Rank", format="%d", width="small"),
            "Symbol": st.column_config.TextColumn("Symbol", width="medium"),
            "Index": st.column_config.TextColumn("Index", width="medium"),
            "Close": st.column_config.NumberColumn("Close (₹)", format="%.2f", width="small"),
            "Price Date": st.column_config.TextColumn("Price Date", width="small"),
            "Sharpe": st.column_config.NumberColumn("Sharpe", format="%.3f", width="small"),
            "Volatility": st.column_config.NumberColumn("Volatility (%)", format="%.1f%%", width="small"),
            "52w_High": st.column_config.NumberColumn("52w High", format="%.2f", width="small"),
            "Median Vol": st.column_config.NumberColumn("Median Vol", format="%,d", width="small"),
            "1Y Change": st.column_config.NumberColumn("1Y Change", format="%.2f%%", width="small"),
            "% from 52wH": st.column_config.NumberColumn("% from 52wH", format="%.2f%%", width="small"),
            "Circuit Close": st.column_config.NumberColumn("Circuit Close", format="%d", width="small"),
        },
        height=650,
    )

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Results",
        csv,
        file_name=f"momentum_screener_{datetime.now(IST).strftime('%Y%m%d')}.csv",
        mime="text/csv",
        width="stretch",
    )


# ──────────────────────────────────────────────
# DOCS
# ──────────────────────────────────────────────


@st.cache_resource
def _load_index_options() -> list[str]:
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        return []
    with open(const_path, "r") as f:
        return list(json.load(f).keys())


_DOCS_SECTIONS = {
    "Overview": "overview.md",
    "Stage 2 Screener": "stage2_screener.md",
    "Momentum Screener": "momentum_screener.md",
    "Phase Chart": "phase_chart.md",
    "Ichimoku Chart": "ichimoku_chart.md",
    "Data & Methodology": "data_methodology.md",
    "Momentum Backtest": "../backtest_user_guide.md",
}

_GUIDE_CSS = """
<style>
.guide-header {
    padding: 1.25rem 0 1rem 0;
    border-bottom: 1px solid rgba(148, 163, 184, 0.2);
    margin-bottom: 1.75rem;
}
.guide-crumb {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.09em;
    color: #94a3b8;
    font-weight: 600;
}
[data-testid="stMarkdownContainer"] table th {
    background: rgba(148, 163, 184, 0.08);
    padding: 0.45rem 0.8rem;
    border-bottom: 2px solid rgba(148, 163, 184, 0.25);
}
[data-testid="stMarkdownContainer"] table td {
    padding: 0.45rem 0.8rem;
    border-bottom: 1px solid rgba(148, 163, 184, 0.12);
}
[data-testid="stMarkdownContainer"] code:not(pre code) {
    background: rgba(148, 163, 184, 0.12);
    padding: 0.15em 0.4em;
    border-radius: 3px;
    font-size: 0.88em;
}
[data-testid="stMarkdownContainer"] pre {
    background: rgba(15, 23, 42, 0.55) !important;
    border: 1px solid rgba(148, 163, 184, 0.18);
    border-radius: 6px;
    padding: 0.9rem 1.1rem;
}
[data-testid="stMarkdownContainer"] blockquote {
    border-left: 3px solid #3b82f6;
    padding: 0.4rem 1rem;
    margin: 0.75rem 0;
    background: rgba(59, 130, 246, 0.07);
    border-radius: 0 4px 4px 0;
}
</style>
"""


def render_docs():
    st.markdown(_GUIDE_CSS, unsafe_allow_html=True)
    docs_dir = os.path.join(os.path.dirname(__file__), "docs")
    tabs = st.tabs(list(_DOCS_SECTIONS.keys()))
    for tab, (section, filename) in zip(tabs, _DOCS_SECTIONS.items()):
        with tab:
            path = os.path.join(docs_dir, filename)
            if not os.path.exists(path):
                st.error(f"Documentation file missing: {filename}")
                continue
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            lines = content.split("\n")
            if lines and lines[0].startswith("# "):
                content = "\n".join(lines[1:]).lstrip("\n")
            with st.container(border=True):
                st.markdown(content)


# ──────────────────────────────────────────────
# SIDEBAR PANELS
# ──────────────────────────────────────────────


def _sidebar_phase_chart() -> str:
    global _last_chart_ticker
    st.markdown("**Stock Symbol**")
    # Widget keys are removed from session state when not rendered (tab switch).
    # Restore explicitly from the persistent non-widget key or process-level fallback.
    if "chart_ticker_input" not in st.session_state:
        with _state_lock:
            proc_ticker = _last_chart_ticker
        restore = st.session_state.get("chart_ticker") or proc_ticker
        if restore:
            st.session_state["chart_ticker_input"] = restore
    chart_ticker = st.text_input("NSE Symbol (e.g. RELIANCE)", key="chart_ticker_input").strip().upper()
    if chart_ticker:
        with _state_lock:
            _last_chart_ticker = chart_ticker
    st.session_state["chart_ticker"] = chart_ticker
    return chart_ticker


def _sidebar_stage2() -> tuple[bool, bool]:
    st.markdown("**Filters**")
    rsi_toggle = st.toggle("RSI between 50–70", value=False, key="stage2_rsi_toggle")
    show_illiquid = st.toggle("Show Illiquid (Avg Vol < 1L)", value=False, key="stage2_show_illiquid")
    st.divider()
    if st.button("🚀 Run", type="primary", width="stretch", key="stage2_run_btn"):
        st.session_state["stage2_run_triggered"] = True
    return rsi_toggle, show_illiquid


def _sidebar_momentum() -> dict:
    st.markdown("**Filters**")
    sort_options = [
        "Average of 3/6/9/12 months",
        "Average of 1/3/6/9/12 months",
        "Average of 1/3/6/12 months",
        "Average of 1/3/12 months",
        "Average of 3/6 months",
        "1 year",
        "9 months",
        "6 months",
        "3 months",
    ]
    sort_method = st.selectbox("Sort by Sharpe", options=sort_options, index=0, key="mom_sort_method")
    min_annual_return = st.number_input(
        "Min Annual Return (%)",
        min_value=0.0,
        max_value=1000.0,
        value=7.0,
        step=0.1,
        format="%.2f",
        key="mom_min_annual_return",
    )
    pct_from_52w_high = st.number_input(
        "Within % of 52w High", min_value=0, max_value=100, value=25, step=1, key="mom_pct_from_52w_high"
    )
    max_circuits = st.number_input(
        "Max Circuits (1yr)", min_value=0, max_value=100, value=18, step=1, key="mom_max_circuits"
    )
    close_above_100dma = st.checkbox("Close > 100 DMA", value=False, key="mom_close_above_100dma")
    close_above_200dma = st.checkbox("Close > 200 DMA", value=True, key="mom_close_above_200dma")
    pos_days_3m = st.number_input(
        "Pos Days 3M (%)", min_value=0, max_value=100, value=45, step=1, key="mom_pos_days_3m"
    )
    pos_days_6m = st.number_input(
        "Pos Days 6M (%)", min_value=0, max_value=100, value=45, step=1, key="mom_pos_days_6m"
    )
    pos_days_12m = st.number_input(
        "Pos Days 12M (%)", min_value=0, max_value=100, value=45, step=1, key="mom_pos_days_12m"
    )
    st.divider()
    if st.button("🚀 Run", type="primary", width="stretch", key="mom_run_btn"):
        st.session_state["momentum_run_triggered"] = True
    return {
        "sort_method": sort_method,
        "min_annual_return": min_annual_return,
        "pct_from_52w_high": pct_from_52w_high,
        "max_circuits": max_circuits,
        "close_above_100dma": close_above_100dma,
        "close_above_200dma": close_above_200dma,
        "pos_days_3m": pos_days_3m,
        "pos_days_6m": pos_days_6m,
        "pos_days_12m": pos_days_12m,
    }


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────


_NAV_GROUPS = {
    "Technical Analysis": (
        "📊 Stage 2 Screener",
        "📈 Phase Chart",
        "☁️ Ichimoku Chart",
    ),
    "Momentum Factor": (
        "🚀 Momentum Screener",
        "⏱ Momentum Backtest",
        "📡 Live Signal",
    ),
    "Info Hub": (
        "📋 Coverage",
        "📚 User Guide",
    ),
}

_NAV_GROUP_ICONS = {
    "Technical Analysis": "📊",
    "Momentum Factor": "🚀",
    "Info Hub": "ℹ️",
}

_NAV_LABEL_ALIASES = {
    "📊 Stage 2": "📊 Stage 2 Screener",
    "🚀 Momentum": "🚀 Momentum Screener",
    "⏱ Backtest": "⏱ Momentum Backtest",
}


def _sidebar_navigation() -> str:
    """Render compact grouped popover menus and return the active page label."""
    valid_pages = {page for pages in _NAV_GROUPS.values() for page in pages}
    saved_page = st.session_state.get("active_screener", "📊 Stage 2 Screener")
    active_page = _NAV_LABEL_ALIASES.get(saved_page, saved_page)
    if active_page not in valid_pages:
        active_page = "📊 Stage 2 Screener"
    st.session_state["active_screener"] = active_page

    st.markdown("### 🧭 Navigation")
    for group_name, pages in _NAV_GROUPS.items():
        is_active_group = active_page in pages
        active_marker = "• " if is_active_group else ""
        menu_label = f"{active_marker}{_NAV_GROUP_ICONS[group_name]} {group_name}"
        with st.popover(menu_label, width="stretch"):
            for page in pages:
                if st.button(
                    page,
                    key=f"nav_{page}",
                    type="primary" if page == active_page else "secondary",
                    width="stretch",
                ):
                    st.session_state["active_screener"] = page
                    st.rerun()

    return active_page


def main():
    user_token = _get_user_token()
    idx_options = _load_index_options()

    bt_params: dict = {}
    ls_params: dict = {}
    rsi_toggle = False
    show_illiquid = False
    mom_filters: dict = {}

    if not _baseline_ok:
        st.warning(
            "⚠️ **screener_ohlcv.parquet not found** — first run will download ~2 years of data "
            "from Yahoo Finance. Run `python scripts/refresh_screener_parquet.py` to seed the "
            "baseline and commit it so future deploys start instantly."
        )

    with st.sidebar:
        screener = _sidebar_navigation()
        st.divider()

        selected_indices = []
        if screener not in (
            "📈 Phase Chart",
            "☁️ Ichimoku Chart",
            "📚 User Guide",
            "⏱ Momentum Backtest",
            "📋 Coverage",
            "📡 Live Signal",
        ):
            st.markdown("### 📦 Indices")
            cols = st.columns(2)
            for i, idx in enumerate(idx_options):
                if cols[i % 2].checkbox(idx, value=True, key=f"shared_idx_{idx}"):
                    selected_indices.append(idx)
            st.caption("💡 N50 + Next50 + Mid150 = LargeMidCap · Mid150 + Small250 = MidSmallCap · All = Total Market")

        if screener in ("📈 Phase Chart", "☁️ Ichimoku Chart"):
            _sidebar_phase_chart()
        elif screener == "📊 Stage 2 Screener":
            rsi_toggle, show_illiquid = _sidebar_stage2()
        elif screener == "🚀 Momentum Screener":
            mom_filters = _sidebar_momentum()
        elif screener == "📋 Coverage":
            pass
        elif screener == "⏱ Momentum Backtest":
            st.markdown("### ⏱ Momentum Backtest")
            bt_params = _sidebar_backtest(idx_options)
        elif screener == "📡 Live Signal":
            ls_params = _sidebar_live_signal(idx_options)

    # ── AUTOREFRESH — only while the active screener's job runs ──
    _kind_for_screener = {
        "📊 Stage 2 Screener": "stage2",
        "🚀 Momentum Screener": "momentum",
        "⏱ Momentum Backtest": "backtest",
    }
    _active_kind = _kind_for_screener.get(screener)
    if _active_kind:
        _active_job = registry.latest(user_token, _active_kind)
        _run_triggered = st.session_state.get(
            "backtest_run_triggered" if _active_kind == "backtest" else f"{_active_kind}_run_triggered",
            False,
        )
        if _run_triggered or (_active_job and _active_job.status in (JobStatus.RUNNING, JobStatus.QUEUED)):
            st_autorefresh(interval=1500, key="job_autorefresh")

    if screener == "📈 Phase Chart":
        ticker = st.session_state.get("chart_ticker", "")
        if not ticker:
            st.markdown('<p class="hero">📈 Stage 2 Phase Chart</p>', unsafe_allow_html=True)
            st.markdown(
                '<p class="sub-hero">Enter an NSE symbol in the sidebar to load the chart.</p>', unsafe_allow_html=True
            )
        else:
            col1, col2 = st.columns([0.85, 0.15])
            with col2:
                use_log_scale = st.toggle("Log Y-Axis", value=True, key="chart_log_scale_toggle")
            render_phase_chart(ticker, use_log_scale=use_log_scale)
    elif screener == "☁️ Ichimoku Chart":
        ticker = st.session_state.get("chart_ticker", "")
        chart_tab, basics_tab = st.tabs(["☁️ Chart", "📖 Basics"])
        with chart_tab:
            if not ticker:
                st.markdown('<p class="hero">☁️ Ichimoku Cloud Chart</p>', unsafe_allow_html=True)
                st.markdown(
                    '<p class="sub-hero">Enter an NSE symbol in the sidebar to load the chart.</p>',
                    unsafe_allow_html=True,
                )
            else:
                control_cols = [*st.columns(2), *st.columns(2)]
                with control_cols[0]:
                    timeframe = st.selectbox(
                        "Timeframe",
                        options=["Daily", "Weekly"],
                        index=0,
                        key="ichimoku_timeframe_select",
                    )
                with control_cols[1]:
                    use_log_scale = st.toggle("Log Y-Axis", value=True, key="ichimoku_log_scale_toggle")
                with control_cols[2]:
                    show_chikou = st.toggle("Show Chikou", value=True, key="ichimoku_chikou_toggle")
                with control_cols[3]:
                    show_crossovers = st.toggle("Show Crosses", value=True, key="ichimoku_crosses_toggle")
                render_ichimoku_chart(ticker, timeframe, use_log_scale, show_chikou, show_crossovers)
        with basics_tab:
            render_ichimoku_basics()
    elif screener == "📋 Coverage":
        coverage_results()
    elif screener == "📊 Stage 2 Screener":
        stage2_results(selected_indices, rsi_toggle, show_illiquid)
    elif screener == "⏱ Momentum Backtest":
        render_backtest_tabs(bt_params)
    elif screener == "📡 Live Signal":
        st.markdown('<p class="hero">📡 Live Signal</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="sub-hero">Weekly trade instructions from a momentum rebalance snapshot</p>',
            unsafe_allow_html=True,
        )
        live_signal_results(ls_params)
    elif screener == "📚 User Guide":
        render_docs()
    else:  # 🚀 Momentum Screener
        momentum_results(selected_indices, idx_options, mom_filters)


if __name__ == "__main__":
    main()
