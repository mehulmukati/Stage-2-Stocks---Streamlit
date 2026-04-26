#!/usr/bin/env python3
"""
Screener app — Stage 2, Momentum, Phase Chart. Parquet-backed, no external DB.
Backtest lives in app_backtest.py (separate parquet baseline).
"""

import difflib
import json
import os
import threading
import warnings
from datetime import datetime

import streamlit as st
from streamlit_autorefresh import st_autorefresh

warnings.filterwarnings("ignore", category=FutureWarning)

from app_backtest import _render_user_guide as _render_backtest_user_guide
from app_backtest import _sidebar_backtest, backtest_results
from charts import phase_chart_figure
from config import IST, SCREENER_OHLCV_PARQUET
from data import _load_constituents, _score_cache, fetch_chart_data
from jobs import JobStatus, registry
from momentum_engine import _calculate_avg_sharpe
from stage2_engine import compute_rolling_stage2 as _compute_rolling_stage2
from ui_helpers import _get_user_token, _poll_job
from workers import momentum_worker, stage2_worker


@st.cache_data(ttl=3600)
def compute_rolling_stage2(df):
    return _compute_rolling_stage2(df)


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


# ──────────────────────────────────────────────
# SHARED HELPERS
# ──────────────────────────────────────────────


def _render_source_banner(source: str, cache_date: str, count: int = None) -> None:
    suffix = f" · {count} stocks" if count is not None else ""
    if source == "memory":
        st.success(f"⚡ Served from memory cache for **{cache_date}**{suffix}.")
    elif source == "db":
        st.info(f"💾 Loaded from local database for **{cache_date}**{suffix}.")
    elif source == "internet":
        st.success(f"🌐 Fetched fresh EOD data and saved to database for **{cache_date}**{suffix}.")


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
    display_df = display_df[["Symbol", "Index", "Stage", "Score", "Close", "Volume", "Avg_Vol", "Vol_Ratio", "RSI"]]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cache Date", cache_date)
    c2.metric("Total Universe", len(df))
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

    c1, c2, c3 = st.columns(3)
    universe_label = (
        "All Indices"
        if len(selected_indices) == len(idx_options)
        else (", ".join(selected_indices) if selected_indices else "None")
    )
    c1.metric("Universe", universe_label)
    c2.metric("Total in Universe", len(full_df))
    c3.metric("Matches", len(display_df))

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


def main():
    user_token = _get_user_token()
    idx_options = _load_index_options()

    bt_params: dict = {}
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
        st.markdown("### 🖥 Screener")
        screener = st.radio(
            "Screener",
            options=["📊 Stage 2", "🚀 Momentum", "📈 Phase Chart", "⏱ Backtest", "📚 User Guide"],
            key="active_screener",
            horizontal=True,
            label_visibility="collapsed",
        )
        st.divider()

        selected_indices = []
        if screener not in ("📈 Phase Chart", "📚 User Guide", "⏱ Backtest"):
            st.markdown("### 📦 Indices")
            cols = st.columns(2)
            for i, idx in enumerate(idx_options):
                if cols[i % 2].checkbox(idx, value=True, key=f"shared_idx_{idx}"):
                    selected_indices.append(idx)
            st.caption("💡 N50 + Next50 + Mid150 = LargeMidCap · Mid150 + Small250 = MidSmallCap · All = Total Market")

        if screener == "📈 Phase Chart":
            _sidebar_phase_chart()
        elif screener == "📊 Stage 2":
            rsi_toggle, show_illiquid = _sidebar_stage2()
        elif screener == "🚀 Momentum":
            mom_filters = _sidebar_momentum()
        elif screener == "⏱ Backtest":
            st.markdown("### ⏱ Backtest")
            bt_params = _sidebar_backtest(idx_options)

    # ── AUTOREFRESH — only while the active screener's job runs ──
    _kind_for_screener = {"📊 Stage 2": "stage2", "🚀 Momentum": "momentum", "⏱ Backtest": "backtest"}
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
    elif screener == "📊 Stage 2":
        stage2_results(selected_indices, rsi_toggle, show_illiquid)
    elif screener == "⏱ Backtest":
        st.markdown('<p class="hero">⏱ Momentum Backtest</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="sub-hero">Classic vs Displacement Band Rule · '
            "Full vs Marginal Rebalance · Benchmarked vs Nifty 50 & Nifty 500</p>",
            unsafe_allow_html=True,
        )
        tab_bt, tab_guide = st.tabs(["📊 Backtest", "📖 User Guide"])
        with tab_bt:
            backtest_results(bt_params)
        with tab_guide:
            _render_backtest_user_guide()
    elif screener == "📚 User Guide":
        render_docs()
    else:  # 🚀 Momentum
        momentum_results(selected_indices, idx_options, mom_filters)


if __name__ == "__main__":
    main()
