#!/usr/bin/env python3
"""
Backtest app — parquet-backed, no DB dependency.

Deployed separately on Streamlit Cloud from app.py (screener).
Data pipeline: data_backtest.py (parquet + yfinance tail delta).
"""
import json
import os
import uuid
import warnings
from datetime import date as _date

import streamlit as st
from streamlit_autorefresh import st_autorefresh

warnings.filterwarnings("ignore", category=FutureWarning)

from dotenv import load_dotenv
load_dotenv()

from backtest_engine import rolling_returns
from charts import nav_chart_figure, rolling_returns_figure
from config import IST
from data_backtest import _load_constituents
from jobs import JobStatus, registry
from workers import backtest_worker


# ──────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="Momentum Backtest | Nifty 750", page_icon="⏱", layout="wide"
)
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
# SHARED HELPERS
# ──────────────────────────────────────────────
def _get_user_token() -> str:
    if "user_token" not in st.session_state:
        st.session_state["user_token"] = str(uuid.uuid4())
    return st.session_state["user_token"]


def _render_job_progress(job) -> None:
    _icons = {"info": "▸", "warning": "⚠️", "error": "❌", "success": "✅"}
    label = "⏳ Queued…" if job.status.value == "QUEUED" else "⏳ Running in background…"
    with st.container(border=True):
        st.markdown(f"**{label}**")
        for ev in list(job.events):
            st.write(f"{_icons.get(ev['level'], '▸')} {ev['msg']}")


def _poll_job(kind: str, worker, submit_params: dict = None) -> bool:
    user_token = _get_user_token()
    job_key_ss = f"{kind}_job_key"
    cache_ss = f"{kind}_cached_result"

    if st.session_state.pop(f"{kind}_run_triggered", False):
        job = registry.submit(user_token, kind, submit_params or {}, worker)
        st.session_state[job_key_ss] = job.key
        st.session_state.pop(cache_ss, None)

    job = registry.latest(user_token, kind)
    job_key = st.session_state.get(job_key_ss)
    if job is None or job.key != job_key:
        return False

    if job.status in (JobStatus.RUNNING, JobStatus.QUEUED):
        _render_job_progress(job)
        return True

    st.session_state.pop(job_key_ss, None)
    if job.status == JobStatus.DONE:
        st.session_state[cache_ss] = job.result
    elif job.status == JobStatus.ERROR:
        st.error(f"❌ {job.error}")
        return True
    return False


@st.cache_resource
def _load_index_options() -> list[str]:
    const_path = os.path.join(os.path.dirname(__file__), "constituents.json")
    if not os.path.exists(const_path):
        return []
    with open(const_path, "r") as f:
        return list(json.load(f).keys())


# ──────────────────────────────────────────────
# ROLLING WINDOW MAP
# ──────────────────────────────────────────────
_WINDOW_MAP = {
    "1 year": 252, "2 years": 504, "3 years": 756,
    "5 years": 1260, "7 years": 1764, "10 years": 2520,
}


# ──────────────────────────────────────────────
# USER GUIDE
# ──────────────────────────────────────────────
_GUIDE_PATH = os.path.join(os.path.dirname(__file__), "backtest_user_guide.md")
_GUIDE_TABS = [
    "Overview",
    "Entry & Exit Band (M / N)",
    "Classic vs Displacement",
    "Full vs Marginal Rebalance",
    "Ranking & Scoring",
    "Realism Settings",
]


def _render_user_guide() -> None:
    try:
        raw = open(_GUIDE_PATH, encoding="utf-8").read()
    except FileNotFoundError:
        st.error("User guide file not found: backtest_user_guide.md")
        return

    import re
    parts = re.split(r"^## (.+)$", raw, flags=re.MULTILINE)
    # parts = [preamble, title1, body1, title2, body2, ...]
    section: dict[str, str] = {}
    for i in range(1, len(parts), 2):
        section[parts[i].strip()] = parts[i + 1].strip()

    for tab, name in zip(st.tabs(_GUIDE_TABS), _GUIDE_TABS):
        with tab:
            content = section.get(name, "")
            if "<!-- warning -->" in content:
                before, after = content.split("<!-- warning -->", 1)
                st.markdown(before)
                st.warning(
                    "⚠️ **This is the most important realism control.** "
                    "Leave it ON unless you have a specific reason to test without it."
                )
                st.markdown(after)
            else:
                st.markdown(content)


# ──────────────────────────────────────────────
# RESULTS
# ──────────────────────────────────────────────
def backtest_results(params: dict):
    roll_label = params.pop("rolling_window", "3 years")

    if st.session_state.get("backtest_run_triggered"):
        if params["n"] <= params["m"]:
            st.session_state.pop("backtest_run_triggered", None)
            st.session_state["backtest_param_error"] = (
                "N (exit threshold) must be greater than M (entry threshold)."
            )
            return
        st.session_state.pop("backtest_param_error", None)
        st.session_state["bt_saved_params"] = {**params, "rolling_window": roll_label}

    if "backtest_param_error" in st.session_state:
        st.error(st.session_state["backtest_param_error"])
        return

    if _poll_job("backtest", backtest_worker, params):
        return

    result = st.session_state.get("backtest_cached_result")
    if result is None:
        st.info("Configure parameters in the sidebar and click **Run Backtest**.")
        return

    ohlcv_date = result.get("ohlcv_date")
    ohlcv_source = result.get("ohlcv_source")
    _source_icons = {
        "memory":        "⚡ session cache",
        "parquet":       "📦 bundled 10y parquet",
        "parquet+delta": "📦🌐 parquet + live delta",
        "error":         "❌",
    }
    source_label = _source_icons.get(ohlcv_source, ohlcv_source or "")
    if ohlcv_date:
        st.caption(f"OHLCV data as of **{ohlcv_date}** · {source_label}")

    nav_df   = result["nav"]
    stats_df = result["stats"]

    avg_turnover    = result.get("avg_turnover_pct", 0.0)
    total_cost_drag = result.get("total_cost_drag_pct", 0.0)
    turnover_str = (
        f"C {avg_turnover.get('Classic', 0):.1f}% / D {avg_turnover.get('Displacement', 0):.1f}%"
        if isinstance(avg_turnover, dict) else f"{avg_turnover:.1f}%"
    )
    drag_str = (
        f"C {total_cost_drag.get('Classic', 0):.2f}% / D {total_cost_drag.get('Displacement', 0):.2f}%"
        if isinstance(total_cost_drag, dict) else f"{total_cost_drag:.2f}%"
    )

    cols = st.columns(5)
    cols[0].metric("Trading Days",             len(result["trading_days"]))
    cols[1].metric("Rebalances",               len(result["rebalance_dates"]))
    cols[2].metric("Avg Turnover / Rebalance", turnover_str)
    cols[3].metric("Portfolio Size (M)",       result.get("m", params.get("m", "—")))
    cols[4].metric("Total Cost Drag",          drag_str)

    st.divider()

    st.subheader("Portfolio NAV (base = 100)")
    st.plotly_chart(nav_chart_figure(nav_df), width="stretch")

    roll_days      = _WINDOW_MAP.get(roll_label, 252)
    available_days = len(nav_df.dropna(how="all"))
    st.subheader(f"Rolling {roll_label} CAGR (%)")
    if roll_days >= available_days:
        st.warning(
            f"⚠️ Rolling window ({roll_label} = {roll_days} trading days) exceeds available data "
            f"({available_days} days). Select a shorter window or extend the backtest date range."
        )
    else:
        st.plotly_chart(rolling_returns_figure(rolling_returns(nav_df, roll_days)), width="stretch")

    st.subheader("Performance Summary")

    _PORTFOLIO_ROWS = [
        "Classic · Full", "Classic · Marginal",
        "Displacement · Full", "Displacement · Marginal",
    ]
    available_portfolio = [r for r in _PORTFOLIO_ROWS if r in stats_df.index]
    if available_portfolio:
        banner_parts = []
        for metric, label in [("CAGR (%)", "CAGR"), ("Sharpe", "Sharpe"), ("Calmar", "Calmar")]:
            if metric in stats_df.columns:
                col_vals = stats_df.loc[available_portfolio, metric].dropna()
                if not col_vals.empty:
                    banner_parts.append(f"**{label}** → {col_vals.idxmax()}")
        if banner_parts:
            st.info("🏆 Best strategy — " + " · ".join(banner_parts))

    st.dataframe(
        stats_df, width="stretch",
        column_config={
            "CAGR (%)":         st.column_config.NumberColumn("CAGR (%)",         format="%.2f%%"),
            "Sharpe":           st.column_config.NumberColumn("Sharpe",            format="%.3f"),
            "Max Drawdown (%)": st.column_config.NumberColumn("Max DD (%)",        format="%.2f%%"),
            "Calmar":           st.column_config.NumberColumn("Calmar",            format="%.3f"),
            "Sortino":          st.column_config.NumberColumn("Sortino",           format="%.3f"),
            "Avg Holdings":     st.column_config.NumberColumn("Avg Holdings",      format="%.1f"),
            "Avg Turnover (%)": st.column_config.NumberColumn("Avg Turnover (%)",  format="%.1f"),
            "Cost Drag (%)":    st.column_config.NumberColumn("Cost Drag (%)",     format="%.3f"),
            "Final NAV":        st.column_config.NumberColumn("Final NAV",         format="%.2f"),
        },
    )

    holdings_log = result.get("holdings_log", [])
    if isinstance(holdings_log, dict):
        for rule_name, log in holdings_log.items():
            with st.expander(f"Rebalance Log — {rule_name} (last 10)"):
                for entry in log[-10:][::-1]:
                    ins  = ", ".join(entry["entries"]) or "—"
                    outs = ", ".join(entry["exits"])   or "—"
                    st.markdown(
                        f"**{entry['date'].date()}** · {len(entry['holdings'])} stocks · "
                        f"**In:** {ins} · **Out:** {outs}"
                    )


# ──────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────
def _sidebar_backtest(idx_options: list[str]) -> dict:
    _s = st.session_state.get("bt_saved_params", {})

    _defaults: dict = {
        "bt_m":                ("m",                    20),
        "bt_n":                ("n",                    30),
        "bt_freq":             ("rebalance_freq",       "monthly"),
        "bt_sort":             ("sort_method",          "Average of 3/6/9/12 months"),
        "bt_rolling":          ("rolling_window",       "1 year"),
        "bt_min_history":      ("min_history_days",     252),
        "bt_cost_pct":         ("transaction_cost_pct", 0.1),
        "bt_use_compositions": ("use_compositions",     True),
    }
    for _wk, (_pk, _fallback) in _defaults.items():
        if _wk not in st.session_state:
            st.session_state[_wk] = _s.get(_pk, _fallback)

    if "bt_start" not in st.session_state:
        st.session_state["bt_start"] = (
            _date.fromisoformat(_s["start_date"]) if "start_date" in _s else _date(2021, 1, 1)
        )
    if "bt_end" not in st.session_state:
        st.session_state["bt_end"] = (
            _date.fromisoformat(_s["end_date"]) if "end_date" in _s else _date.today()
        )
    for _idx in idx_options:
        _ck = f"bt_idx_{_idx}"
        if _ck not in st.session_state:
            st.session_state[_ck] = _idx in _s.get("universe", idx_options)

    st.markdown("**Portfolio Parameters**")
    bt_m    = st.number_input("Entry threshold M (top-M enters)", min_value=1,  max_value=200, step=1,  key="bt_m")
    bt_n    = st.number_input("Exit threshold N (exits if > N)",  min_value=2,  max_value=300, step=1,  key="bt_n")
    bt_freq = st.selectbox("Rebalance frequency", ["weekly", "biweekly", "monthly"], key="bt_freq")
    bt_sort = st.selectbox(
        "Rank by Sharpe",
        ["Average of 3/6/9/12 months", "Average of 3/6 months",
         "1 year", "9 months", "6 months", "3 months"],
        key="bt_sort",
    )

    st.markdown("**Universe**")
    bt_universe   = []
    bt_idx_cols = st.columns(2)
    for i, idx in enumerate(idx_options):
        if bt_idx_cols[i % 2].checkbox(idx, key=f"bt_idx_{idx}"):
            bt_universe.append(idx)

    st.markdown("**Date Range**")
    bt_start   = st.date_input("Start date", key="bt_start")
    bt_end     = st.date_input("End date",   key="bt_end")
    bt_rolling = st.selectbox(
        "Rolling return window",
        ["1 year", "2 years", "3 years", "5 years", "7 years", "10 years"],
        key="bt_rolling",
    )

    st.markdown("**Realism Settings**")
    bt_min_history = st.number_input(
        "Min history (trading days)", min_value=63, max_value=1260, step=21, key="bt_min_history",
        help="Minimum trading days of data a stock must have before it can be ranked. 252 ≈ 1 year.",
    )
    bt_cost_pct = st.slider(
        "Transaction cost per trade (%)", min_value=0.0, max_value=1.0, step=0.05, key="bt_cost_pct",
        help="One-way cost applied to each stock traded at rebalance (slippage + brokerage).",
    )
    bt_use_compositions = st.toggle(
        "Use historical constituents (anti-survivorship)", key="bt_use_compositions",
        help="Filter the universe to stocks that were actually in the index at each rebalance date.",
    )
    st.divider()
    if st.button("▶ Run Backtest", type="primary", width="stretch", key="bt_run_btn"):
        st.session_state["backtest_run_triggered"] = True

    return {
        "m":                    bt_m,
        "n":                    bt_n,
        "rebalance_freq":       bt_freq,
        "sort_method":          bt_sort,
        "universe":             bt_universe,
        "start_date":           bt_start.strftime("%Y-%m-%d"),
        "end_date":             bt_end.strftime("%Y-%m-%d"),
        "rolling_window":       bt_rolling,
        "transaction_cost_pct": bt_cost_pct,
        "use_compositions":     bt_use_compositions,
        "min_history_days":     bt_min_history,
    }


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    user_token  = _get_user_token()
    idx_options = _load_index_options()

    with st.sidebar:
        st.markdown("### ⏱ Backtest")
        bt_params = _sidebar_backtest(idx_options)

    # Autorefresh while a backtest job is running
    active_job    = registry.latest(user_token, "backtest")
    run_triggered = st.session_state.get("backtest_run_triggered", False)
    if run_triggered or (active_job and active_job.status in (JobStatus.RUNNING, JobStatus.QUEUED)):
        st_autorefresh(interval=1500, key="job_autorefresh")

    st.markdown('<p class="hero">⏱ Momentum Backtest</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-hero">Classic vs Displacement Band Rule · '
        'Full vs Marginal Rebalance · Benchmarked vs Nifty 50 & Nifty 500</p>',
        unsafe_allow_html=True,
    )

    tab_bt, tab_guide = st.tabs(["📊 Backtest", "📖 User Guide"])
    with tab_bt:
        backtest_results(bt_params)
    with tab_guide:
        _render_user_guide()


if __name__ == "__main__":
    main()
