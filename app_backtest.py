#!/usr/bin/env python3
"""
Backtest app — parquet-backed, no DB dependency.

Deployed separately on Streamlit Cloud from app.py (screener).
Data pipeline: data_backtest.py (parquet + yfinance tail delta).
"""

import json
import logging
import os
import re
import warnings
from datetime import date as _date

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from backtest_engine import _compute_summary_stats, rolling_returns
from charts import nav_chart_figure, portfolio_churn_figure, portfolio_weights_figure, rolling_returns_figure
from jobs import JobStatus, registry
from ui_helpers import _get_user_token, _poll_job
from workers import backtest_worker


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
    "1 year": 252,
    "2 years": 504,
    "3 years": 756,
    "5 years": 1260,
    "7 years": 1764,
    "10 years": 2520,
}


# ──────────────────────────────────────────────
# USER GUIDE
# ──────────────────────────────────────────────
_GUIDE_PATH = os.path.join(os.path.dirname(__file__), "backtest_user_guide.md")


def _render_user_guide() -> None:
    try:
        raw = open(_GUIDE_PATH, encoding="utf-8").read()
    except FileNotFoundError:
        st.error("User guide file not found: backtest_user_guide.md")
        return

    parts = re.split(r"^## (.+)$", raw, flags=re.MULTILINE)
    # parts = [preamble, title1, body1, title2, body2, ...]
    sections: dict[str, str] = {}
    for i in range(1, len(parts), 2):
        sections[parts[i].strip()] = parts[i + 1].strip()

    if not sections:
        st.warning("No sections found in user guide.")
        return

    for tab, (name, content) in zip(st.tabs(list(sections.keys())), sections.items()):
        with tab:
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
            st.session_state["backtest_param_error"] = "N (exit threshold) must be greater than M (entry threshold)."
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
        "memory": "⚡ session cache",
        "parquet": "📦 bundled 10y parquet",
        "parquet+delta": "📦🌐 parquet + live delta",
        "error": "❌",
    }
    source_label = _source_icons.get(ohlcv_source, ohlcv_source or "")
    if ohlcv_date:
        st.caption(f"OHLCV data as of **{ohlcv_date}** · {source_label}")

    nav_df = result["nav"]
    stats_df = result["stats"]

    avg_turnover = result.get("avg_turnover_pct", 0.0)
    total_cost_drag = result.get("total_cost_drag_pct", 0.0)
    turnover_str = (
        f"C {avg_turnover.get('Classic', 0):.1f}% / D {avg_turnover.get('Displacement', 0):.1f}%"
        if isinstance(avg_turnover, dict)
        else f"{avg_turnover:.1f}%"
    )
    drag_str = (
        f"C {total_cost_drag.get('Classic', 0):.2f}% / D {total_cost_drag.get('Displacement', 0):.2f}%"
        if isinstance(total_cost_drag, dict)
        else f"{total_cost_drag:.2f}%"
    )

    cols = st.columns(5)
    cols[0].metric("Trading Days", len(result["trading_days"]))
    cols[1].metric("Rebalances", len(result["rebalance_dates"]))
    cols[2].metric("Avg Turnover / Rebalance", turnover_str)
    cols[3].metric("Portfolio Size (M)", result.get("m", params.get("m", "—")))
    cols[4].metric("Total Cost Drag", drag_str)

    st.divider()

    st.subheader("Portfolio NAV (base = 100)")
    st.plotly_chart(nav_chart_figure(nav_df), width="stretch")

    roll_days = _WINDOW_MAP.get(roll_label, 252)
    available_days = len(nav_df.dropna(how="all"))
    st.subheader(f"Rolling {roll_label} CAGR (%)")
    if roll_days >= available_days:
        st.warning(
            f"⚠️ Rolling window ({roll_label} = {roll_days} trading days) exceeds available data "
            f"({available_days} days). Select a shorter window or extend the backtest date range."
        )
    else:
        st.plotly_chart(rolling_returns_figure(rolling_returns(nav_df, roll_days)), width="stretch")

    churn_log = result.get("holdings_log", {})
    if isinstance(churn_log, dict) and churn_log:
        st.subheader("Portfolio Churn per Rebalance")
        st.plotly_chart(portfolio_churn_figure(churn_log), width="stretch")

        st.subheader("Portfolio Weights per Rebalance")
        for rule_name in ("Classic", "Displacement"):
            entries = churn_log.get(rule_name, [])
            if entries:
                _show_weights_chart(entries, rule_name)

    st.subheader("Performance Summary")

    _PORTFOLIO_ROWS = [
        "Classic · Full",
        "Classic · Marginal",
        "Classic · Prop",
        "Displacement · Full",
        "Displacement · Marginal",
        "Displacement · Prop",
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
        stats_df,
        width="stretch",
        column_config={
            "CAGR (%)": st.column_config.NumberColumn("CAGR (%)", format="%.2f%%"),
            "Sharpe": st.column_config.NumberColumn("Sharpe", format="%.3f"),
            "Max Drawdown (%)": st.column_config.NumberColumn("Max DD (%)", format="%.2f%%"),
            "Calmar": st.column_config.NumberColumn("Calmar", format="%.3f"),
            "Sortino": st.column_config.NumberColumn("Sortino", format="%.3f"),
            "Avg Holdings": st.column_config.NumberColumn("Avg Holdings", format="%.1f"),
            "Avg Turnover (%)": st.column_config.NumberColumn("Avg Turnover (%)", format="%.1f"),
            "Cost Drag (%)": st.column_config.NumberColumn("Cost Drag (%)", format="%.3f"),
            "Tax Drag (%)": st.column_config.NumberColumn("Tax Drag (%)", format="%.3f"),
            "Brokerage Drag (%)": st.column_config.NumberColumn("Brok Drag (%)", format="%.3f"),
            "Final NAV": st.column_config.NumberColumn("Final NAV", format="%.2f"),
        },
    )

    dl_log = result.get("holdings_log", {})
    if isinstance(dl_log, dict) and dl_log:

        def _fmt_weights(w_dict: dict, holdings: list) -> str:
            """Serialise weight dict as 'TICKER:X.XX%; ...' in holdings order."""
            if not w_dict:
                return ""
            return "; ".join(f"{s}:{w_dict.get(s, 0.0):.4f}%" for s in holdings)

        dl_rows = []
        for rule_name, log in dl_log.items():
            for rebal_idx, entry in enumerate(log, start=1):
                holdings = entry["holdings"]  # already sorted
                fw = entry.get("full_weights", {})
                mw = entry.get("marg_weights", {})
                pw = entry.get("prop_weights", {})
                dl_rows.append(
                    {
                        "Rebalance #": rebal_idx,
                        "Date": entry["date"].date(),
                        "Band Rule": rule_name,
                        "#Holdings": len(holdings),
                        "#Entries": len(entry["entries"]),
                        "#Exits": len(entry["exits"]),
                        "Full Turnover %": entry.get("full_turnover_pct", ""),
                        "Marginal Turnover %": entry.get("marg_turnover_pct", ""),
                        "Prop Turnover %": entry.get("prop_turnover_pct", ""),
                        "Entries (tickers)": "; ".join(entry["entries"]),
                        "Exits (tickers)": "; ".join(entry["exits"]),
                        "Holdings (Full Weights %)": _fmt_weights(fw, holdings),
                        "Holdings (Marg Weights %)": _fmt_weights(mw, holdings),
                        "Holdings (Prop Weights %)": _fmt_weights(pw, holdings),
                        "Valid Universe Size": entry.get("valid_universe_size", ""),
                    }
                )
        dl_csv = pd.DataFrame(dl_rows).to_csv(index=False).encode("utf-8")
        _p = st.session_state.get("bt_saved_params", params)
        _fname = (
            f"backtest_rebalance_log_{_p.get('m', 'M')}_{_p.get('n', 'N')}_"
            f"{_p.get('rebalance_freq', 'freq')}_"
            f"{_p.get('start_date', '')}_to_{_p.get('end_date', '')}.csv"
        )
        st.download_button(
            "📥 Download Full Rebalance Log",
            dl_csv,
            file_name=_fname,
            mime="text/csv",
            width="stretch",
        )

    holdings_log = result.get("holdings_log", [])
    if isinstance(holdings_log, dict):
        for rule_name, log in holdings_log.items():
            with st.expander(f"Rebalance Log — {rule_name} (last 10)"):
                for entry in log[-10:][::-1]:
                    ins = ", ".join(entry["entries"]) or "—"
                    outs = ", ".join(entry["exits"]) or "—"
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
        "bt_m": ("m", 20),
        "bt_n": ("n", 30),
        "bt_freq": ("rebalance_freq", "monthly"),
        "bt_sort": ("sort_method", "Average of 3/6/9/12 months"),
        "bt_rolling": ("rolling_window", "1 year"),
        "bt_min_history": ("min_history_days", 252),
        "bt_cost_pct": ("transaction_cost_pct", 0.1),
        "bt_use_compositions": ("use_compositions", True),
        "bt_initial_capital": ("initial_capital", 1_000_000),
        "bt_brokerage_per_sale": ("brokerage_per_sale", 0.0),
        "bt_stcg_rate": ("stcg_rate", 20.0),
        "bt_ltcg_rate": ("ltcg_rate", 12.5),
        "bt_stage2_drop_exit": ("stage2_drop_exit", False),
        "bt_stage2_drop_threshold": ("stage2_drop_threshold", 2),
        "bt_stage2_entry_filter": ("stage2_entry_filter", False),
        "bt_stage2_entry_threshold": ("stage2_entry_threshold", 2),
    }
    for _wk, (_pk, _fallback) in _defaults.items():
        if _wk not in st.session_state:
            st.session_state[_wk] = _s.get(_pk, _fallback)

    if "bt_start" not in st.session_state:
        st.session_state["bt_start"] = (
            _date.fromisoformat(_s["start_date"]) if "start_date" in _s else _date(2021, 1, 1)
        )
    if "bt_end" not in st.session_state:
        st.session_state["bt_end"] = _date.fromisoformat(_s["end_date"]) if "end_date" in _s else _date.today()
    for _idx in idx_options:
        _ck = f"bt_idx_{_idx}"
        if _ck not in st.session_state:
            st.session_state[_ck] = _idx in _s.get("universe", idx_options)

    st.markdown("**Portfolio Parameters**")
    bt_m = st.number_input("Entry threshold M (top-M enters)", min_value=1, max_value=200, step=1, key="bt_m")
    bt_n = st.number_input("Exit threshold N (exits if > N)", min_value=2, max_value=300, step=1, key="bt_n")
    bt_freq = st.selectbox(
        "Rebalance frequency",
        ["weekly", "biweekly", "monthly", "quarterly", "half-yearly"],
        key="bt_freq",
    )
    if bt_freq == "weekly":
        with st.expander("Weekly Stage 2 signals", expanded=False):
            st.markdown("**Entry filter**")
            bt_stage2_entry_filter = st.toggle(
                "Enter on Stage 2 score jump",
                key="bt_stage2_entry_filter",
                help="Also allow a stock to enter if its Weinstein Stage 2 score (0–8) rises by the threshold "
                "or more since last week — even if it isn't in the top-M momentum rank. "
                "In displacement mode, Stage 2 jumpers compete with top-M entrants for freed slots "
                "(hard cap of M is preserved).",
            )
            bt_stage2_entry_threshold = st.number_input(
                "Score jump threshold",
                min_value=1,
                max_value=4,
                step=1,
                key="bt_stage2_entry_threshold",
                help="Stage 2 points that must rise in one week to trigger entry (e.g. 2 means score 4→6).",
                disabled=not bt_stage2_entry_filter,
            )
            st.markdown("**Exit signal**")
            bt_stage2_drop_exit = st.toggle(
                "Exit on Stage 2 score drop",
                key="bt_stage2_drop_exit",
                help="Also exit a held stock if its Weinstein Stage 2 score drops by the threshold "
                "or more versus last week's score.",
            )
            bt_stage2_drop_threshold = st.number_input(
                "Score drop threshold",
                min_value=1,
                max_value=4,
                step=1,
                key="bt_stage2_drop_threshold",
                help="Stage 2 points that must fall in one week to trigger exit (e.g. 2 means score 6→4).",
                disabled=not bt_stage2_drop_exit,
            )
    else:
        bt_stage2_entry_filter = False
        bt_stage2_entry_threshold = st.session_state.get("bt_stage2_entry_threshold", 2)
        bt_stage2_drop_exit = False
        bt_stage2_drop_threshold = st.session_state.get("bt_stage2_drop_threshold", 2)

    bt_sort = st.selectbox(
        "Rank by Sharpe",
        [
            "Average of 3/6/9/12 months",
            "Average of 1/3/6/9/12 months",
            "Average of 1/3/6/12 months",
            "Average of 1/3/12 months",
            "Average of 3/6 months",
            "1 year",
            "9 months",
            "6 months",
            "3 months",
        ],
        key="bt_sort",
    )

    st.markdown("**Universe**")
    bt_universe = []
    bt_idx_cols = st.columns(2)
    for i, idx in enumerate(idx_options):
        if bt_idx_cols[i % 2].checkbox(idx, key=f"bt_idx_{idx}"):
            bt_universe.append(idx)

    st.markdown("**Date Range**")
    bt_start = st.date_input("Start date", key="bt_start")
    bt_end = st.date_input("End date", key="bt_end")
    bt_rolling = st.selectbox(
        "Rolling return window",
        ["1 year", "2 years", "3 years", "5 years", "7 years", "10 years"],
        key="bt_rolling",
    )

    st.markdown("**Realism Settings**")
    bt_min_history = st.number_input(
        "Min history (trading days)",
        min_value=63,
        max_value=1260,
        step=21,
        key="bt_min_history",
        help="Minimum trading days of data a stock must have before it can be ranked. 252 ≈ 1 year.",
    )
    bt_cost_pct = st.slider(
        "Transaction cost per trade (%)",
        min_value=0.0,
        max_value=1.0,
        step=0.05,
        key="bt_cost_pct",
        help="One-way cost applied to each stock traded at rebalance (slippage + brokerage).",
    )
    bt_max_position_pct = st.slider(
        "Max position size (%)",
        min_value=0,
        max_value=50,
        step=1,
        key="bt_max_position_pct",
        help=(
            "Maximum weight any single stock may hold after rebalance (%). "
            "Excess above this cap is redistributed proportionally to all smaller positions. "
            "0 = no cap (default). Recommended: 15% for Marginal/Prop variants to prevent "
            "concentration from large exits funnelling weight into a single entrant."
        ),
    )
    bt_use_compositions = st.toggle(
        "Use historical constituents (anti-survivorship)",
        key="bt_use_compositions",
        help="Filter the universe to stocks that were actually in the index at each rebalance date.",
    )
    with st.expander("India Tax & Brokerage", expanded=False):
        bt_india_on = st.toggle("Enable India-specific costs", value=False, key="bt_india_on")
        if bt_india_on:
            bt_initial_capital = st.number_input(
                "Initial capital (INR)",
                min_value=100_000,
                max_value=100_000_000,
                value=st.session_state.get("bt_initial_capital", 1_000_000),
                step=100_000,
                key="bt_initial_capital",
                format="%d",
                help="Used to convert flat Rs brokerage per sale into NAV drag.",
            )
            bt_brokerage_per_sale = st.number_input(
                "Brokerage per sale (Rs)",
                min_value=0.0,
                max_value=100.0,
                value=st.session_state.get("bt_brokerage_per_sale", 15.0),
                step=1.0,
                key="bt_brokerage_per_sale",
                help="Flat charge per stock sold (exits only). No charge on buys.",
            )
            bt_stcg_rate = st.slider(
                "STCG rate (%)",
                min_value=0.0,
                max_value=30.0,
                value=st.session_state.get("bt_stcg_rate", 20.0),
                step=0.5,
                key="bt_stcg_rate",
                help="Tax on gains from holdings held ≤ 12 calendar months. Current India rate: 20%.",
            )
            bt_ltcg_rate = st.slider(
                "LTCG rate (%)",
                min_value=0.0,
                max_value=20.0,
                value=st.session_state.get("bt_ltcg_rate", 12.5),
                step=0.5,
                key="bt_ltcg_rate",
                help="Tax on gains from holdings held > 12 calendar months. Current India rate: 12.5%.",
            )
        else:
            bt_initial_capital = st.session_state.get("bt_initial_capital", 1_000_000)
            bt_brokerage_per_sale = 0.0
            bt_stcg_rate = 0.0
            bt_ltcg_rate = 0.0
    st.divider()
    if st.button("▶ Run Backtest", type="primary", width="stretch", key="bt_run_btn"):
        st.session_state["backtest_run_triggered"] = True

    return {
        "m": bt_m,
        "n": bt_n,
        "rebalance_freq": bt_freq,
        "sort_method": bt_sort,
        "universe": bt_universe,
        "start_date": bt_start.strftime("%Y-%m-%d"),
        "end_date": bt_end.strftime("%Y-%m-%d"),
        "rolling_window": bt_rolling,
        "transaction_cost_pct": bt_cost_pct,
        "use_compositions": bt_use_compositions,
        "min_history_days": bt_min_history,
        "initial_capital": bt_initial_capital,
        "brokerage_per_sale": bt_brokerage_per_sale,
        "stcg_rate": bt_stcg_rate / 100.0,
        "ltcg_rate": bt_ltcg_rate / 100.0,
        "stage2_drop_exit": bt_stage2_drop_exit,
        "stage2_drop_threshold": int(bt_stage2_drop_threshold),
        "stage2_entry_filter": bt_stage2_entry_filter,
        "stage2_entry_threshold": int(bt_stage2_entry_threshold),
        "max_position_pct": bt_max_position_pct if bt_max_position_pct > 0 else None,
    }


# ──────────────────────────────────────────────
# WEIGHTS CHART DISPLAY HELPER
# ──────────────────────────────────────────────
_WEIGHTS_INLINE_THRESHOLD = 50


_WT_LABELS = {"full": "Full (equal)", "marg": "Marginal (slot-fill)", "prop": "Prop (prop-fill)"}


def _show_weights_chart(entries: list[dict], rule_name: str, file_stem: str = "") -> None:
    """Render 2 charts (full + marg) for one rule, inline or as HTML downloads."""
    large = len(entries) >= _WEIGHTS_INLINE_THRESHOLD
    if large:
        st.info(
            f"**{rule_name}** has {len(entries)} rebalances — too large to render inline. "
            "Download the interactive charts to view them in your browser."
        )

    for wt in ("full", "marg", "prop"):
        fig = portfolio_weights_figure(entries, rule_name, weight_type=wt)
        label = _WT_LABELS[wt]
        if not large:
            st.plotly_chart(fig, width="stretch")
        else:
            fname = f"{file_stem or rule_name.lower()}_{wt}_weights.html"
            html_bytes = fig.to_html(full_html=True, include_plotlyjs="cdn").encode("utf-8")
            st.download_button(
                label=f"⬇ Download {rule_name} · {label} weights (HTML)",
                data=html_bytes,
                file_name=fname,
                mime="text/html",
                key=f"dl_weights_{rule_name}_{wt}_{file_stem}",
            )


# ──────────────────────────────────────────────
# WEIGHTS CSV UPLOAD
# ──────────────────────────────────────────────
def _parse_rebalance_csv(rule_df: pd.DataFrame) -> list[dict]:
    """Convert rebalance log CSV rows for one band rule into holdings_log entry format."""

    def _parse_weights(cell) -> dict[str, float]:
        out: dict[str, float] = {}
        if not isinstance(cell, str) or not cell.strip():
            return out
        for part in cell.split(";"):
            part = part.strip()
            if ":" in part:
                ticker, val = part.split(":", 1)
                try:
                    out[ticker.strip()] = float(val.replace("%", "").strip())
                except ValueError:
                    pass
        return out

    entries = []
    for _, row in rule_df.iterrows():
        entries.append(
            {
                "date": pd.Timestamp(row["Date"]),
                "full_weights": _parse_weights(row.get("Holdings (Full Weights %)", "")),
                "marg_weights": _parse_weights(row.get("Holdings (Marg Weights %)", "")),
            }
        )
    return entries


def _render_weights_csv_uploader() -> None:
    st.divider()
    st.subheader("📊 Portfolio Weights — Upload Rebalance Log")
    uploaded = st.file_uploader(
        "Upload a downloaded rebalance log CSV to visualise portfolio weights",
        type="csv",
        key="weights_csv",
    )
    if not uploaded:
        return

    with st.status("Building weight charts…", expanded=True) as status:
        st.write("Parsing CSV…")
        try:
            df = pd.read_csv(uploaded)
        except Exception as exc:
            status.update(label="Failed to parse CSV", state="error")
            st.error(f"Could not parse CSV: {exc}")
            return

        rendered = False
        for rule_name in ("Classic", "Displacement"):
            rule_df = df[df["Band Rule"] == rule_name]
            if rule_df.empty:
                continue
            st.write(f"Building {rule_name} charts ({len(rule_df)} rebalances)…")
            entries = _parse_rebalance_csv(rule_df)
            if entries:
                _show_weights_chart(
                    entries, rule_name, file_stem=uploaded.name.replace(".csv", f"_{rule_name.lower()}")
                )
                rendered = True

        if not rendered:
            status.update(label="No data found", state="error")
            st.warning("No Classic or Displacement rows found in the uploaded CSV.")
        else:
            status.update(label="Charts ready", state="complete", expanded=False)


# ──────────────────────────────────────────────
# DEBUG TAB
# ──────────────────────────────────────────────


def _render_debug_tab(result: dict | None) -> None:
    """Portfolio Debugger: explain why a stock was or wasn't held on a given rebalance date."""
    st.markdown("### 🔍 Portfolio Debugger")
    st.caption("Select a rebalance date and a stock ticker to understand the portfolio decision on that date.")

    if not result:
        st.info("Run a backtest first, then use this tab to inspect individual portfolio decisions.")
        return

    holdings_log = result.get("holdings_log", {})
    if not isinstance(holdings_log, dict) or not holdings_log:
        st.warning("No holdings log available.")
        return

    m = result.get("m", "?")

    c1, c2, c3 = st.columns([1, 2, 2])
    with c1:
        rule = st.selectbox("Band rule", [r for r in ("Classic", "Displacement") if r in holdings_log], key="dbg_rule")
    log = holdings_log.get(rule, [])
    if not log:
        st.warning(f"No rebalance log for {rule}.")
        return

    date_options = [str(e["date"].date()) for e in reversed(log)]
    with c2:
        chosen_date_str = st.selectbox("Rebalance date", date_options, key="dbg_date")
    with c3:
        ticker = st.text_input("Stock ticker (e.g. RELIANCE)", key="dbg_ticker").strip().upper()

    entry = next((e for e in log if str(e["date"].date()) == chosen_date_str), None)
    if entry is None:
        st.error("Could not find the selected date in the log.")
        return

    holdings = entry["holdings"]
    full_ranking: list[str] = entry.get("full_ranking", [])
    universe_size = entry.get("valid_universe_size", "?")

    st.divider()

    # ── Top-10 ranked stocks table (always shown for context) ──
    if full_ranking:
        top10 = full_ranking[:10]
        top10_df = __import__("pandas").DataFrame(
            [
                {
                    "Rank": i + 1,
                    "Symbol": sym,
                    "Held": "✅" if sym in holdings else "",
                }
                for i, sym in enumerate(top10)
            ]
        )
        st.markdown(f"**Top-10 ranked on {chosen_date_str}** (M={m}, universe={universe_size} symbols)")
        st.dataframe(top10_df, hide_index=True, use_container_width=False, width=340)
    else:
        st.info("No ranking data available for this rebalance date (backtest run before this feature was added).")

    if not ticker:
        return

    st.divider()
    st.markdown(f"#### Decision for **{ticker}** on {chosen_date_str}")

    if ticker in holdings:
        rank_pos = (full_ranking.index(ticker) + 1) if ticker in full_ranking else None
        st.success(f"✅ **Held** — {ticker} was in the portfolio.")
        weights = {
            "Full Rebalance": entry.get("full_weights", {}).get(ticker),
            "Marginal Rebalance": entry.get("marg_weights", {}).get(ticker),
            "Prop Rebalance": entry.get("prop_weights", {}).get(ticker),
        }
        if rank_pos:
            st.markdown(f"Rank: **#{rank_pos}** out of {len(full_ranking)} scored symbols")
        cols = st.columns(3)
        for col, (variant, w) in zip(cols, weights.items()):
            col.metric(variant, f"{w:.2f}%" if w is not None else "—")

    elif ticker in full_ranking:
        rank_pos = full_ranking.index(ticker) + 1
        n_held = len(holdings)
        st.warning(f"🟡 **Ranked but not held** — {ticker} passed all filters but its rank was too low.")
        st.markdown(
            f"- Rank: **#{rank_pos}** out of {len(full_ranking)} scored symbols\n"
            f"- Enters portfolio only if rank ≤ M = **{m}**\n"
            f"- Portfolio held **{n_held}** stocks on this date"
        )

    else:
        _EXCLUSION_REASON_LABELS = {
            "insufficient_history": "Insufficient history — fewer than the configured minimum trading days"
            " before this date",
            "missing_data": "Missing data — >5% of close prices were missing",
            "low_volume": "Low volume — median daily volume below the minimum threshold",
            "no_valid_score": "No valid Sharpe score — could not compute a momentum score",
            "no_data": "No OHLCV data — symbol not found in the backtest dataset",
        }
        index_universe: set[str] = set(entry.get("index_universe") or [])
        excluded_reasons: dict[str, str] = entry.get("excluded_reasons") or {}

        st.error(f"🔴 **Excluded before ranking** — {ticker} did not pass the pre-ranking filters.")

        if index_universe and ticker not in index_universe:
            st.markdown(
                "**Reason:** Not in index — not a constituent of the selected indices on this date "
                "(checked via compositions.parquet)"
            )
        elif ticker in excluded_reasons:
            label = _EXCLUSION_REASON_LABELS.get(excluded_reasons[ticker], excluded_reasons[ticker])
            st.markdown(f"**Reason:** {label}")
        else:
            st.markdown("Specific reason unavailable — re-run the backtest to capture per-symbol diagnostics.")

        st.caption(
            f"Ranked universe: **{len(full_ranking)}** symbols · " f"Valid index universe: **{universe_size}** symbols"
        )


# ──────────────────────────────────────────────
# WALK-FORWARD TAB
# ──────────────────────────────────────────────


def _render_walkforward_tab(result: dict | None, params: dict) -> None:
    """Walk-Forward Validation: split NAV into in-sample and out-of-sample windows."""
    import pandas as pd

    st.markdown("### 📐 Walk-Forward Validation")
    st.caption(
        "Split the backtest period at a chosen date and compare performance in each window. "
        "If out-of-sample numbers are materially worse, the strategy may be overfit to the calibration window."
    )

    if not result:
        st.info("Run a backtest first, then use this tab to split the results into calibration and forward windows.")
        return

    nav_df: pd.DataFrame = result["nav"]
    bt_start = pd.Timestamp(params["start_date"])
    bt_end = pd.Timestamp(params["end_date"])
    midpoint = bt_start + (bt_end - bt_start) / 2

    split_date = st.date_input(
        "Split date (in-sample ends here; out-of-sample begins the next day)",
        value=midpoint.date(),
        min_value=(bt_start + pd.Timedelta(days=30)).date(),
        max_value=(bt_end - pd.Timedelta(days=30)).date(),
        key="wf_split",
    )
    split_ts = pd.Timestamp(split_date)

    nav_in = nav_df[nav_df.index <= split_ts]
    nav_out = nav_df[nav_df.index > split_ts]

    if nav_in.empty or nav_out.empty:
        st.warning("Split date leaves one window empty — move the split date further inside the backtest range.")
        return

    # ── header metrics ──
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("In-sample start", str(nav_in.index[0].date()))
    c2.metric("In-sample end", str(nav_in.index[-1].date()))
    c3.metric("Out-of-sample start", str(nav_out.index[0].date()))
    c4.metric("Out-of-sample end", str(nav_out.index[-1].date()))

    # ── strategy columns only (exclude benchmarks for the comparison tables) ──
    strategy_cols = [c for c in nav_df.columns if any(v in c for v in ("Full", "Marginal", "Prop"))]
    bench_cols = [c for c in nav_df.columns if c not in strategy_cols]

    # ── compute stats on raw (un-normalised) slices ──
    stats_in = _compute_summary_stats(nav_in[strategy_cols]) if strategy_cols else pd.DataFrame()
    stats_out = _compute_summary_stats(nav_out[strategy_cols]) if strategy_cols else pd.DataFrame()

    st.divider()
    left, right = st.columns(2)

    def _fmt_stats(df: pd.DataFrame) -> pd.DataFrame:
        """Format stats DataFrame for display."""
        display = df.copy()
        for col in display.columns:
            if "%" in col or col in ("CAGR (%)", "Max DD (%)", "Volatility (%)"):
                display[col] = display[col].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
            elif col == "Sharpe":
                display[col] = display[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        return display

    with left:
        days_in = len(nav_in)
        st.markdown(f"**📅 In-sample** · {nav_in.index[0].date()} → {nav_in.index[-1].date()} · {days_in} days")
        if not stats_in.empty:
            st.dataframe(stats_in, use_container_width=True)
        else:
            st.info("No strategy data.")

    with right:
        days_out = len(nav_out)
        st.markdown(f"**🔮 Out-of-sample** · {nav_out.index[0].date()} → {nav_out.index[-1].date()} · {days_out} days")
        if not stats_out.empty:
            st.dataframe(stats_out, use_container_width=True)
        else:
            st.info("No strategy data.")

    # ── NAV chart with split marker ──
    st.divider()
    st.markdown("**Normalised NAV — both windows rebased to 100**")

    # Normalise each window independently to base 100
    nav_in_norm = (nav_in[strategy_cols] / nav_in[strategy_cols].iloc[0]) * 100
    nav_out_norm = (nav_out[strategy_cols] / nav_out[strategy_cols].iloc[0]) * 100

    nav_combined = pd.concat([nav_in_norm, nav_out_norm])
    fig = nav_chart_figure(nav_combined)
    fig.add_vline(
        x=split_ts.timestamp() * 1000,
        line_dash="dot",
        line_color="rgba(255,255,255,0.5)",
        annotation_text="Split",
        annotation_position="top",
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── benchmark comparison (informational, not split) ──
    if bench_cols:
        with st.expander("Benchmark stats (full period, not split)"):
            bench_stats = _compute_summary_stats(nav_df[bench_cols])
            st.dataframe(bench_stats, use_container_width=True)


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────


def render_backtest_tabs(bt_params: dict) -> None:
    """Render the hero heading and all four backtest tabs. Call from any entry point."""
    st.markdown('<p class="hero">⏱ Momentum Backtest</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-hero">Classic vs Displacement Band Rule · '
        "Full vs Marginal Rebalance · Benchmarked vs Nifty 50 & Nifty 500</p>",
        unsafe_allow_html=True,
    )
    tab_bt, tab_debug, tab_wf, tab_guide = st.tabs(["📊 Backtest", "🔍 Debug", "📐 Walk-Forward", "📖 User Guide"])
    with tab_bt:
        backtest_results(bt_params)
        _render_weights_csv_uploader()
    with tab_debug:
        _render_debug_tab(st.session_state.get("backtest_cached_result"))
    with tab_wf:
        _render_walkforward_tab(st.session_state.get("backtest_cached_result"), bt_params)
    with tab_guide:
        _render_user_guide()


def main():
    st.set_page_config(page_title="Momentum Backtest | Nifty 750", page_icon="⏱", layout="wide")
    st.markdown(
        """
<style>
.hero { text-align: center; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
.sub-hero { text-align: center; opacity: 0.6; margin-top: -8px; }
</style>
""",
        unsafe_allow_html=True,
    )
    user_token = _get_user_token()
    idx_options = _load_index_options()

    with st.sidebar:
        st.markdown("### ⏱ Backtest")
        bt_params = _sidebar_backtest(idx_options)

    active_job = registry.latest(user_token, "backtest")
    run_triggered = st.session_state.get("backtest_run_triggered", False)
    if run_triggered or (active_job and active_job.status in (JobStatus.RUNNING, JobStatus.QUEUED)):
        st_autorefresh(interval=1500, key="job_autorefresh")

    render_backtest_tabs(bt_params)


if __name__ == "__main__":
    main()
