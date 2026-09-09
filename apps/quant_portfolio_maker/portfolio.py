"""Streamlit workspace for multi-stock quant portfolio construction and replay."""

from __future__ import annotations

import importlib
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import quant_portfolio_metrics as portfolio_metrics
import workers as worker_functions
from backtest_engine import _compute_summary_stats, rolling_returns
from charts import nav_chart_figure, rolling_returns_figure
from jobs import JobStatus, registry
from ui_helpers import _get_user_token, _poll_job

if getattr(portfolio_metrics, "QUANT_PORTFOLIO_METRICS_VERSION", 0) < 2:
    portfolio_metrics = importlib.reload(portfolio_metrics)
if getattr(worker_functions, "SCREENER_WORKER_VERSION", 0) < 7:
    worker_functions = importlib.reload(worker_functions)

slice_and_rebase_nav = portfolio_metrics.slice_and_rebase_nav
quant_portfolio_worker = worker_functions.quant_portfolio_worker
PORTFOLIO_UI_VERSION = 7

_ROLLING_WINDOWS = {"1 year": 252, "3 years": 756, "5 years": 1_260}


def render_portfolio_sidebar(index_options: list[str]) -> dict:
    """Render Portfolio Lab controls and return a serializable worker payload."""
    st.markdown("### 📊 Portfolio Lab")
    strategy_label = st.selectbox("Quant method", ["HA + EMA Trend", "Ichimoku"], key="qpm_pf_strategy")
    settings: dict = {}
    if strategy_label == "Ichimoku":
        settings["preset"] = st.selectbox("Ichimoku preset", ["Balanced", "Aggressive", "Conservative"])
        settings["timeframe"] = st.selectbox("Signal timeframe", ["Weekly", "Daily"])
        ranking_method = "strategy_score"
        ranking_lookback = 55
    else:
        ranking_label = st.selectbox(
            "Entry ranking",
            ["Strategy composite", "Relative strength vs Nifty 100"],
            key="qpm_pf_ranking",
        )
        ranking_method = "relative_strength" if ranking_label == "Relative strength vs Nifty 100" else "strategy_score"
        if ranking_method == "relative_strength":
            ranking_lookback = int(
                st.number_input(
                    "Relative-strength lookback (sessions)",
                    min_value=5,
                    max_value=252,
                    value=55,
                    step=5,
                    key="qpm_pf_ranking_lookback",
                )
            )
            st.caption("Score = stock return factor ÷ Nifty 100 return factor, measured through the signal date.")
        else:
            ranking_lookback = 55
    selected = st.multiselect("Historical index universe", index_options, default=index_options, key="qpm_pf_indices")
    st.caption("Membership is evaluated from the latest composition snapshot available on each signal date.")
    max_holdings = st.number_input("Fixed slots", min_value=1, max_value=50, value=10, step=1)
    initial_capital = st.number_input("Initial capital (₹)", min_value=10_000, value=1_000_000, step=100_000)
    today = date.today()
    dates = st.date_input("Backtest range", value=(today - timedelta(days=365 * 2), today), max_value=today)
    start_date, end_date = (
        dates if isinstance(dates, tuple) and len(dates) == 2 else (today - timedelta(days=365 * 2), today)
    )
    with st.expander("Costs, taxes & data gates"):
        transaction_cost = st.number_input("Transaction cost (%)", 0.0, 5.0, 0.10, 0.01)
        brokerage = st.number_input("Brokerage per order (₹)", 0.0, 10_000.0, 0.0, 10.0)
        stcg = st.number_input("STCG rate (%)", 0.0, 100.0, 0.0, 1.0)
        ltcg = st.number_input("LTCG rate (%)", 0.0, 100.0, 0.0, 1.0)
        min_history = st.number_input("Minimum history (sessions)", 50, 2_000, 260, 10)
        if strategy_label == "HA + EMA Trend":
            min_volume = st.number_input("Minimum 20-session median volume", 0, 100_000_000, 100_000, 10_000)
        else:
            min_volume = 0
            st.caption("Ichimoku does not use a volume or liquidity gate.")
        use_compositions = st.toggle("Historical constituent filter", value=True)
    if st.button("Run Quant Portfolio", type="primary", width="stretch"):
        st.session_state["quant_portfolio_run_triggered"] = True
    return {
        "strategy": "ha_ema" if strategy_label == "HA + EMA Trend" else "ichimoku",
        "strategy_settings": settings,
        "universe": selected,
        "max_holdings": int(max_holdings),
        "initial_capital": float(initial_capital),
        "start_date": str(start_date),
        "end_date": str(end_date),
        "transaction_cost_pct": float(transaction_cost),
        "brokerage_per_order": float(brokerage),
        "stcg_rate": float(stcg),
        "ltcg_rate": float(ltcg),
        "min_history_days": int(min_history),
        "minimum_median_volume": float(min_volume),
        "ranking_method": ranking_method,
        "ranking_lookback_sessions": ranking_lookback,
        "use_compositions": use_compositions,
    }


def _download(label: str, frame: pd.DataFrame, filename: str) -> None:
    st.download_button(label, frame.to_csv(index=True).encode("utf-8"), filename, "text/csv")


def _metric_grid(metrics: dict) -> None:
    keys = [
        "Trading Days",
        "Signals",
        "Executed Entries",
        "Signal Conversion (%)",
        "Market Exposure (%)",
        "Average Holdings",
    ]
    for column, key in zip(st.columns(6), keys):
        value = metrics.get(key, 0)
        column.metric(key, f"{value:,.1f}" if isinstance(value, float) else f"{value:,}")


def _render_debugger(result: dict) -> None:
    decisions = result["decision_log"]
    st.markdown("#### Decision debugger")
    if decisions.empty:
        st.info("This run produced no entry or exit events.")
        return
    dates = sorted(pd.to_datetime(decisions["Decision_Date"]).dt.date.unique(), reverse=True)
    selected_date = st.selectbox("Decision date", dates)
    subset = decisions[pd.to_datetime(decisions["Decision_Date"]).dt.date.eq(selected_date)]
    symbols = sorted(subset["Symbol"].unique())
    symbol = st.selectbox("Symbol", symbols)
    row = subset[subset["Symbol"].eq(symbol)].iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Event", row["Event"])
    c2.metric("Outcome", row["Outcome"])
    c3.metric("Rank score", f"{row['Score']:.3f}")
    c4.metric("Execution", str(row["Execution_Date"])[:10])
    st.write(row["Reason"])
    diagnostics = row["Diagnostics"] if isinstance(row["Diagnostics"], dict) else {}
    st.json({key: (None if pd.isna(value) else value) for key, value in diagnostics.items()})
    st.dataframe(decisions.drop(columns=["Diagnostics"]), width="stretch", hide_index=True)
    _download("Download full decision log", decisions, "quant_decision_log.csv")


def render_portfolio_lab(params: dict) -> None:
    st.markdown('<p class="hero">📊 Quant Portfolio Lab</p>', unsafe_allow_html=True)
    st.caption("Fixed-slot, long-only portfolios · completed candle → next observed Open · exits before ranked entries")
    user_token = _get_user_token()
    active = registry.latest(user_token, "quant_portfolio")
    run_requested = st.session_state.get("quant_portfolio_run_triggered", False)
    if run_requested or (active and active.status in (JobStatus.RUNNING, JobStatus.QUEUED)):
        st_autorefresh(interval=1500, key="qpm_portfolio_autorefresh")
    if _poll_job("quant_portfolio", quant_portfolio_worker, params):
        return
    result = st.session_state.get("quant_portfolio_cached_result")
    if result is None:
        st.info("Choose a method and universe in the sidebar, then click **Run Quant Portfolio**.")
        return

    data_start = result.get("ohlcv_start_date")
    coverage = f"{data_start or '—'} → {result.get('ohlcv_date', '—')}"
    st.caption(f"Candle coverage **{coverage}** · {result.get('ohlcv_source', '—')}")
    definition = result["strategy"]["definition"]
    warmup = int(definition.get("warmup_periods", 0))
    if warmup:
        unit = "weekly candles" if definition["timeframe"].lower() == "weekly" else "daily candles"
        st.info(
            f"{definition['label']} needs approximately {warmup} completed {unit} before its signals are ready. "
            "That warm-up uses data before the selected backtest start when such history is available."
        )
    st.caption("Full-backtest metrics")
    _metric_grid(result["metrics"])
    tabs = st.tabs(["Overview", "Performance", "Holdings & trades", "Quant diagnostics", "Debugger", "Methodology"])
    with tabs[0]:
        use_log_scale = st.toggle("Logarithmic Y-axis", value=True, key="qpm_pf_log_nav")
        nav_figure = nav_chart_figure(result["nav"])
        nav_figure.update_yaxes(type="log" if use_log_scale else "linear")
        st.plotly_chart(nav_figure, width="stretch")
        st.dataframe(result["stats"].style.format(precision=2), width="stretch")
        _download("Download NAV", result["nav"], "quant_portfolio_nav.csv")
    with tabs[1]:
        nav = result["nav"].dropna(how="all")
        slider_min, slider_max = nav.index.min().date(), nav.index.max().date()
        slider_key = "qpm_pf_analysis_period"
        saved_period = st.session_state.get(slider_key)
        if saved_period and (saved_period[0] < slider_min or saved_period[1] > slider_max):
            st.session_state[slider_key] = (slider_min, slider_max)
        start, end = st.slider(
            "Analysis period",
            min_value=slider_min,
            max_value=slider_max,
            value=(slider_min, slider_max),
            key=slider_key,
        )
        period, rebased = slice_and_rebase_nav(nav, start, end)
        if period.empty:
            st.warning("No observed trading sessions fall within the selected analysis period.")
        else:
            actual_start, actual_end = period.index.min(), period.index.max()
            st.caption(
                f"Observed sessions: **{actual_start:%d %b %Y} → {actual_end:%d %b %Y}** · "
                "each series independently rebased to 100 at its first valid observation"
            )
            use_log_scale = st.toggle(
                "Logarithmic Y-axis",
                value=True,
                key="qpm_pf_log_performance",
            )
            performance_figure = nav_chart_figure(rebased)
            performance_figure.update_layout(title="Selected-period performance · base 100")
            performance_figure.update_yaxes(type="log" if use_log_scale else "linear")
            st.plotly_chart(performance_figure, width="stretch")

            st.markdown("#### Selected-period statistics")
            st.dataframe(_compute_summary_stats(rebased).style.format(precision=2), width="stretch")

            with st.expander("Rolling CAGR", expanded=False):
                rolling_label = st.selectbox(
                    "Rolling window",
                    list(_ROLLING_WINDOWS),
                    index=1,
                    key="qpm_pf_rolling_window",
                )
                rolling_days = _ROLLING_WINDOWS[rolling_label]
                if rolling_days >= len(period):
                    st.info(
                        f"The selected period has {len(period):,} sessions; "
                        f"a {rolling_label} rolling view needs more than {rolling_days:,}."
                    )
                else:
                    st.plotly_chart(
                        rolling_returns_figure(rolling_returns(period, rolling_days)),
                        width="stretch",
                    )
    with tabs[2]:
        latest = result["holdings_log"][-1] if result["holdings_log"] else {"Holdings": [], "Weights": {}}
        st.write("Open holdings", latest["Holdings"] or "None")
        if latest["Weights"]:
            st.bar_chart(pd.Series(latest["Weights"], name="Weight"))
        st.dataframe(result["trades"], width="stretch", hide_index=True)
        _download("Download trades", result["trades"], "quant_portfolio_trades.csv")
    with tabs[3]:
        st.dataframe(pd.Series(result["metrics"], name="Value").to_frame(), width="stretch")
        if not result["exit_reason_breakdown"].empty:
            st.markdown("#### Exit reasons")
            st.dataframe(result["exit_reason_breakdown"], width="stretch")
        if not result["data_failures"].empty:
            st.warning(f"Signal calculation failed for {len(result['data_failures'])} symbols.")
            st.dataframe(result["data_failures"], width="stretch", hide_index=True)
        if not result["corporate_actions"].empty:
            st.markdown("#### Corporate actions applied")
            st.dataframe(result["corporate_actions"], width="stretch", hide_index=True)
    with tabs[4]:
        _render_debugger(result)
    with tabs[5]:
        definition = result["strategy"]["definition"]
        st.markdown(f"#### {definition['label']} ({definition['timeframe']})")
        st.write(definition["description"])
        ranking = result.get("ranking", {})
        if ranking:
            ranking_text = ranking.get("label", "Strategy composite")
            if ranking.get("benchmark"):
                ranking_text += (
                    f" · {ranking.get('lookback_sessions', 55)} sessions · benchmark: {ranking['benchmark']}"
                )
            st.markdown(f"**Entry ranking:** {ranking_text}")
        st.json(result["strategy"]["config"])
        st.markdown(
            "Entries compete only on their execution session and are sorted by strategy score, then ticker. "
            "Each successful entry receives one fixed slot (1 ÷ configured slots) of current portfolio value. "
            "Unfilled slots remain cash; existing positions drift and are not rebalanced. Signals never execute "
            "before the next observed Open. Whole shares, costs, brokerage, liquidity/history gates, historical "
            "membership, and taxes are included in the replay."
        )
