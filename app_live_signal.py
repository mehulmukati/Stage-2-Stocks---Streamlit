#!/usr/bin/env python3
"""
Live Signal tab — weekly trade instructions from a warm-up backtest snapshot.

Runs run_backtest over a short warm-up window (default 52 weeks) ending on the
chosen signal date, then snapshots the last rebalance event for trade execution.
"""

import hashlib
import json
import warnings
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from data import check_data_freshness

warnings.filterwarnings("ignore", category=FutureWarning)

_ALL_5_INDICES = [
    "Nifty 50",
    "Nifty Next 50",
    "Nifty Midcap 150",
    "Nifty Smallcap 250",
    "Nifty Microcap 250",
]

_SORT_OPTIONS = [
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


# ── helpers ──────────────────────────────────────────────────────────────────


def _next_business_day(d: date) -> date:
    """Return the next calendar day that is a weekday (Mon–Fri) after d."""
    d = d + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _previous_business_day(d: date) -> date:
    """Return the prior weekday before *d* (Mon–Fri calendar only)."""
    d = d - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _simulation_start_date(params: dict) -> date:
    """Anchor live-signal history to the portfolio's initial execution date.

    The strategy is ranked at the close before a portfolio start date and is
    executed on that start date.  Keeping this anchor fixed means subsequent
    weekly signals replay the same history and therefore the same prior
    holdings; users do not have to increase the lookback every week.
    """
    portfolio_start = params.get("portfolio_start")
    anchor = _previous_business_day(portfolio_start) if portfolio_start else params["signal_date"]
    return anchor - timedelta(weeks=params["warmup"])


def _strategy_fingerprint(params: dict, ohlcv_date: object, ohlcv_source: object) -> str:
    """Stable identity for one reproducible live-portfolio replay.

    Signal date and portfolio value are deliberately excluded: they do not
    define the strategy path.  Every selection, risk and quality setting that
    can alter holdings is included, as is the data version reported by the
    loader.
    """
    fields = (
        "portfolio_start",
        "warmup",
        "band",
        "variant",
        "m",
        "n",
        "sort_method",
        "freq",
        "s2_drop",
        "s2_threshold",
        "s2_entry",
        "s2_entry_threshold",
        "max_pos",
        "min_history",
        "min_annual_return",
        "pct_from_52w_high",
        "max_circuits",
        "close_above_100dma",
        "close_above_200dma",
        "pos_days_3m_min",
        "pos_days_6m_min",
        "pos_days_12m_min",
    )
    payload = {field: str(params.get(field)) for field in fields}
    payload["indices"] = sorted(params.get("indices", []))
    if params.get("freq") in {"weekly", "biweekly"}:
        payload["rebalance_weekday"] = params["signal_date"].strftime("%A")
    payload["ohlcv_date"] = str(ohlcv_date)
    payload["ohlcv_source"] = str(ohlcv_source)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:12]


def _parse_ticker_reason(s: str) -> tuple[str, str]:
    """Split 'TICKER (reason)' into (ticker, reason)."""
    s = s.strip()
    if "(" in s and s.endswith(")"):
        idx = s.rfind("(")
        return s[:idx].strip(), s[idx + 1 : -1]
    return s, ""


def _to_date(val) -> date:
    """Normalise pd.Timestamp or date to date."""
    return val.date() if hasattr(val, "date") else val


def _classify_weight_changes(
    prev_weights: dict[str, float],
    new_weights: dict[str, float],
    tolerance: float = 0.001,
) -> list[dict]:
    """Classify the complete portfolio from signed target-weight changes.

    Weights are expressed in percentage points.  Changes within *tolerance*
    are non-actionable and therefore HOLDs; positive and negative changes are
    BUYs and SELLs respectively.  Taking the union also retains complete exits
    (new weight zero) and new entries (previous weight zero).
    """
    changes = []
    for ticker in sorted(set(prev_weights) | set(new_weights)):
        previous = float(prev_weights.get(ticker, 0.0))
        new = float(new_weights.get(ticker, 0.0))
        change = new - previous
        action = "BUY" if change > tolerance else "SELL" if change < -tolerance else "HOLD"
        changes.append(
            {
                "Ticker": ticker,
                "Previous weight (%)": previous,
                "New weight (%)": new,
                "Weight change (%)": change,
                "Action": action,
            }
        )
    return changes


def _symbols_needed_for_replay(
    indices: list[str],
    constituents: dict[str, list[str]],
    compositions: pd.DataFrame,
    corporate_actions: list[dict],
) -> set[str]:
    """Return current members, historical members, and event successors needed for replay."""

    def canonical(value) -> str:
        return "".join(ch for ch in str(value).upper() if ch.isalnum())

    index_keys = {canonical(index) for index in indices}
    symbols = {
        symbol for index, members in constituents.items() if canonical(index) in index_keys for symbol in members
    }
    if not compositions.empty:
        historical = compositions[compositions["INDEX_NAME"].map(canonical).isin(index_keys)]
        symbols.update(historical["SYMBOL"].dropna().astype(str))
    for action in corporate_actions:
        symbols.add(action["old_symbol"])
        symbols.add(action["successor_symbol"])
    return symbols


# ── sidebar ──────────────────────────────────────────────────────────────────


def _sidebar_live_signal(idx_options: list[str]) -> dict:
    st.markdown("### 📡 Live Signal")

    signal_date = st.date_input(
        "Signal date",
        value=date.today(),
        max_value=date.today(),
        key="ls_signal_date",
        help="Closing prices from this date are used for ranking. "
        "Trades are assumed to execute on the next working day after this date.",
    )

    portfolio_value = st.number_input(
        "Portfolio value (₹)",
        min_value=10_000,
        max_value=100_000_000,
        value=1_000_000,
        step=10_000,
        format="%d",
        key="ls_portfolio_value",
        help="Total invested capital. Used to calculate ₹ values and share quantities.",
    )

    portfolio_start = st.date_input(
        "Portfolio start date",
        value=date.today() + timedelta(days=1),
        key="ls_portfolio_start",
        help="When you started (or plan to start) your portfolio. This anchors the simulation "
        "history, so each later rebalance replays the same portfolio path.",
    )

    warmup = st.slider(
        "Simulation lookback (weeks)",
        26,
        156,
        52,
        step=1,
        key="ls_warmup",
        help="How far back from the portfolio's initial rebalance to run the backtest engine "
        "to compute Marginal weight drift. Keep this fixed for subsequent signals.",
    )

    st.divider()

    st.markdown("**Strategy**")

    band = st.selectbox(
        "Band rule",
        ["classic", "displacement"],
        format_func=str.capitalize,
        key="ls_band",
    )
    variant = st.selectbox(
        "Variant",
        ["Marginal Rebalance", "Prop Rebalance", "Full Rebalance"],
        key="ls_variant",
    )

    col_m, col_n = st.columns(2)
    m = col_m.number_input("M (entry)", min_value=5, max_value=50, value=15, step=1, key="ls_m")
    n = col_n.number_input("N (exit)", min_value=6, max_value=200, value=30, step=1, key="ls_n")

    sort_method = st.selectbox("Rank by Sharpe", _SORT_OPTIONS, index=0, key="ls_sort_method")
    freq = st.selectbox(
        "Rebalance frequency",
        ["weekly", "biweekly", "monthly", "quarterly", "half-yearly"],
        key="ls_freq",
    )

    if freq == "weekly":
        st.divider()
        st.markdown("**Stage 2 signals** (weekly)")
        s2_entry = st.toggle(
            "Enter on Stage 2 score jump",
            value=False,
            key="ls_s2_entry",
            help="Allow a stock to enter if its Weinstein Stage 2 score rises by the threshold or more since last week "
            "— even if it isn't in the top-M momentum rank.",
        )
        s2_entry_threshold = st.number_input(
            "Score jump threshold",
            min_value=1,
            max_value=4,
            value=2,
            step=1,
            disabled=not s2_entry,
            key="ls_s2_entry_threshold",
            help="Stage 2 points that must rise in one week to trigger entry (e.g. 2 means score 4→6).",
        )
        s2_drop = st.toggle("Stage 2 drop exit", value=False, key="ls_s2_drop")
        s2_threshold = st.number_input(
            "Drop threshold",
            min_value=1,
            max_value=4,
            value=2,
            step=1,
            disabled=not s2_drop,
            key="ls_s2_threshold",
            help="Stage 2 points that must fall in one week to trigger exit (e.g. 2 means score 6→4).",
        )
    else:
        s2_entry = False
        s2_entry_threshold = st.session_state.get("ls_s2_entry_threshold", 2)
        s2_drop = False
        s2_threshold = st.session_state.get("ls_s2_threshold", 2)

    st.divider()
    st.markdown("**Position cap & universe**")
    max_pos = st.slider(
        "Max position (%)",
        0,
        50,
        15,
        step=1,
        key="ls_max_pos",
        help="0 = no cap. Recommended 15% for Marginal variant.",
    )

    indices = st.multiselect(
        "Index universe",
        options=idx_options or _ALL_5_INDICES,
        default=idx_options or _ALL_5_INDICES,
        key="ls_indices",
    )
    st.divider()

    with st.expander("Quality Filters", expanded=False):
        st.caption(
            "Stocks that fail these filters are excluded from portfolio selection at each rebalance. "
            "0 / 100 / 999 = no filter (default). Match these to the Momentum Screener for consistent results."
        )
        ls_min_annual_return = st.number_input(
            "Min Annual Return (%)",
            min_value=0.0,
            max_value=1000.0,
            step=0.1,
            value=7.0,
            key="ls_min_annual_return",
            help="Exclude stocks whose 1-year price change is below this threshold.",
        )
        ls_pct_from_52w_high = st.number_input(
            "Within % of 52w High",
            min_value=0,
            max_value=100,
            step=1,
            value=25,
            key="ls_pct_from_52w_high",
            help="Exclude stocks more than this % below their 52-week high. 100 = no filter.",
        )
        ls_max_circuits = st.number_input(
            "Max Circuits (1yr)",
            min_value=0,
            max_value=999,
            step=1,
            value=18,
            key="ls_max_circuits",
            help="Exclude stocks with more than this many circuit-limit closes in the past year. 999 = no filter.",
        )
        ls_close_above_100dma = st.checkbox("Close > 100 DMA", value=False, key="ls_close_above_100dma")
        ls_close_above_200dma = st.checkbox("Close > 200 DMA", value=True, key="ls_close_above_200dma")
        _pd_cols = st.columns(3)
        ls_pos_days_3m = _pd_cols[0].number_input(
            "Pos Days 3M (%)",
            min_value=0,
            max_value=100,
            step=1,
            value=45,
            key="ls_pos_days_3m",
            help="Min % of up-close days over last 3 months.",
        )
        ls_pos_days_6m = _pd_cols[1].number_input(
            "Pos Days 6M (%)",
            min_value=0,
            max_value=100,
            step=1,
            value=45,
            key="ls_pos_days_6m",
            help="Min % of up-close days over last 6 months.",
        )
        ls_pos_days_12m = _pd_cols[2].number_input(
            "Pos Days 12M (%)",
            min_value=0,
            max_value=100,
            step=1,
            value=45,
            key="ls_pos_days_12m",
            help="Min % of up-close days over last 12 months.",
        )

    min_history = st.number_input(
        "Min history (trading days)",
        min_value=63,
        max_value=1260,
        value=252,
        step=21,
        key="ls_min_history",
        help="Minimum trading days of data a stock must have before it can be ranked. 252 ≈ 1 year.",
    )

    freshness_issues = check_data_freshness()
    live_data_blocked = False
    for _lvl, _msg in freshness_issues:
        (st.error if _lvl == "error" else st.warning)(_msg)
        if _lvl == "error" or "constituents.json" in _msg or "compositions.parquet" in _msg:
            live_data_blocked = True
    if live_data_blocked:
        st.error("LiveSignal is disabled until constituent and composition data are current.")
        st.session_state["ls_run_triggered"] = False
        st.session_state["ls_result"] = None
    st.divider()

    if st.button(
        "📡 Generate Signal",
        type="primary",
        width="stretch",
        key="ls_run_btn",
        disabled=live_data_blocked,
    ):
        st.session_state["ls_run_triggered"] = True
        st.session_state["ls_result"] = None  # invalidate cached result

    return {
        "signal_date": signal_date,
        "band": band,
        "variant": variant,
        "m": int(m),
        "n": int(n),
        "sort_method": sort_method,
        "freq": freq,
        "s2_drop": s2_drop,
        "s2_threshold": s2_threshold,
        "s2_entry": s2_entry,
        "s2_entry_threshold": int(s2_entry_threshold),
        "max_pos": max_pos,
        "indices": list(indices),
        "portfolio_start": portfolio_start,
        "warmup": int(warmup),
        "portfolio_value": int(portfolio_value),
        "min_history": int(min_history),
        "min_annual_return": float(ls_min_annual_return),
        "pct_from_52w_high": float(ls_pct_from_52w_high),
        "max_circuits": int(ls_max_circuits),
        "close_above_100dma": bool(ls_close_above_100dma),
        "close_above_200dma": bool(ls_close_above_200dma),
        "pos_days_3m_min": float(ls_pos_days_3m),
        "pos_days_6m_min": float(ls_pos_days_6m),
        "pos_days_12m_min": float(ls_pos_days_12m),
    }


# ── engine call ───────────────────────────────────────────────────────────────


def _run_signal(params: dict) -> dict:
    from backtest_engine import BacktestConfig, latest_tradable_date, run_backtest, trading_session_age
    from corporate_actions import load_corporate_actions
    from data_backtest import (
        _load_constituents,
        load_benchmark_series,
        load_compositions,
        load_ohlcv_for_backtest,
        sync_benchmark_data,
    )

    sync_benchmark_data()
    symbol_data_all, ohlcv_date, src = load_ohlcv_for_backtest(emit=lambda _l, _m: None)
    if not symbol_data_all:
        return {"error": "OHLCV data missing. Run: python scripts/refresh_backtest_parquet.py"}

    indices = params["indices"]
    compositions_df = load_compositions()
    if indices:
        constituents = _load_constituents()
        allowed = _symbols_needed_for_replay(indices, constituents, compositions_df, load_corporate_actions())
        symbol_data = {s: df for s, df in symbol_data_all.items() if s in allowed}
    else:
        symbol_data = symbol_data_all

    benchmarks = load_benchmark_series()

    signal_date = params["signal_date"]
    start_date = _simulation_start_date(params)

    cfg = BacktestConfig(
        m=params["m"],
        n=params["n"],
        rebalance_freq=params.get("freq", "weekly"),
        sort_method=params["sort_method"],
        start_date=str(start_date),
        end_date=str(signal_date),
        compositions_df=compositions_df,
        index_names=indices,
        transaction_cost_pct=0.001,
        min_history_days=params.get("min_history", 252),
        apply_volume_filter=True,
        brokerage_per_sale=0.0,
        initial_capital=1_000_000.0,
        ltcg_rate=0.125,
        stcg_rate=0.20,
        band_rule=params["band"],
        # Keep live rebalances on the selected weekday.  A later signal one
        # week on must replay the same schedule, not insert Friday simply
        # because a previous run ended on Tuesday.
        rebalance_anchor_date=str(signal_date),
        portfolio_start_date=str(params["portfolio_start"]) if params.get("portfolio_start") else None,
        stage2_drop_exit=params["s2_drop"],
        stage2_drop_threshold=params["s2_threshold"],
        stage2_entry_filter=params.get("s2_entry", False),
        stage2_entry_threshold=params.get("s2_entry_threshold", 2),
        max_position_pct=float(params["max_pos"]) if params["max_pos"] > 0 else None,
        min_annual_return=params.get("min_annual_return", 0.0),
        pct_from_52w_high=params.get("pct_from_52w_high", 100.0),
        max_circuits=params.get("max_circuits", 999),
        close_above_100dma=params.get("close_above_100dma", False),
        close_above_200dma=params.get("close_above_200dma", False),
        pos_days_3m_min=params.get("pos_days_3m_min", 0.0),
        pos_days_6m_min=params.get("pos_days_6m_min", 0.0),
        pos_days_12m_min=params.get("pos_days_12m_min", 0.0),
    )
    result = run_backtest(symbol_data, benchmarks, cfg)

    if "error" in result:
        return {"error": result["error"]}

    # Collect close prices for all tickers that appear in the last two rebalance events
    holdings_log = result["holdings_log"]
    tickers_needed: set[str] = set()
    if holdings_log:
        tickers_needed.update(holdings_log[-1].get("holdings", []))
        tickers_needed.update(t for s in holdings_log[-1].get("exits", []) for t, _ in [_parse_ticker_reason(s)])
        for action in holdings_log[-1].get("corporate_actions", []):
            tickers_needed.add(action["old_symbol"])
            tickers_needed.add(action["successor_symbol"])
    if len(holdings_log) >= 2:
        tickers_needed.update(holdings_log[-2].get("holdings", []))

    target_date = pd.Timestamp(params["signal_date"])
    close_prices: dict[str, float] = {}
    latest_price_dates: dict[str, str] = {}
    tradability_status: dict[str, str] = {}
    trading_calendar = pd.DatetimeIndex(
        sorted({date for frame in symbol_data.values() for date in frame.index if date <= target_date})
    )
    for ticker in tickers_needed:
        if ticker not in symbol_data:
            tradability_status[ticker] = "NO DATA"
            continue
        frame = symbol_data[ticker]
        closes = frame["Close"]
        avail = closes[closes.index <= target_date].dropna()
        if not avail.empty:
            close_prices[ticker] = float(avail.iloc[-1])
        last_tradable = latest_tradable_date(frame, target_date)
        if last_tradable is None:
            tradability_status[ticker] = "NO TRADABLE PRICE"
            continue
        latest_price_dates[ticker] = str(last_tradable.date())
        age = trading_session_age(last_tradable, target_date, trading_calendar)
        tradability_status[ticker] = "TRADABLE" if age <= 3 else f"STALE ({age} sessions)"

    current_event = holdings_log[-1] if holdings_log else {}
    prior_holdings = set(holdings_log[-2].get("holdings", [])) if len(holdings_log) >= 2 else set()
    handled_old_symbols = {action["old_symbol"] for action in current_event.get("corporate_actions", [])}
    blocking_stale_incumbents = sorted(
        ticker
        for ticker in prior_holdings
        if tradability_status.get(ticker, "NO DATA") != "TRADABLE" and ticker not in handled_old_symbols
    )

    strategy_fingerprint = _strategy_fingerprint(params, ohlcv_date, src)
    return {
        "holdings_log": holdings_log,
        "ohlcv_date": ohlcv_date,
        "ohlcv_source": src,
        "portfolio_reset_date": result.get("portfolio_reset_date"),
        "strategy_fingerprint": strategy_fingerprint,
        "close_prices": close_prices,
        "latest_price_dates": latest_price_dates,
        "tradability_status": tradability_status,
        "blocking_stale_incumbents": blocking_stale_incumbents,
    }


# ── results renderer ──────────────────────────────────────────────────────────


def live_signal_results(params: dict) -> None:
    if not st.session_state.get("ls_run_triggered", False):
        st.info("Configure your strategy in the sidebar and click **Generate Signal**.")
        return

    # Run only when button was clicked (ls_result cleared by button handler)
    result = st.session_state.get("ls_result")
    if result is None:
        with st.spinner("Loading data and computing signal… (~15 seconds)"):
            result = _run_signal(params)
        st.session_state["ls_result"] = result

    if "error" in result:
        st.error(f"Signal failed: {result['error']}")
        return

    holdings_log = result["holdings_log"]
    ohlcv_date = result["ohlcv_date"]

    if not holdings_log:
        st.error(
            "No rebalance events found in the warm-up window. "
            "Try increasing the warm-up period or choosing a later signal date."
        )
        return

    current = holdings_log[-1]
    previous = holdings_log[-2] if len(holdings_log) >= 2 else None

    # Select weight snapshot for the chosen variant
    _v = params["variant"]
    weight_key = "prop_weights" if "Prop" in _v else "marg_weights" if "Marginal" in _v else "full_weights"
    weights: dict[str, float] = dict(current[weight_key])
    prev_weights: dict[str, float] = dict(previous[weight_key]) if previous else {}

    # A merger changes the security's identity without being a market trade.
    # Move the prior snapshot to the successor so the four tables show the
    # executable successor sale instead of an impossible sale of the old scrip.
    corporate_action_events = current.get("corporate_actions", [])
    for action in corporate_action_events:
        old_symbol = action["old_symbol"]
        successor_symbol = action["successor_symbol"]
        if old_symbol in prev_weights:
            prev_weights[successor_symbol] = prev_weights.get(successor_symbol, 0.0) + prev_weights.pop(old_symbol)

    # Parse entries / exits (stored as "TICKER (reason)" strings)
    entries: dict[str, str] = {t: r for t, r in (_parse_ticker_reason(s) for s in current.get("entries", []))}
    exits: dict[str, str] = {t: r for t, r in (_parse_ticker_reason(s) for s in current.get("exits", []))}
    holdings: set[str] = set(current.get("holdings", []))

    rebalance_date = _to_date(current["date"])
    exec_date = _next_business_day(rebalance_date)
    reset_date = result.get("portfolio_reset_date")
    reset_date = _to_date(reset_date) if reset_date is not None else None
    # A fresh portfolio is an engine event, not a display override.  This is
    # essential: the all-BUY view must be the same state used by the next run.
    fresh_portfolio = reset_date == rebalance_date

    # ── param validation warning ──────────────────────────────────────────────
    if params["n"] <= params["m"]:
        st.warning(f"N ({params['n']}) must be greater than M ({params['m']}). Results may be unreliable.")

    if params["warmup"] < 26 and params["variant"] != "Full Rebalance":
        st.warning(
            f"Portfolio start date is less than 26 weeks before signal date ({params['warmup']} weeks). "
            "Marginal weights may not reflect realistic drift — try an earlier start date."
        )

    # ── header ───────────────────────────────────────────────────────────────
    band_lbl = params["band"].capitalize()
    var_lbl = "Prop" if "Prop" in params["variant"] else "Marginal" if "Marginal" in params["variant"] else "Full"
    cap_lbl = f" · cap {params['max_pos']}%" if params["max_pos"] > 0 else ""
    s2_lbl = f" · S2 drop={params['s2_threshold']}" if params["s2_drop"] else ""
    st.markdown(
        f"**{band_lbl} · {var_lbl} · M={params['m']} · N={params['n']}" f" · {params['sort_method']}{s2_lbl}{cap_lbl}**"
    )
    sim_start = _simulation_start_date(params)
    portfolio_start = params.get("portfolio_start")
    port_lbl = portfolio_start.strftime("%b %d, %Y") if portfolio_start else "—"
    fresh_lbl = " 🆕 **Fresh portfolio — all positions are BUY**" if fresh_portfolio else ""
    st.caption(
        f"Portfolio start: **{port_lbl}**{fresh_lbl} "
        f"· Simulation from: **{sim_start.strftime('%b %d, %Y')}** ({params['warmup']} weeks back) "
        f"· Data as-of: **{ohlcv_date}** "
        f"· Replay ID: **{result.get('strategy_fingerprint', '—')}** "
        f"· {params['freq'].capitalize()} schedule: **{params['signal_date'].strftime('%A')}** "
        f"· Last rebalance: **{rebalance_date.strftime('%a %b %d, %Y')}** "
        f"· Execute on: **{exec_date.strftime('%a %b %d, %Y')}** at or after 09:15 IST"
    )
    st.divider()

    for action in corporate_action_events:
        st.info(
            f"**Corporate action applied:** {action['old_symbol']} → {action['successor_symbol']} "
            f"effective {action['effective_date']} at {action['share_ratio']:.4g} successor shares per old share. "
            "The identity conversion is not treated as a taxable market sale."
        )

    blocking_stale_incumbents: list[str] = result.get("blocking_stale_incumbents", [])
    if blocking_stale_incumbents:
        st.error(
            "**Trade list blocked:** stale incumbent price with no registered corporate action: "
            + ", ".join(blocking_stale_incumbents)
            + ". Update the corporate-action registry or restore current tradable data before execution."
        )

    # ── portfolio sizing helpers ──────────────────────────────────────────────
    portfolio_value: int = params.get("portfolio_value", 1_000_000)
    close_prices: dict[str, float] = result.get("close_prices", {})
    latest_price_dates: dict[str, str] = result.get("latest_price_dates", {})
    tradability_status: dict[str, str] = result.get("tradability_status", {})

    def _val(weight_pct: float) -> int:
        return round(portfolio_value * weight_pct / 100)

    def _allocate_qtys(
        buy_targets: dict[str, float],
        sell_targets: dict[str, float],
        prices: dict[str, float],
    ) -> dict[str, int]:
        """Joint integer allocation: minimise weight deviation s.t. sell ₹ ≈ buy ₹.

        Uses a two-phase greedy:
          Phase 1 — unconstrained optimal: round each stock to nearest integer share.
          Phase 2 — cash-balance correction: iteratively apply the cheapest single-share
                    adjustment (scored by weight-deviation cost per unit remainder) until
                    |sell_cash - buy_cash| ≤ half the cheapest stock price.
        """
        all_targets = {**buy_targets, **sell_targets}
        if not all_targets:
            return {}
        priced = {t: all_targets[t] for t in all_targets if prices.get(t, 0) > 0}
        if not priced:
            return {}

        exact = {t: priced[t] / prices[t] for t in priced}
        f = {t: int(exact[t]) for t in exact}  # floor quantities
        r = {t: exact[t] - f[t] for t in exact}  # remainders ∈ [0, 1)
        d = {t: 1 if r[t] >= 0.5 else 0 for t in exact}  # phase-1: round to nearest

        def _gap() -> float:
            s = sum((f[t] + d[t]) * prices[t] for t in sell_targets if t in f)
            b = sum((f[t] + d[t]) * prices[t] for t in buy_targets if t in f)
            return s - b  # positive → sell side heavy, negative → buy side heavy

        tol = min(prices[t] for t in priced) / 2

        for _ in range(200):
            g = _gap()
            if abs(g) <= tol:
                break
            candidates: list[tuple] = []
            for t in sell_targets:
                if t not in f:
                    continue
                if g < 0 and d[t] == 0:  # bump sell up → gap increases toward 0
                    candidates.append((1 - 2 * r[t], +prices[t], t, +1))
                if g > 0 and d[t] >= 1:  # un-bump sell → gap decreases toward 0
                    candidates.append((2 * r[t] - 1, -prices[t], t, -1))
            for t in buy_targets:
                if t not in f:
                    continue
                if g > 0 and d[t] == 0:  # bump buy up → gap decreases toward 0
                    candidates.append((1 - 2 * r[t], -prices[t], t, +1))
                if g < 0 and d[t] >= 1:  # un-bump buy → gap increases toward 0
                    candidates.append((2 * r[t] - 1, +prices[t], t, -1))
            if not candidates:
                break
            candidates.sort()
            score, delta_gap, best, delta_d = candidates[0]
            if abs(g + delta_gap) < abs(g):
                d[best] += delta_d
            else:
                break

        return {t: f[t] + d[t] for t in exact}

    # ── pre-cap weights for trimming callout ─────────────────────────────────
    pre_cap_key = (
        "pre_cap_prop_weights"
        if "Prop" in _v
        else "pre_cap_marg_weights" if "Marginal" in _v else "pre_cap_full_weights"
    )
    pre_cap_weights: dict[str, float] = current.get(pre_cap_key, {})

    # One comparison drives every table, the metrics and the CSV.  A reset
    # deliberately has no previous portfolio, so every opening position is BUY.
    comparison_prev_weights = {} if fresh_portfolio else prev_weights
    portfolio_changes = _classify_weight_changes(comparison_prev_weights, weights)
    buy_changes = [row for row in portfolio_changes if row["Action"] == "BUY"]
    sell_changes = [row for row in portfolio_changes if row["Action"] == "SELL"]
    hold_changes = [row for row in portfolio_changes if row["Action"] == "HOLD"]

    # ── metric cards ─────────────────────────────────────────────────────────
    turnover_key = (
        "prop_turnover_pct" if "Prop" in _v else "marg_turnover_pct" if "Marginal" in _v else "full_turnover_pct"
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Holdings", len(holdings))
    c2.metric("Buys", len(buy_changes), delta="fresh start" if fresh_portfolio else None)
    c3.metric(
        "Sells",
        len(sell_changes),
        delta=f"-{len(sell_changes)}" if sell_changes else None,
        delta_color="inverse",
    )
    c4.metric("Turnover", f"{current.get(turnover_key, 0):.1f}%")

    # ── position cap trimming callout ─────────────────────────────────────────
    if params["max_pos"] > 0 and pre_cap_weights:
        trimmed = {
            t: (pre_cap_weights.get(t, 0.0), weights.get(t, 0.0))
            for t in holdings
            if pre_cap_weights.get(t, 0.0) > weights.get(t, 0.0) + 0.05
        }
        if trimmed:
            freed_pct = sum(pre - post for pre, post in trimmed.values())
            n_uncapped = len(holdings) - len(trimmed)
            lines = [f"**Position cap {params['max_pos']}% trimmed {len(trimmed)} holding(s):**"]
            for t, (pre, post) in sorted(trimmed.items(), key=lambda x: -(x[1][0] - x[1][1])):
                lines.append(f"- {t}: {pre:.2f}% → {post:.2f}% (freed {pre - post:.2f}%)")
            lines.append(
                f"\nTotal freed: **{freed_pct:.2f}%** redistributed proportionally "
                f"to {n_uncapped} uncapped holding(s)."
            )
            st.warning("\n".join(lines))

    # ── joint quantity allocation (pre-compute before rendering any table) ────
    # Complete exits are sized independently so rounding can never leave an
    # intended exit open.  All other changes share the cash-balancing allocator.
    complete_exits = [row for row in sell_changes if row["New weight (%)"] <= 0.001]
    partial_sells = [row for row in sell_changes if row["New weight (%)"] > 0.001]
    exit_targets = {row["Ticker"]: abs(row["Weight change (%)"]) * portfolio_value / 100 for row in complete_exits}
    exit_qtys: dict[str, int] = _allocate_qtys({}, exit_targets, close_prices)
    buy_targets = {row["Ticker"]: row["Weight change (%)"] * portfolio_value / 100 for row in buy_changes}
    partial_sell_targets = {
        row["Ticker"]: abs(row["Weight change (%)"]) * portfolio_value / 100 for row in partial_sells
    }
    joint_qtys: dict[str, int] = _allocate_qtys(buy_targets, partial_sell_targets, close_prices)

    # ── BUY ──────────────────────────────────────────────────────────────────
    st.markdown("---")
    buy_rows = []
    for change in buy_changes:
        ticker = change["Ticker"]
        buy_rows.append(
            {
                **change,
                "Buy weight (%)": change["Weight change (%)"],
                "Buy value (₹)": _val(change["Weight change (%)"]),
                "Qty to buy": joint_qtys.get(ticker),
                "Buy type": "New position" if change["Previous weight (%)"] <= 0.001 else "Incremental buy",
                "Reason": entries.get(ticker, "") or ("Top-M" if ticker in entries else "Weight increase"),
            }
        )
    buy_rows.sort(key=lambda row: -row["Buy weight (%)"])
    buy_columns = [
        "Ticker",
        "Previous weight (%)",
        "New weight (%)",
        "Buy weight (%)",
        "Buy value (₹)",
        "Qty to buy",
        "Buy type",
        "Reason",
    ]
    st.markdown(f"#### Buy &nbsp;({len(buy_rows)})")
    st.dataframe(
        pd.DataFrame(buy_rows, columns=buy_columns),
        hide_index=True,
        width="stretch",
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Previous weight (%)": st.column_config.NumberColumn("Previous weight (%)", format="%.2f%%", width="small"),
            "New weight (%)": st.column_config.NumberColumn("New weight (%)", format="%.2f%%", width="small"),
            "Buy weight (%)": st.column_config.NumberColumn("Buy weight (%)", format="%.2f%%", width="small"),
            "Buy value (₹)": st.column_config.NumberColumn("Buy value (₹)", format="₹%d", width="small"),
            "Qty to buy": st.column_config.NumberColumn("Qty to buy", format="%d", width="small"),
            "Buy type": st.column_config.TextColumn("Buy type", width="small"),
            "Reason": st.column_config.TextColumn("Reason"),
        },
    )

    # ── SELL ─────────────────────────────────────────────────────────────────
    st.markdown("---")
    sell_rows = []
    complete_exit_tickers = {row["Ticker"] for row in complete_exits}
    for change in sell_changes:
        ticker = change["Ticker"]
        is_exit = ticker in complete_exit_tickers
        sell_weight = abs(change["Weight change (%)"])
        sell_rows.append(
            {
                **change,
                "Sell weight (%)": sell_weight,
                "Sell value (₹)": _val(sell_weight),
                "Qty to sell": exit_qtys.get(ticker) if is_exit else joint_qtys.get(ticker),
                "Sell type": "Complete exit" if is_exit else "Partial sell",
                "Reason": exits.get(ticker, "") or ("WRH" if ticker in exits else "Weight reduction"),
            }
        )
    sell_rows.sort(key=lambda row: -row["Sell weight (%)"])
    sell_columns = [
        "Ticker",
        "Previous weight (%)",
        "New weight (%)",
        "Sell weight (%)",
        "Sell value (₹)",
        "Qty to sell",
        "Sell type",
        "Reason",
    ]
    st.markdown(f"#### Sell &nbsp;({len(sell_rows)})")
    st.dataframe(
        pd.DataFrame(sell_rows, columns=sell_columns),
        hide_index=True,
        width="stretch",
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Previous weight (%)": st.column_config.NumberColumn("Previous weight (%)", format="%.2f%%", width="small"),
            "New weight (%)": st.column_config.NumberColumn("New weight (%)", format="%.2f%%", width="small"),
            "Sell weight (%)": st.column_config.NumberColumn("Sell weight (%)", format="%.2f%%", width="small"),
            "Sell value (₹)": st.column_config.NumberColumn("Sell value (₹)", format="₹%d", width="small"),
            "Qty to sell": st.column_config.NumberColumn("Qty to sell", format="%d", width="small"),
            "Sell type": st.column_config.TextColumn("Sell type", width="small"),
            "Reason": st.column_config.TextColumn("Reason"),
        },
    )

    # ── HOLD ─────────────────────────────────────────────────────────────────
    st.markdown("---")
    hold_columns = ["Ticker", "Previous weight (%)", "New weight (%)", "Weight change (%)", "Position value (₹)"]
    hold_rows = [{**row, "Position value (₹)": _val(row["New weight (%)"])} for row in hold_changes]
    st.markdown(f"#### Hold — no position changes &nbsp;({len(hold_rows)})")
    st.dataframe(
        pd.DataFrame(hold_rows, columns=hold_columns),
        hide_index=True,
        width="stretch",
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Previous weight (%)": st.column_config.NumberColumn("Previous weight (%)", format="%.2f%%", width="small"),
            "New weight (%)": st.column_config.NumberColumn("New weight (%)", format="%.2f%%", width="small"),
            "Weight change (%)": st.column_config.NumberColumn("Weight change (%)", format="%.3f%%", width="small"),
            "Position value (₹)": st.column_config.NumberColumn("Position value (₹)", format="₹%d", width="small"),
        },
    )

    # ── COMPLETE PORTFOLIO ───────────────────────────────────────────────────
    st.markdown("---")
    complete_columns = [
        "Ticker",
        "Previous weight (%)",
        "New weight (%)",
        "Weight change (%)",
        "Action",
        "Latest price date",
        "Tradability",
    ]
    complete_rows = [
        {
            **row,
            "Latest price date": latest_price_dates.get(row["Ticker"], ""),
            "Tradability": tradability_status.get(row["Ticker"], "NO DATA"),
        }
        for row in portfolio_changes
    ]
    if complete_rows:
        complete_rows.append(
            {
                "Ticker": "TOTAL",
                "Previous weight (%)": round(sum(row["Previous weight (%)"] for row in portfolio_changes), 4),
                "New weight (%)": round(sum(row["New weight (%)"] for row in portfolio_changes), 4),
                "Weight change (%)": round(sum(row["Weight change (%)"] for row in portfolio_changes), 4),
                "Action": "",
                "Latest price date": "",
                "Tradability": "",
            }
        )
    st.markdown(f"#### Complete portfolio &nbsp;({len(portfolio_changes)})")
    st.dataframe(
        pd.DataFrame(complete_rows, columns=complete_columns),
        hide_index=True,
        width="stretch",
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Previous weight (%)": st.column_config.NumberColumn("Previous weight (%)", format="%.2f%%", width="small"),
            "New weight (%)": st.column_config.NumberColumn("New weight (%)", format="%.2f%%", width="small"),
            "Weight change (%)": st.column_config.NumberColumn("Weight change (%)", format="%.2f%%", width="small"),
            "Action": st.column_config.TextColumn("Action", width="small"),
            "Latest price date": st.column_config.TextColumn("Latest price date", width="small"),
            "Tradability": st.column_config.TextColumn("Tradability", width="small"),
        },
    )

    # ── download ─────────────────────────────────────────────────────────────
    st.markdown("---")
    trade_rows: list[dict] = []
    _base = {
        "Signal Date": str(params["signal_date"]),
        "Rebalance Date": str(rebalance_date),
        "Execute Date": str(exec_date),
        "Portfolio Reset Date": str(reset_date or ""),
        "Replay ID": result.get("strategy_fingerprint", ""),
        "Portfolio Value (₹)": portfolio_value,
    }
    for change in portfolio_changes:
        ticker = change["Ticker"]
        action = change["Action"]
        if action == "BUY":
            qty = joint_qtys.get(ticker, "") or ""
            reason = entries.get(ticker, "") or ("Top-M" if ticker in entries else "Weight increase")
        elif action == "SELL":
            qty_source = exit_qtys if ticker in complete_exit_tickers else joint_qtys
            qty = qty_source.get(ticker, "") or ""
            reason = exits.get(ticker, "") or ("WRH" if ticker in exits else "Weight reduction")
        else:
            qty = ""
            reason = "No change"
        trade_rows.append(
            {
                "Ticker": ticker,
                "Action": action,
                "Previous Weight (%)": round(change["Previous weight (%)"], 4),
                "New Weight (%)": round(change["New weight (%)"], 4),
                "Weight Change (%)": round(change["Weight change (%)"], 4),
                "Trade Value (₹)": _val(abs(change["Weight change (%)"])) if action != "HOLD" else 0,
                "Qty": qty,
                "Reason": reason,
                "Latest Price Date": latest_price_dates.get(ticker, ""),
                "Tradability": tradability_status.get(ticker, "NO DATA"),
                **_base,
            }
        )

    trade_df = pd.DataFrame(trade_rows)
    if blocking_stale_incumbents:
        st.warning("CSV download is unavailable until the stale-incumbent error is resolved.")
    else:
        st.download_button(
            "📥 Download Trade List (CSV)",
            trade_df.to_csv(index=False).encode("utf-8"),
            file_name=f"live_signal_{params['signal_date']}.csv",
            mime="text/csv",
            width="stretch",
        )
