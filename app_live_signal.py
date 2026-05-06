#!/usr/bin/env python3
"""
Live Signal tab — weekly trade instructions from a warm-up backtest snapshot.

Runs run_backtest over a short warm-up window (default 52 weeks) ending on the
chosen signal date, then snapshots the last rebalance event for trade execution.
"""

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
        help="When you started (or plan to start) your portfolio. "
        "Shown on the signal for reference — does not affect the simulation.",
    )

    warmup = st.slider(
        "Simulation lookback (weeks)",
        26,
        156,
        52,
        step=4,
        key="ls_warmup",
        help="How far back to run the backtest engine to compute Marginal weight drift. "
        "Independent of portfolio start date.",
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

    for _lvl, _msg in check_data_freshness():
        (st.error if _lvl == "error" else st.warning)(_msg)
    st.divider()

    if st.button("📡 Generate Signal", type="primary", width="stretch", key="ls_run_btn"):
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
    from backtest_engine import BacktestConfig, run_backtest
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
    if indices:
        constituents = _load_constituents()
        allowed = {s for idx, syms in constituents.items() if idx in indices for s in syms}
        symbol_data = {s: df for s, df in symbol_data_all.items() if s in allowed}
    else:
        symbol_data = symbol_data_all

    compositions_df = load_compositions()
    benchmarks = load_benchmark_series()

    signal_date = params["signal_date"]
    start_date = signal_date - timedelta(weeks=params["warmup"])

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
    if len(holdings_log) >= 2:
        tickers_needed.update(holdings_log[-2].get("holdings", []))

    target_date = pd.Timestamp(params["signal_date"])
    close_prices: dict[str, float] = {}
    for ticker in tickers_needed:
        if ticker not in symbol_data:
            continue
        closes = symbol_data[ticker]["Close"]
        avail = closes[closes.index <= target_date].dropna()
        if not avail.empty:
            close_prices[ticker] = float(avail.iloc[-1])

    return {
        "holdings_log": holdings_log,
        "ohlcv_date": ohlcv_date,
        "ohlcv_source": src,
        "close_prices": close_prices,
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
    weights: dict[str, float] = current[weight_key]
    prev_weights: dict[str, float] = previous[weight_key] if previous else {}

    # Parse entries / exits (stored as "TICKER (reason)" strings)
    entries: dict[str, str] = {t: r for t, r in (_parse_ticker_reason(s) for s in current.get("entries", []))}
    exits: dict[str, str] = {t: r for t, r in (_parse_ticker_reason(s) for s in current.get("exits", []))}
    holdings: set[str] = set(current.get("holdings", []))

    # Fresh portfolio: start date is on or after signal date — no existing positions
    portfolio_start = params.get("portfolio_start")
    fresh_portfolio = portfolio_start is not None and portfolio_start >= params["signal_date"]

    # When fresh, everything is a buy — ignore engine's entry/incumbent split
    if fresh_portfolio:
        entries = {t: "" for t in holdings}
        exits = {}
    incumbents: set[str] = holdings - set(entries)

    rebalance_date = _to_date(current["date"])
    exec_date = _next_business_day(rebalance_date)

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
    sim_start = params["signal_date"] - timedelta(weeks=params["warmup"])
    portfolio_start = params.get("portfolio_start")
    port_lbl = portfolio_start.strftime("%b %d, %Y") if portfolio_start else "—"
    fresh_lbl = " 🆕 **Fresh portfolio — all positions are BUY**" if fresh_portfolio else ""
    st.caption(
        f"Portfolio start: **{port_lbl}**{fresh_lbl} "
        f"· Simulation from: **{sim_start.strftime('%b %d, %Y')}** ({params['warmup']} weeks back) "
        f"· Data as-of: **{ohlcv_date}** "
        f"· Last rebalance: **{rebalance_date.strftime('%a %b %d, %Y')}** "
        f"· Execute on: **{exec_date.strftime('%a %b %d, %Y')}** at or after 09:15 IST"
    )
    st.divider()

    # ── portfolio sizing helpers ──────────────────────────────────────────────
    portfolio_value: int = params.get("portfolio_value", 1_000_000)
    close_prices: dict[str, float] = result.get("close_prices", {})

    def _val(weight_pct: float) -> int:
        return round(portfolio_value * weight_pct / 100)

    def _qty(ticker: str, weight_pct: float) -> int | None:
        price = close_prices.get(ticker)
        v = portfolio_value * weight_pct / 100
        return int(v / price) if price and price > 0 else None

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

    # ── metric cards ─────────────────────────────────────────────────────────
    turnover_key = (
        "prop_turnover_pct" if "Prop" in _v else "marg_turnover_pct" if "Marginal" in _v else "full_turnover_pct"
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Holdings", len(holdings))
    if fresh_portfolio:
        c2.metric("Buy all", len(holdings), delta="fresh start")
        c3.metric("Exits", 0)
    else:
        c2.metric("New entries", len(entries), delta=f"+{len(entries)}" if entries else None)
        c3.metric("Exits", len(exits), delta=f"-{len(exits)}" if exits else None, delta_color="inverse")
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
    # Exits are independent of entries; allocate separately.
    _exit_sell_targets = {t: prev_weights.get(t, weights.get(t, 0.0)) * portfolio_value / 100 for t in exits}
    exit_qtys: dict[str, int] = _allocate_qtys({}, _exit_sell_targets, close_prices)

    # Entries and hold-trims are two sides of the same cash flow; allocate jointly.
    _is_marginal_alloc = params["variant"] != "Full Rebalance"
    _target_eq_alloc = round(100.0 / len(holdings), 4) if holdings else 0.0
    _buy_targets: dict[str, float] = {t: weights.get(t, 0.0) * portfolio_value / 100 for t in entries}
    _sell_targets: dict[str, float] = {}
    if _is_marginal_alloc:
        for t in incumbents:
            _cur_w = weights.get(t, 0.0)
            _prev_w = prev_weights.get(t, 0.0) if previous else _cur_w
            _delta_w = _cur_w - _prev_w
            if _delta_w < -0.001:
                _sell_targets[t] = abs(_delta_w) * portfolio_value / 100
    else:
        for t in incumbents:
            _delta_w = _target_eq_alloc - weights.get(t, 0.0)
            if _delta_w > 0.001:
                _buy_targets[t] = _delta_w * portfolio_value / 100
            elif _delta_w < -0.001:
                _sell_targets[t] = abs(_delta_w) * portfolio_value / 100
    joint_qtys: dict[str, int] = _allocate_qtys(_buy_targets, _sell_targets, close_prices)

    # ── EXITS ────────────────────────────────────────────────────────────────
    st.markdown("---")
    if exits:
        st.markdown(f"#### Sell — exit entirely &nbsp;({len(exits)})")
        exit_rows = []
        for t, r in exits.items():
            w = prev_weights.get(t, weights.get(t, 0.0))
            exit_rows.append(
                {
                    "Ticker": t,
                    "Weight held (%)": w,
                    "Value (₹)": _val(w),
                    "Qty to sell": exit_qtys.get(t),
                    "Exit reason": r or "WRH",
                }
            )
        exit_rows.sort(key=lambda x: -x["Weight held (%)"])
        total_exit_w = sum(r["Weight held (%)"] for r in exit_rows)
        total_exit_v = sum(r["Value (₹)"] for r in exit_rows)
        exit_rows.append(
            {
                "Ticker": "TOTAL",
                "Weight held (%)": round(total_exit_w, 4),
                "Value (₹)": total_exit_v,
                "Qty to sell": None,
                "Exit reason": "",
            }
        )
        st.dataframe(
            pd.DataFrame(exit_rows),
            hide_index=True,
            use_container_width=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Weight held (%)": st.column_config.NumberColumn("Weight held (%)", format="%.2f%%", width="small"),
                "Value (₹)": st.column_config.NumberColumn("Value (₹)", format="₹%d", width="small"),
                "Qty to sell": st.column_config.NumberColumn("Qty to sell", format="%d", width="small"),
                "Exit reason": st.column_config.TextColumn("Exit reason"),
            },
        )
    else:
        st.info("No exits this week.")

    # ── ENTRIES ──────────────────────────────────────────────────────────────
    st.markdown("---")
    if entries:
        st.markdown(f"#### Buy — new positions &nbsp;({len(entries)})")
        # Capital source summary
        freed_value = portfolio_value * sum(prev_weights.get(t, 0.0) for t in exits) / 100
        total_entry_cost = portfolio_value * sum(weights.get(t, 0.0) for t in entries) / 100
        per_entry = total_entry_cost / len(entries)
        if fresh_portfolio:
            st.info(
                f"**Fresh portfolio:** Deploying **₹{portfolio_value:,.0f}** across "
                f"{len(holdings)} positions (**₹{per_entry:,.0f}** avg per position)"
            )
        elif exits and not incumbents:
            # exits fully fund the entries, no dilution needed
            st.info(
                f"**Capital source:** {len(exits)} exit(s) free "
                f"**₹{freed_value:,.0f}** → allocated across {len(entries)} "
                f"new entr{'y' if len(entries) == 1 else 'ies'} "
                f"(**₹{per_entry:,.0f}** each)"
            )
        elif exits and incumbents:
            # partial exits + dilution of incumbents
            dilution_value = total_entry_cost - freed_value
            avg_dilution_pct = (dilution_value / portfolio_value * 100) / len(incumbents) if incumbents else 0
            st.info(
                f"**Capital source:** {len(exits)} exit(s) free **₹{freed_value:,.0f}**"
                f" + proportional dilution of {len(incumbents)} existing holdings"
                f" (~**₹{dilution_value:,.0f}** / avg **{avg_dilution_pct:.2f}%** each)"
                f" → total **₹{total_entry_cost:,.0f}** for {len(entries)} new "
                f"entr{'y' if len(entries) == 1 else 'ies'} (**₹{per_entry:,.0f}** each)"
            )
        else:
            # no exits at all — purely funded by diluting incumbents
            avg_dilution_pct = (total_entry_cost / portfolio_value * 100) / len(incumbents) if incumbents else 0
            st.info(
                f"**Capital source:** No exits this week. "
                f"**₹{total_entry_cost:,.0f}** funded by proportional dilution of "
                f"{len(incumbents)} existing holdings "
                f"(avg **{avg_dilution_pct:.2f}%** trimmed from each → "
                f"**₹{per_entry:,.0f}** per new entr{'y' if len(entries) == 1 else 'y'})"
            )
        entry_rows = []
        for t, r in entries.items():
            w = weights.get(t, 0.0)
            entry_rows.append(
                {
                    "Ticker": t,
                    "Target weight (%)": w,
                    "Value (₹)": _val(w),
                    "Qty to buy": joint_qtys.get(t),
                    "Entry reason": r or "Top-M",
                }
            )
        entry_rows.sort(key=lambda x: -x["Target weight (%)"])
        total_entry_w = sum(r["Target weight (%)"] for r in entry_rows)
        total_entry_v = sum(r["Value (₹)"] for r in entry_rows)
        entry_rows.append(
            {
                "Ticker": "TOTAL",
                "Target weight (%)": round(total_entry_w, 4),
                "Value (₹)": total_entry_v,
                "Qty to buy": None,
                "Entry reason": "",
            }
        )
        st.dataframe(
            pd.DataFrame(entry_rows),
            hide_index=True,
            use_container_width=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Target weight (%)": st.column_config.NumberColumn("Target weight (%)", format="%.2f%%", width="small"),
                "Value (₹)": st.column_config.NumberColumn("Value (₹)", format="₹%d", width="small"),
                "Qty to buy": st.column_config.NumberColumn("Qty to buy", format="%d", width="small"),
                "Entry reason": st.column_config.TextColumn("Entry reason"),
            },
        )
    else:
        st.info("No new entries this week.")

    # ── INCUMBENTS ───────────────────────────────────────────────────────────
    st.markdown("---")
    if incumbents:
        is_full = params["variant"] == "Full Rebalance"
        is_marginal = not is_full
        section_title = "Rebalance to equal weight — all touched" if is_full else "Hold / partial sell"
        st.markdown(f"#### {section_title} &nbsp;({len(incumbents)})")

        target_eq = round(100.0 / len(holdings), 4) if len(holdings) > 0 else 0.0
        inc_rows = []
        for t in sorted(incumbents):
            cur_w = weights.get(t, 0.0)
            prev_w = prev_weights.get(t, 0.0) if previous else cur_w
            row: dict = {
                "Ticker": t,
                "Prev weight (%)": prev_w,
                "Current weight (%)": cur_w,
            }
            if is_marginal:
                delta_w = cur_w - prev_w  # negative = need to sell
                row["Change (%)"] = round(delta_w, 4)
                row["Trade value (₹)"] = _val(abs(delta_w))
                qty = joint_qtys.get(t, 0)
                row["Qty"] = qty
                row["Action"] = "SELL" if (delta_w < -0.001 and qty > 0) else "HOLD"
            else:
                row["Target weight (%)"] = target_eq
                row["Target value (₹)"] = _val(target_eq)
                delta_w = target_eq - cur_w
                row["Qty delta"] = joint_qtys.get(t, 0)
                row["Action"] = "BUY" if delta_w > 0 else "SELL"
            inc_rows.append(row)

        inc_rows.sort(key=lambda x: x.get("Change (%)", 0.0))  # SELLs (most negative) first for marginal
        if is_full:
            inc_rows.sort(key=lambda x: -x["Current weight (%)"])

        # total row
        total_prev_w = round(sum(r.get("Prev weight (%)", 0.0) for r in inc_rows), 4)
        total_cur_w = round(sum(r["Current weight (%)"] for r in inc_rows), 4)
        total_row: dict = {"Ticker": "TOTAL", "Prev weight (%)": total_prev_w, "Current weight (%)": total_cur_w}
        if is_marginal:
            sell_rows = [r for r in inc_rows if r.get("Action") == "SELL"]
            total_row["Change (%)"] = round(sum(r["Change (%)"] for r in inc_rows), 4)
            total_row["Trade value (₹)"] = sum(r["Trade value (₹)"] for r in sell_rows)
            total_row["Qty"] = None
            total_row["Action"] = f"{len(sell_rows)} sell(s)"
        else:
            total_row["Target weight (%)"] = round(sum(r.get("Target weight (%)", 0.0) for r in inc_rows), 4)
            total_row["Target value (₹)"] = sum(r.get("Target value (₹)", 0) for r in inc_rows)
            total_row["Qty delta"] = None
            total_row["Action"] = ""
        inc_rows.append(total_row)

        inc_df = pd.DataFrame(inc_rows)
        col_cfg: dict = {
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Prev weight (%)": st.column_config.NumberColumn("Prev weight (%)", format="%.2f%%", width="small"),
            "Current weight (%)": st.column_config.NumberColumn("Current weight (%)", format="%.2f%%", width="small"),
        }
        if is_marginal:
            col_cfg["Change (%)"] = st.column_config.NumberColumn("Change (%)", format="%.2f%%", width="small")
            col_cfg["Trade value (₹)"] = st.column_config.NumberColumn("Trade value (₹)", format="₹%d", width="small")
            col_cfg["Qty"] = st.column_config.NumberColumn("Qty", format="%d", width="small")
            col_cfg["Action"] = st.column_config.TextColumn("Action", width="small")
        else:
            col_cfg["Target weight (%)"] = st.column_config.NumberColumn(
                "Target weight (%)", format="%.2f%%", width="small"
            )
            col_cfg["Target value (₹)"] = st.column_config.NumberColumn("Target value (₹)", format="₹%d", width="small")
            col_cfg["Qty delta"] = st.column_config.NumberColumn("Qty delta", format="%d", width="small")
            col_cfg["Action"] = st.column_config.TextColumn("Action", width="small")

        st.dataframe(inc_df, hide_index=True, use_container_width=True, column_config=col_cfg)

    # ── download ─────────────────────────────────────────────────────────────
    st.markdown("---")
    trade_rows: list[dict] = []
    _base = {
        "Signal Date": str(params["signal_date"]),
        "Rebalance Date": str(rebalance_date),
        "Execute Date": str(exec_date),
        "Portfolio Value (₹)": portfolio_value,
    }
    for t, r in exits.items():
        w = prev_weights.get(t, weights.get(t, 0.0))
        trade_rows.append(
            {
                "Ticker": t,
                "Action": "SELL",
                "Target Weight (%)": 0.0,
                "Value (₹)": _val(w),
                "Qty": exit_qtys.get(t, "") or "",
                "Reason": r or "WRH",
                **_base,
            }
        )
    for t, r in entries.items():
        w = round(weights.get(t, 0.0), 4)
        trade_rows.append(
            {
                "Ticker": t,
                "Action": "BUY",
                "Target Weight (%)": w,
                "Value (₹)": _val(w),
                "Qty": joint_qtys.get(t, "") or "",
                "Reason": r or "Top-M",
                **_base,
            }
        )
    for t in sorted(incumbents):
        action = "REBALANCE" if params["variant"] == "Full Rebalance" else "HOLD"
        w = round(weights.get(t, 0.0), 4)
        trade_rows.append(
            {
                "Ticker": t,
                "Action": action,
                "Target Weight (%)": w,
                "Value (₹)": _val(w),
                "Qty": _qty(t, w) or "",
                "Reason": "Incumbent",
                **_base,
            }
        )

    trade_df = pd.DataFrame(trade_rows)
    st.download_button(
        "📥 Download Trade List (CSV)",
        trade_df.to_csv(index=False).encode("utf-8"),
        file_name=f"live_signal_{params['signal_date']}.csv",
        mime="text/csv",
        width="stretch",
    )
