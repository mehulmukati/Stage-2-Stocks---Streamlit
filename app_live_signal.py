#!/usr/bin/env python3
"""
Live Signal tab — weekly trade instructions from a warm-up backtest snapshot.

Runs run_backtest over a short warm-up window (default 52 weeks) ending on the
chosen signal date, then snapshots the last rebalance event for trade execution.
"""

import hashlib
import io
import json
import warnings
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from data import check_data_freshness, load_nse_holidays

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

_REPLAY_BUFFER_WEEKS = 52


# ── helpers ──────────────────────────────────────────────────────────────────


def _next_business_day(d: date, holidays: frozenset[str] | set[str] | None = None) -> date:
    """Return the next NSE Capital Market session after *d*."""
    holidays = load_nse_holidays() if holidays is None else holidays
    d = d + timedelta(days=1)
    while d.weekday() >= 5 or d.isoformat() in holidays:
        d += timedelta(days=1)
    return d


def _previous_business_day(d: date) -> date:
    """Return the prior weekday before *d* (Mon–Fri calendar only)."""
    d = d - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _simulation_start_date(params: dict) -> date:
    """Return the internal replay start used to establish the inception reset.

    The strategy is ranked at the close before a portfolio start date and is
    executed on that start date. The fixed buffer is deliberately not a user
    setting: pre-inception holdings are discarded by the backtest engine.
    """
    portfolio_start = params.get("portfolio_start")
    anchor = _previous_business_day(portfolio_start) if portfolio_start else params["signal_date"]
    return anchor - timedelta(weeks=_REPLAY_BUFFER_WEEKS)


def _strategy_fingerprint(params: dict, ohlcv_date: object, ohlcv_source: object) -> str:
    """Stable identity for one reproducible live-portfolio replay.

    Signal date and portfolio value are deliberately excluded: they do not
    define the strategy path.  Every selection, risk and quality setting that
    can alter holdings is included, as is the data version reported by the
    loader.
    """
    fields = (
        "portfolio_start",
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


def _comparison_weights_for_live_event(
    current: dict,
    previous: dict | None,
    weight_key: str,
    pre_rebalance_key: str,
    fresh_portfolio: bool,
) -> dict[str, float]:
    """Return the executable pre-trade weights for a live event.

    New engine results carry the price-drifted, corporate-action-adjusted
    portfolio on the current event. The fallback keeps old cached results
    renderable until Streamlit regenerates them.
    """
    if fresh_portfolio:
        return {}
    previous_target = dict(previous.get(weight_key, {})) if previous else {}
    if pre_rebalance_key in current:
        return dict(current[pre_rebalance_key])
    for action in current.get("corporate_actions", []):
        old_symbol = action["old_symbol"]
        successor_symbol = action["successor_symbol"]
        if old_symbol in previous_target:
            previous_target[successor_symbol] = previous_target.get(successor_symbol, 0.0) + previous_target.pop(
                old_symbol
            )
    return previous_target


def _normalise_broker_snapshot(raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Return a strict Ticker/Quantity broker snapshot and validation errors."""
    empty = pd.DataFrame(columns=["Ticker", "Quantity"])
    if raw is None or raw.empty:
        return empty, []

    aliases = {
        "ticker": "Ticker",
        "symbol": "Ticker",
        "tradingsymbol": "Ticker",
        "security": "Ticker",
        "quantity": "Quantity",
        "qty": "Quantity",
        "netqty": "Quantity",
        "shares": "Quantity",
    }
    renamed: dict[object, str] = {}
    for column in raw.columns:
        key = "".join(ch for ch in str(column).lower() if ch.isalnum())
        if key in aliases and aliases[key] not in renamed.values():
            renamed[column] = aliases[key]
    frame = raw.rename(columns=renamed)
    missing = [column for column in ("Ticker", "Quantity") if column not in frame.columns]
    if missing:
        return empty, ["Broker snapshot requires Ticker and Quantity columns."]

    frame = frame[["Ticker", "Quantity"]].copy()
    frame["Ticker"] = (
        frame["Ticker"].fillna("").astype(str).str.strip().str.upper().str.replace(r"\.NS$", "", regex=True)
    )
    frame = frame[frame["Ticker"] != ""].reset_index(drop=True)
    if frame.empty:
        return empty, []

    errors: list[str] = []
    numeric_qty = pd.to_numeric(frame["Quantity"], errors="coerce")
    bad_qty = frame.loc[numeric_qty.isna(), "Ticker"].tolist()
    if bad_qty:
        errors.append("Non-numeric quantity for: " + ", ".join(bad_qty))
    fractional = frame.loc[numeric_qty.notna() & ((numeric_qty % 1).abs() > 1e-9), "Ticker"].tolist()
    if fractional:
        errors.append("Quantities must be whole shares for: " + ", ".join(fractional))
    negative = frame.loc[numeric_qty.notna() & (numeric_qty < 0), "Ticker"].tolist()
    if negative:
        errors.append("Negative quantities are not supported for: " + ", ".join(negative))
    duplicates = sorted(frame.loc[frame["Ticker"].duplicated(keep=False), "Ticker"].unique())
    if duplicates:
        errors.append("Duplicate tickers must be consolidated: " + ", ".join(duplicates))

    if errors:
        return frame, errors
    frame["Quantity"] = numeric_qty.astype(int)
    frame = frame[frame["Quantity"] > 0].sort_values("Ticker").reset_index(drop=True)
    return frame, []


def _read_broker_snapshot(file_name: str, payload: bytes) -> tuple[pd.DataFrame, list[str]]:
    """Read a fresh CSV/XLSX broker export and normalise its required columns."""
    try:
        suffix = file_name.lower().rsplit(".", 1)[-1]
        if suffix == "csv":
            raw = pd.read_csv(io.BytesIO(payload))
        elif suffix == "xlsx":
            raw = pd.read_excel(io.BytesIO(payload))
        else:
            return pd.DataFrame(columns=["Ticker", "Quantity"]), ["Upload a CSV or XLSX file."]
    except Exception as exc:
        return pd.DataFrame(columns=["Ticker", "Quantity"]), [f"Could not read broker snapshot: {exc}"]
    return _normalise_broker_snapshot(raw)


def _reconcile_actual_portfolio(
    snapshot: pd.DataFrame,
    cash: float,
    reserve_cash: float,
    target_weights: dict[str, float],
    prices: dict[str, float],
) -> dict:
    """Convert strategy weights and actual holdings into executable whole-share deltas."""
    positions, errors = _normalise_broker_snapshot(snapshot)
    cash = float(cash)
    reserve_cash = float(reserve_cash)
    if cash < 0:
        errors.append("Available cash cannot be negative.")

    actual_qty = dict(zip(positions.get("Ticker", []), positions.get("Quantity", [])))
    positive_targets = {ticker: float(weight) for ticker, weight in target_weights.items() if float(weight) > 0}
    target_total = sum(positive_targets.values())
    normalised_targets = (
        {ticker: weight * 100.0 / target_total for ticker, weight in positive_targets.items()}
        if target_total > 0
        else {}
    )
    required = set(actual_qty) | set(normalised_targets)
    missing_prices = sorted(ticker for ticker in required if not prices.get(ticker, 0) > 0)
    if missing_prices:
        errors.append("No signal-date price for: " + ", ".join(missing_prices))

    securities_value = sum(actual_qty[ticker] * prices.get(ticker, 0.0) for ticker in actual_qty)
    gross_value = cash + securities_value
    if gross_value <= 0:
        errors.append("Broker snapshot plus cash must have a positive value.")
    if reserve_cash < 0:
        errors.append("Minimum cash reserve cannot be negative.")
    if reserve_cash > gross_value:
        errors.append("Minimum cash reserve exceeds the marked-to-market portfolio value.")
    if errors:
        return {
            "errors": errors,
            "rows": [],
            "gross_value": gross_value,
            "securities_value": securities_value,
            "cash": cash,
        }

    investable_value = gross_value - reserve_cash
    rows: list[dict] = []
    for ticker in sorted(required):
        price = float(prices[ticker])
        current_qty = int(actual_qty.get(ticker, 0))
        strategy_weight = normalised_targets.get(ticker, 0.0)
        target_value = investable_value * strategy_weight / 100.0
        target_qty = int(target_value // price)
        order_qty = target_qty - current_qty
        action = "BUY" if order_qty > 0 else "SELL" if order_qty < 0 else "HOLD"
        current_value = current_qty * price
        projected_value = target_qty * price
        rows.append(
            {
                "Ticker": ticker,
                "Price": price,
                "Actual quantity": current_qty,
                "Actual value (₹)": current_value,
                "Actual weight (%)": current_value / gross_value * 100.0,
                "Strategy target (%)": strategy_weight,
                "Target value (₹)": target_value,
                "Target quantity": target_qty,
                "Order quantity": order_qty,
                "Action": action,
                "Trade value (₹)": abs(order_qty) * price,
                "Projected value (₹)": projected_value,
                "Projected weight (%)": projected_value / gross_value * 100.0,
            }
        )

    projected_securities = sum(row["Projected value (₹)"] for row in rows)
    projected_cash = gross_value - projected_securities
    turnover_pct = sum(row["Trade value (₹)"] for row in rows) / gross_value * 100.0
    return {
        "errors": [],
        "rows": rows,
        "gross_value": gross_value,
        "securities_value": securities_value,
        "cash": cash,
        "reserve_cash": reserve_cash,
        "investable_value": investable_value,
        "projected_cash": projected_cash,
        "turnover_pct": turnover_pct,
    }


def _portfolio_from_replay(
    weights: dict[str, float],
    portfolio_value: float,
    prices: dict[str, float],
) -> tuple[pd.DataFrame, float, list[str]]:
    """Turn replayed pre-trade weights into whole-share positions plus residual cash."""
    positive_weights = {ticker: float(weight) for ticker, weight in weights.items() if float(weight) > 0}
    missing_prices = sorted(ticker for ticker in positive_weights if not prices.get(ticker, 0) > 0)
    if missing_prices:
        return (
            pd.DataFrame(columns=["Ticker", "Quantity"]),
            float(portfolio_value),
            ["No signal-date price for replayed holding(s): " + ", ".join(missing_prices)],
        )

    total_weight = sum(positive_weights.values())
    if total_weight <= 0:
        return pd.DataFrame(columns=["Ticker", "Quantity"]), float(portfolio_value), []

    positions = []
    securities_value = 0.0
    for ticker, weight in sorted(positive_weights.items()):
        price = float(prices[ticker])
        quantity = int((float(portfolio_value) * weight / total_weight) // price)
        if quantity > 0:
            positions.append({"Ticker": ticker, "Quantity": quantity})
            securities_value += quantity * price
    cash = max(0.0, float(portfolio_value) - securities_value)
    return pd.DataFrame(positions, columns=["Ticker", "Quantity"]), cash, []


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


# ── inputs ───────────────────────────────────────────────────────────────────


def _live_signal_inputs(idx_options: list[str]) -> dict:
    st.markdown("### Portfolio basis")
    source_label = st.radio(
        "Choose how Live Signal determines your portfolio before this rebalance",
        ["Replay from start date", "Use current portfolio snapshot"],
        horizontal=True,
        key="ls_portfolio_source",
    )
    portfolio_source = "replay" if source_label.startswith("Replay") else "snapshot"
    st.caption(
        "Replay reconstructs the model portfolio from inception. Snapshot reconciles the positions you actually own."
    )

    date_col, history_col = st.columns(2)
    signal_date = date_col.date_input(
        "Signal date",
        value=date.today(),
        max_value=date.today(),
        key="ls_signal_date",
        help="Closing prices from this date are used for ranking. Trades execute on the next NSE session.",
    )
    portfolio_start = history_col.date_input(
        "Portfolio start date" if portfolio_source == "replay" else "Strategy inception date",
        value=date.today() + timedelta(days=1),
        key="ls_portfolio_start",
        help="Anchors the strategy path. Keep this date unchanged for subsequent signals.",
    )

    broker_snapshot = pd.DataFrame(columns=["Ticker", "Quantity"])
    broker_errors: list[str] = []
    cash_balance = 0.0
    reserve_cash = 0.0
    expected_portfolio_value = 0.0
    portfolio_value = 0.0

    if portfolio_source == "replay":
        portfolio_value = st.number_input(
            "Portfolio value at signal date (₹)",
            min_value=10_000,
            max_value=100_000_000,
            value=1_000_000,
            step=10_000,
            format="%d",
            key="ls_portfolio_value",
            help="Used to turn the replayed model weights into indicative whole-share quantities.",
        )
        st.info("No holdings upload is needed. Trades will be calculated from the replayed strategy portfolio.")
    else:
        with st.expander("Current positions", expanded=True):
            upload_col, template_col = st.columns([3, 1])
            uploaded_snapshot = upload_col.file_uploader(
                "Upload broker positions",
                type=["csv", "xlsx"],
                key="ls_broker_snapshot_upload",
                help="Accepted aliases include Symbol/Trading Symbol and Qty/Net Qty/Shares.",
            )
            template_col.download_button(
                "Template",
                b"Ticker,Quantity\n",
                file_name="live_signal_broker_snapshot_template.csv",
                mime="text/csv",
                width="stretch",
                key="ls_snapshot_template",
            )
            imported_snapshot = pd.DataFrame(columns=["Ticker", "Quantity"])
            snapshot_key = "manual"
            if uploaded_snapshot is not None:
                payload = uploaded_snapshot.getvalue()
                snapshot_key = hashlib.sha256(payload).hexdigest()[:10]
                imported_snapshot, broker_errors = _read_broker_snapshot(uploaded_snapshot.name, payload)
            editor_seed = (
                imported_snapshot if not imported_snapshot.empty else pd.DataFrame([{"Ticker": "", "Quantity": 0}])
            )
            edited_snapshot = st.data_editor(
                editor_seed,
                num_rows="dynamic",
                hide_index=True,
                width="stretch",
                key=f"ls_broker_positions_{snapshot_key}",
                column_config={
                    "Ticker": st.column_config.TextColumn("Ticker", required=True),
                    "Quantity": st.column_config.NumberColumn("Quantity", min_value=0, step=1, required=True),
                },
            )
            broker_snapshot, editor_errors = _normalise_broker_snapshot(edited_snapshot)
            broker_errors.extend(error for error in editor_errors if error not in broker_errors)
            for error in broker_errors:
                st.error(error)

        cash_col, reserve_col, check_col = st.columns(3)
        cash_balance = cash_col.number_input(
            "Available cash (₹)",
            min_value=0,
            max_value=100_000_000,
            value=0,
            step=1_000,
            format="%d",
            key="ls_cash_balance",
        )
        reserve_cash = reserve_col.number_input(
            "Minimum cash reserve (₹)",
            min_value=0,
            max_value=100_000_000,
            value=0,
            step=1_000,
            format="%d",
            key="ls_reserve_cash",
            help="Cash deliberately left uninvested after rounding.",
        )
        expected_portfolio_value = check_col.number_input(
            "Expected broker total (₹)",
            min_value=0,
            max_value=100_000_000,
            value=0,
            step=10_000,
            format="%d",
            key="ls_expected_portfolio_value",
            help="Optional cross-check.",
        )
    st.divider()

    st.markdown("**Strategy**")
    strategy_col, schedule_col = st.columns(2)
    band = strategy_col.selectbox(
        "Band rule",
        ["classic", "displacement"],
        format_func=str.capitalize,
        key="ls_band",
    )
    variant = schedule_col.selectbox(
        "Variant",
        ["Marginal Rebalance", "Prop Rebalance", "Full Rebalance"],
        key="ls_variant",
    )

    col_m, col_n = st.columns(2)
    m = col_m.number_input("M (entry)", min_value=5, max_value=50, value=15, step=1, key="ls_m")
    n = col_n.number_input("N (exit)", min_value=6, max_value=200, value=30, step=1, key="ls_n")

    ranking_col, frequency_col = st.columns(2)
    sort_method = ranking_col.selectbox("Rank by Sharpe", _SORT_OPTIONS, index=0, key="ls_sort_method")
    freq = frequency_col.selectbox(
        "Rebalance frequency",
        ["weekly", "biweekly", "monthly", "quarterly", "half-yearly"],
        key="ls_freq",
    )

    if freq == "weekly":
        with st.expander("Stage 2 signals (weekly)", expanded=False):
            entry_col, exit_col = st.columns(2)
            s2_entry = entry_col.toggle(
                "Enter on score jump",
                value=False,
                key="ls_s2_entry",
                help="Allow entry after a qualifying weekly Weinstein Stage 2 score increase.",
            )
            s2_entry_threshold = entry_col.number_input(
                "Score jump threshold",
                min_value=1,
                max_value=4,
                value=2,
                step=1,
                disabled=not s2_entry,
                key="ls_s2_entry_threshold",
            )
            s2_drop = exit_col.toggle("Exit on score drop", value=False, key="ls_s2_drop")
            s2_threshold = exit_col.number_input(
                "Drop threshold",
                min_value=1,
                max_value=4,
                value=2,
                step=1,
                disabled=not s2_drop,
                key="ls_s2_threshold",
            )
    else:
        s2_entry = False
        s2_entry_threshold = st.session_state.get("ls_s2_entry_threshold", 2)
        s2_drop = False
        s2_threshold = st.session_state.get("ls_s2_threshold", 2)

    st.markdown("**Position cap & universe**")
    risk_col, universe_col = st.columns(2)
    max_pos = risk_col.slider(
        "Max position (%)",
        0,
        50,
        15,
        step=1,
        key="ls_max_pos",
        help="0 = no cap. Recommended 15% for Marginal variant.",
    )

    indices = universe_col.multiselect(
        "Index universe",
        options=idx_options or _ALL_5_INDICES,
        default=idx_options or _ALL_5_INDICES,
        key="ls_indices",
    )
    with st.expander("Advanced quality filters", expanded=False):
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
        # Missing/invalid reference data is an error. A stale current-
        # constituent file remains blocking even though it is rendered as a
        # warning; composition TIME_STAMP values are effective dates and are
        # deliberately not treated as freshness watermarks.
        if _lvl == "error" or "constituents.json" in _msg:
            live_data_blocked = True
    if live_data_blocked:
        st.error("LiveSignal is disabled until the blocking reference-data issues above are resolved.")
        st.session_state["ls_run_triggered"] = False
        st.session_state["ls_result"] = None
    st.divider()

    snapshot_missing = portfolio_source == "snapshot" and broker_snapshot.empty and cash_balance <= 0
    if snapshot_missing:
        st.warning("Enter at least one broker position or a positive cash balance.")
    if st.button(
        "📡 Generate Signal",
        type="primary",
        width="stretch",
        key="ls_run_btn",
        disabled=live_data_blocked or bool(broker_errors) or snapshot_missing,
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
        "portfolio_source": portfolio_source,
        "portfolio_value": float(portfolio_value),
        "broker_snapshot": broker_snapshot,
        "cash_balance": float(cash_balance),
        "reserve_cash": float(reserve_cash),
        "expected_portfolio_value": float(expected_portfolio_value),
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

    indices = params["indices"]
    compositions_df = load_compositions()
    constituents = _load_constituents()
    selected_indices = indices or list(constituents)
    broker_snapshot = params.get("broker_snapshot")
    broker_tickers = (
        set(broker_snapshot["Ticker"].astype(str))
        if isinstance(broker_snapshot, pd.DataFrame) and "Ticker" in broker_snapshot.columns
        else set()
    )
    required_current_symbols = sorted(
        {symbol for index_name, symbols in constituents.items() if index_name in selected_indices for symbol in symbols}
        | broker_tickers
    )

    refresh_messages: list[dict[str, str]] = []

    def _capture_refresh(level: str, message: str) -> None:
        refresh_messages.append({"level": level, "message": message})

    sync_benchmark_data()
    ohlcv = load_ohlcv_for_backtest(emit=_capture_refresh, required_symbols=required_current_symbols)
    freshness = {
        "target_date": ohlcv.target_date,
        "actual_latest_date": ohlcv.actual_latest_date,
        "max_price_date": ohlcv.max_price_date,
        "status": ohlcv.refresh_status,
        "source": ohlcv.source,
        "attempts": ohlcv.attempts,
        "updated_count": len(ohlcv.updated_symbols),
        "required_count": len(ohlcv.requested_symbols),
        "missing_target_symbols": ohlcv.missing_target_symbols,
        "stale_symbols": ohlcv.stale_symbols,
        "error": ohlcv.refresh_error,
        "messages": refresh_messages,
    }
    benchmark_load = load_benchmark_series(with_status=True)
    freshness["benchmark"] = {
        "target_date": benchmark_load.target_date,
        "actual_latest_date": benchmark_load.actual_latest_date,
        "status": benchmark_load.status,
        "missing": benchmark_load.missing,
    }
    symbol_data_all = ohlcv.symbol_data
    if not symbol_data_all:
        return {
            "error": "OHLCV data is missing. Run: python scripts/refresh_backtest_parquet.py",
            "data_freshness": freshness,
        }
    if not ohlcv.is_usable_for_signal:
        available = ohlcv.actual_latest_date or ohlcv.max_price_date or "unknown"
        if ohlcv.stale_symbols:
            reason = f"{len(ohlcv.stale_symbols)} required symbols are more than three NSE sessions stale"
        elif ohlcv.missing_target_symbols:
            reason = f"{len(ohlcv.missing_target_symbols)} required symbols have no recent tradable price"
        else:
            reason = ohlcv.refresh_error or "required recent prices are unavailable"
        return {
            "error": (
                f"Signal not generated. Required data targets {ohlcv.target_date}, but verified coverage is "
                f"through {available}. {reason}. Click Generate Signal to retry."
            ),
            "data_freshness": freshness,
        }

    ohlcv_date = ohlcv.actual_latest_date
    src = ohlcv.source
    if indices:
        allowed = _symbols_needed_for_replay(indices, constituents, compositions_df, load_corporate_actions())
        # An actual holding may have left the selected index but still needs a
        # current price so Live Signal can generate its exit order.
        allowed.update(broker_tickers)
        symbol_data = {s: df for s, df in symbol_data_all.items() if s in allowed}
    else:
        symbol_data = symbol_data_all

    benchmarks = benchmark_load.series

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
        rank_on_rebalance_date=True,
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
    tickers_needed.update(broker_tickers)
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
        "data_freshness": freshness,
    }


# ── results renderer ──────────────────────────────────────────────────────────


def live_signal_results(params: dict) -> None:
    if not st.session_state.get("ls_run_triggered", False):
        st.info("Configure the portfolio and strategy in **Input**, then click **Generate Signal**.")
        return

    # Run only when button was clicked (ls_result cleared by button handler)
    result = st.session_state.get("ls_result")
    if result is None:
        with st.spinner("Loading data and computing signal… (~15 seconds)"):
            result = _run_signal(params)
        st.session_state["ls_result"] = result

    freshness = result.get("data_freshness", {})
    if freshness:
        target = freshness.get("target_date", "—")
        verified = freshness.get("actual_latest_date") or "not fully covered"
        max_available = freshness.get("max_price_date") or "—"
        status = str(freshness.get("status", "unknown")).replace("_", " ").title()
        attempts = int(freshness.get("attempts", 0) or 0)
        required_count = int(freshness.get("required_count", 0) or 0)
        updated_count = int(freshness.get("updated_count", 0) or 0)
        summary = (
            f"**Price data:** target NSE session **{target}** · verified universe through **{verified}** "
            f"· newest individual price **{max_available}** · status **{status}** "
            f"· source **{freshness.get('source', '—')}**"
        )
        if attempts:
            summary += f" · Yahoo attempts **{attempts}** · symbols updated **{updated_count}/{required_count}**"
        (st.success if freshness.get("status") in {"fresh", "not_needed", "memory"} else st.warning)(summary)

        missing = freshness.get("missing_target_symbols", [])
        stale = freshness.get("stale_symbols", [])
        if missing:
            sample = ", ".join(missing[:20])
            suffix = f" (+{len(missing) - 20} more)" if len(missing) > 20 else ""
            st.warning(f"Required symbols without a target-session price: {sample}{suffix}")
            if not stale:
                st.info(
                    "Signal generation is continuing with each symbol's latest tradable price. "
                    "The ranking engine allows at most three completed NSE sessions of lag."
                )
        if stale:
            sample = ", ".join(stale[:20])
            suffix = f" (+{len(stale) - 20} more)" if len(stale) > 20 else ""
            st.error(f"Required symbols more than three NSE sessions stale: {sample}{suffix}")
        if freshness.get("error") and stale:
            st.caption(f"Refresh detail: {freshness['error']}")
        benchmark = freshness.get("benchmark", {})
        if benchmark:
            benchmark_as_of = benchmark.get("actual_latest_date") or "not fully covered"
            benchmark_missing = benchmark.get("missing", [])
            benchmark_text = (
                f"Benchmark data through **{benchmark_as_of}** · status "
                f"**{str(benchmark.get('status', 'unknown')).replace('_', ' ').title()}**"
            )
            if benchmark_missing:
                benchmark_text += " · missing: " + ", ".join(benchmark_missing)
            st.caption(benchmark_text)

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
    pre_rebalance_key = f"pre_rebalance_{weight_key}"
    weights: dict[str, float] = dict(current[weight_key])

    # A merger changes the security's identity without being a market trade.
    # Move the prior snapshot to the successor so the four tables show the
    # executable successor sale instead of an impossible sale of the old scrip.
    corporate_action_events = current.get("corporate_actions", [])
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

    # ── header ───────────────────────────────────────────────────────────────
    band_lbl = params["band"].capitalize()
    var_lbl = "Prop" if "Prop" in params["variant"] else "Marginal" if "Marginal" in params["variant"] else "Full"
    cap_lbl = f" · cap {params['max_pos']}%" if params["max_pos"] > 0 else ""
    s2_lbl = f" · S2 drop={params['s2_threshold']}" if params["s2_drop"] else ""
    st.markdown(
        f"**{band_lbl} · {var_lbl} · M={params['m']} · N={params['n']}" f" · {params['sort_method']}{s2_lbl}{cap_lbl}**"
    )
    portfolio_start = params.get("portfolio_start")
    port_lbl = portfolio_start.strftime("%b %d, %Y") if portfolio_start else "—"
    fresh_lbl = " 🆕 **Strategy replay starts at this event**" if fresh_portfolio else ""
    st.caption(
        f"Portfolio start: **{port_lbl}**{fresh_lbl} "
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

    # ── actual broker portfolio reconciliation ───────────────────────────────
    close_prices: dict[str, float] = result.get("close_prices", {})
    latest_price_dates: dict[str, str] = result.get("latest_price_dates", {})
    tradability_status: dict[str, str] = result.get("tradability_status", {})
    portfolio_source = params.get("portfolio_source", "snapshot")
    if portfolio_source == "replay":
        pre_trade_weights = _comparison_weights_for_live_event(
            current, previous, weight_key, pre_rebalance_key, fresh_portfolio
        )
        broker_snapshot, cash_balance, replay_errors = _portfolio_from_replay(
            pre_trade_weights,
            params.get("portfolio_value", 0.0),
            close_prices,
        )
        if replay_errors:
            for error in replay_errors:
                st.error(error)
            st.error("Trade list blocked. The replayed portfolio could not be valued at the signal date.")
            return
        reserve_cash = 0.0
    else:
        broker_snapshot = params.get("broker_snapshot", pd.DataFrame(columns=["Ticker", "Quantity"]))
        cash_balance = params.get("cash_balance", 0.0)
        reserve_cash = params.get("reserve_cash", 0.0)
    reconciliation = _reconcile_actual_portfolio(
        broker_snapshot,
        cash_balance,
        reserve_cash,
        weights,
        close_prices,
    )
    if reconciliation["errors"]:
        for error in reconciliation["errors"]:
            st.error(error)
        st.error("Trade list blocked. Correct the broker snapshot or price data and regenerate the signal.")
        return

    portfolio_value = float(reconciliation["gross_value"])
    expected_value = float(params.get("expected_portfolio_value", 0.0)) if portfolio_source == "snapshot" else 0.0
    if expected_value > 0:
        value_gap = portfolio_value - expected_value
        value_tolerance = max(1_000.0, expected_value * 0.005)
        if abs(value_gap) > value_tolerance:
            st.warning(
                f"Broker-total cross-check differs by ₹{abs(value_gap):,.0f}: "
                f"calculated ₹{portfolio_value:,.0f} versus expected ₹{expected_value:,.0f}."
            )

    actual_rows: list[dict] = reconciliation["rows"]
    trade_rows = [row for row in actual_rows if row["Action"] != "HOLD"]
    buy_rows = sorted((row for row in trade_rows if row["Action"] == "BUY"), key=lambda row: -row["Trade value (₹)"])
    sell_rows = sorted((row for row in trade_rows if row["Action"] == "SELL"), key=lambda row: -row["Trade value (₹)"])
    projected_rows = [row for row in actual_rows if row["Target quantity"] > 0]
    blocking_trade_tickers = sorted(
        row["Ticker"] for row in trade_rows if tradability_status.get(row["Ticker"], "NO DATA") != "TRADABLE"
    )

    # ── pre-cap weights for trimming callout ─────────────────────────────────
    pre_cap_key = (
        "pre_cap_prop_weights"
        if "Prop" in _v
        else "pre_cap_marg_weights" if "Marginal" in _v else "pre_cap_full_weights"
    )
    pre_cap_weights: dict[str, float] = current.get(pre_cap_key, {})

    c1, c2, c3, c4 = st.columns(4)
    portfolio_metric = "Replayed portfolio" if portfolio_source == "replay" else "Calculated portfolio"
    c1.metric(portfolio_metric, f"₹{portfolio_value:,.0f}")
    c2.metric("Buys", len(buy_rows))
    c3.metric("Sells", len(sell_rows), delta=f"-{len(sell_rows)}" if sell_rows else None, delta_color="inverse")
    c4.metric("Actual turnover", f"{reconciliation['turnover_pct']:.1f}%")
    st.caption(
        f"Securities ₹{reconciliation['securities_value']:,.0f} · cash ₹{reconciliation['cash']:,.0f} · "
        f"reserve ₹{reconciliation['reserve_cash']:,.0f} · projected residual cash "
        f"₹{reconciliation['projected_cash']:,.0f}"
    )

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

    st.info(
        "**Execution pricing:** trade values and quantities are indicative estimates based on "
        "signal-date closing prices. Recalculate against the actual next-session price before placing orders."
    )
    if blocking_trade_tickers:
        st.error(
            "Trade list blocked because these required orders lack a current tradable price: "
            + ", ".join(blocking_trade_tickers)
        )

    common_columns = [
        "Ticker",
        "Price",
        "Actual quantity",
        "Actual value (₹)",
        "Actual weight (%)",
        "Strategy target (%)",
        "Target value (₹)",
        "Target quantity",
        "Order quantity",
        "Trade value (₹)",
    ]
    column_config = {
        "Price": st.column_config.NumberColumn("Signal price", format="₹%.2f"),
        "Actual value (₹)": st.column_config.NumberColumn("Actual value", format="₹%.0f"),
        "Actual weight (%)": st.column_config.NumberColumn("Actual weight", format="%.2f%%"),
        "Strategy target (%)": st.column_config.NumberColumn("Strategy target", format="%.2f%%"),
        "Target value (₹)": st.column_config.NumberColumn("Target value", format="₹%.0f"),
        "Trade value (₹)": st.column_config.NumberColumn("Trade value", format="₹%.0f"),
    }

    st.markdown("---")
    actual_position_count = sum(row["Actual quantity"] > 0 for row in actual_rows)
    portfolio_heading = (
        "Replayed portfolio before trading" if portfolio_source == "replay" else "Actual portfolio before trading"
    )
    st.markdown(f"#### {portfolio_heading} &nbsp;({actual_position_count})")
    st.dataframe(
        pd.DataFrame([row for row in actual_rows if row["Actual quantity"] > 0], columns=common_columns),
        hide_index=True,
        width="stretch",
        column_config=column_config,
    )

    st.markdown("---")
    st.markdown(f"#### Sell first &nbsp;({len(sell_rows)})")
    sell_display = [{**row, "Quantity to sell": abs(row["Order quantity"])} for row in sell_rows]
    st.dataframe(
        pd.DataFrame(sell_display),
        hide_index=True,
        width="stretch",
        column_config={**column_config, "Quantity to sell": st.column_config.NumberColumn(format="%d")},
    )

    st.markdown("---")
    st.markdown(f"#### Buy after sells &nbsp;({len(buy_rows)})")
    buy_display = [{**row, "Quantity to buy": row["Order quantity"]} for row in buy_rows]
    st.dataframe(
        pd.DataFrame(buy_display),
        hide_index=True,
        width="stretch",
        column_config={**column_config, "Quantity to buy": st.column_config.NumberColumn(format="%d")},
    )

    st.markdown("---")
    st.markdown(f"#### Projected post-trade portfolio &nbsp;({len(projected_rows)})")
    projected_display = [
        {
            "Ticker": row["Ticker"],
            "Target quantity": row["Target quantity"],
            "Projected value (₹)": row["Projected value (₹)"],
            "Projected weight (%)": row["Projected weight (%)"],
            "Strategy target (%)": row["Strategy target (%)"],
            "Latest price date": latest_price_dates.get(row["Ticker"], ""),
            "Tradability": tradability_status.get(row["Ticker"], "NO DATA"),
        }
        for row in projected_rows
    ]
    projected_display.append(
        {
            "Ticker": "CASH",
            "Target quantity": None,
            "Projected value (₹)": reconciliation["projected_cash"],
            "Projected weight (%)": reconciliation["projected_cash"] / portfolio_value * 100.0,
            "Strategy target (%)": 0.0,
            "Latest price date": "",
            "Tradability": "",
        }
    )
    st.dataframe(
        pd.DataFrame(projected_display),
        hide_index=True,
        width="stretch",
        column_config={
            "Projected value (₹)": st.column_config.NumberColumn(format="₹%.0f"),
            "Projected weight (%)": st.column_config.NumberColumn(format="%.2f%%"),
            "Strategy target (%)": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )

    st.markdown("---")
    download_rows = []
    for row in trade_rows:
        download_rows.append(
            {
                "Ticker": row["Ticker"],
                "Action": row["Action"],
                "Order Quantity": abs(row["Order quantity"]),
                "Actual Quantity": row["Actual quantity"],
                "Target Quantity": row["Target quantity"],
                "Signal Price": row["Price"],
                "Trade Value (₹)": row["Trade value (₹)"],
                "Actual Weight (%)": round(row["Actual weight (%)"], 4),
                "Strategy Target (%)": round(row["Strategy target (%)"], 4),
                "Signal Date": str(params["signal_date"]),
                "Execute Date": str(exec_date),
                "Latest Price Date": latest_price_dates.get(row["Ticker"], ""),
                "Tradability": tradability_status.get(row["Ticker"], "NO DATA"),
                "Calculated Portfolio Value (₹)": round(portfolio_value, 2),
                "Input Cash (₹)": reconciliation["cash"],
                "Minimum Reserve (₹)": reconciliation["reserve_cash"],
                "Projected Cash (₹)": round(reconciliation["projected_cash"], 2),
                "Replay ID": result.get("strategy_fingerprint", ""),
            }
        )
    trade_df = pd.DataFrame(download_rows)
    if blocking_stale_incumbents or blocking_trade_tickers:
        st.warning("CSV download is unavailable until all stale or missing-price errors are resolved.")
    else:
        st.download_button(
            "📥 Download Trade List (CSV)",
            trade_df.to_csv(index=False).encode("utf-8"),
            file_name=f"live_signal_{portfolio_source}_{params['signal_date']}.csv",
            mime="text/csv",
            width="stretch",
        )


def render_live_signal_tabs(idx_options: list[str]) -> None:
    """Render Live Signal inputs and output in the main content pane."""
    input_tab, results_tab = st.tabs(["⚙️ Input", "📊 Results"])
    with input_tab:
        params = _live_signal_inputs(idx_options)
    with results_tab:
        live_signal_results(params)
