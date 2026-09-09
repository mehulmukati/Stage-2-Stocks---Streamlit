"""Streamlit presentation layer for the Quant-Portfolio-Maker sub-application."""

from __future__ import annotations

import difflib
import importlib
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd
import streamlit as st

import apps.quant_portfolio_maker as qpm_navigation
import charts
import data as data_access
import ha_ema_engine
import ichimoku_engine
import ichimoku_strategy as ichimoku_strategies
from ha_ema_summary import build_ha_ema_summary
from ichimoku_summary import build_ichimoku_summary
from ichimoku_tutorial import (
    diagram_svg,
    extract_cheat_sheet,
    load_tutorial,
    strip_cheat_sheet,
    strip_title,
    tutorial_parts,
)
from strategy_replay import ReplayConfig, replay_single_stock

# Streamlit can retain the package object across script reruns. Reload an older
# object so a newly-added workspace constant is available without restarting.
if not hasattr(qpm_navigation, "PORTFOLIO_LAB_PAGE"):
    qpm_navigation = importlib.reload(qpm_navigation)

ENHANCED_ICHIMOKU_PAGE = qpm_navigation.ENHANCED_ICHIMOKU_PAGE
HA_EMA_PAGE = qpm_navigation.HA_EMA_PAGE
PORTFOLIO_LAB_PAGE = qpm_navigation.PORTFOLIO_LAB_PAGE
PAGE_LABELS = qpm_navigation.PAGE_LABELS


_DOCS_DIR = _REPO_ROOT / "docs"
_ICHI_STATE_PREFIX = "qpm_ichimoku_strategy_"


@st.cache_data(ttl=3600)
def _compute_ichimoku(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    return ichimoku_engine.compute_ichimoku(df, timeframe=timeframe)


@st.cache_data(ttl=3600)
def _compute_ha_ema_signals(df: pd.DataFrame, config: ha_ema_engine.HAEMAStrategyConfig) -> pd.DataFrame:
    return ha_ema_engine.compute_ha_ema_signals(df, config)


@st.cache_data(ttl=3600)
def _compute_ichimoku_strategy_comparison(
    df: pd.DataFrame,
    config: ichimoku_strategies.IchimokuStrategyConfig,
    replay_config: ReplayConfig,
):
    return ichimoku_strategies.build_strategy_comparison(df, config, replay_config)


def _theme_type() -> str:
    context = getattr(st, "context", None)
    theme = getattr(context, "theme", None)
    return str(getattr(theme, "type", "dark"))


def _closest_symbol(ticker: str, threshold: float = 0.6) -> str | None:
    constituents = data_access._load_constituents()
    symbols = list(dict.fromkeys(symbol for group in constituents.values() for symbol in group))
    matches = difflib.get_close_matches(ticker.upper(), symbols, n=1, cutoff=threshold)
    return matches[0] if matches else None


def _load_single_stock_data(ticker: str) -> tuple[str, pd.DataFrame]:
    """Load one symbol and use the shared constituent list for fuzzy fallback."""

    with st.spinner(f"Loading data for {ticker}…"):
        frame = data_access.fetch_chart_data(ticker)
    if not frame.empty:
        return ticker, frame

    closest = _closest_symbol(ticker)
    if not closest:
        st.error(f"❌ Symbol **{ticker}** was not found in available stocks.")
        return ticker, pd.DataFrame()
    st.info(f"ℹ️ Symbol **{ticker}** not found. Loading closest match **{closest}**.")
    with st.spinner(f"Loading data for {closest}…"):
        return closest, data_access.fetch_chart_data(closest)


def _render_replay_output(
    replay: dict,
    initial_capital: float,
    ticker: str,
    final_date,
    file_prefix: str,
) -> None:
    """Render the shared single-stock replay and trade ledger."""

    summary = replay["summary"]
    start_date = pd.Timestamp(summary["comparison_start_date"]).date()
    signal_date = pd.Timestamp(summary["first_buy_signal_date"]).date()
    st.caption(f"Common comparison start: **{start_date} open** · first BUY signal: **{signal_date} close**")

    metric_cols = [*st.columns(2), *st.columns(2)]
    metric_cols[0].metric(
        "Pre-tax liquidation value",
        f"₹{summary['pre_tax_liquidation_value']:,.0f}",
        f"{summary['pre_tax_return_pct']:+.1f}%",
    )
    metric_cols[1].metric(
        "Estimated post-tax value",
        f"₹{summary['after_tax_liquidation_value']:,.0f}",
        f"{summary['after_tax_return_pct']:+.1f}%",
    )
    metric_cols[2].metric("Closed trades", f"{summary['closed_trades']}")
    metric_cols[3].metric("Maximum drawdown", f"{summary['max_drawdown_pct']:.1f}%")

    detail_cols = st.columns(4)
    detail_cols[0].metric("Market exposure", f"{summary['exposure_pct']:.1f}%")
    win_rate = summary.get("win_rate_pct")
    detail_cols[1].metric("Closed-trade win rate", "—" if win_rate is None else f"{win_rate:.1f}%")
    detail_cols[2].metric("Buy & hold (pre-tax)", f"{summary['buy_hold_pre_tax_return_pct']:+.1f}%")
    detail_cols[3].metric("Estimated liquidation tax", f"₹{summary['estimated_liquidation_tax']:,.0f}")

    if summary["open_position"]:
        st.info(
            f"The replay still holds {summary['open_shares']:,} shares. Headline pre/post-tax values assume a "
            "hypothetical sale at the latest close; the equity chart taxes realised exits only."
        )
    if summary.get("unexecuted_signal"):
        st.warning(
            f"The final {summary['unexecuted_signal']} signal has no following session and is therefore not executed."
        )

    st.plotly_chart(charts.ha_ema_equity_figure(replay["equity"], float(initial_capital)), width="stretch")
    st.markdown("#### Trade ledger")
    trades = replay["trades"].copy()
    if trades.empty:
        st.info("No executable trades were generated after the common start date.")
    else:
        display_trades = trades.copy()
        for column in ("Entry Signal", "Entry Date", "Exit Signal", "Exit Date"):
            display_trades[column] = pd.to_datetime(display_trades[column], errors="coerce").dt.date
        st.dataframe(
            display_trades,
            hide_index=True,
            width="stretch",
            column_config={
                "Entry Price": st.column_config.NumberColumn(format="₹%.2f"),
                "Exit Price": st.column_config.NumberColumn(format="₹%.2f"),
                "Entry Cost": st.column_config.NumberColumn(format="₹%.2f"),
                "Exit Cost": st.column_config.NumberColumn(format="₹%.2f"),
                "P&L": st.column_config.NumberColumn(format="₹%.2f"),
                "Return %": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )
        st.download_button(
            "📥 Download trade ledger",
            trades.to_csv(index=False).encode("utf-8"),
            file_name=f"{file_prefix}_replay_{ticker}_{pd.Timestamp(final_date).date()}.csv",
            mime="text/csv",
            width="stretch",
        )
    st.caption(
        "Illustrative strategy-only tax estimate. It excludes other investments, surcharge, cess, dividends "
        "and personal circumstances; consult a tax professional before relying on it."
    )


def _replay_config(prefix: str) -> tuple[ReplayConfig, float]:
    with st.expander("Capital, costs and tax assumptions", expanded=True):
        capital_cols = st.columns(3)
        initial_capital = capital_cols[0].number_input(
            "Initial capital (₹)", min_value=10_000, value=100_000, step=10_000, key=f"{prefix}_initial_capital"
        )
        transaction_cost = capital_cols[1].number_input(
            "Trading cost per side (%)",
            min_value=0.0,
            max_value=5.0,
            value=0.10,
            step=0.05,
            key=f"{prefix}_transaction_cost",
        )
        brokerage = capital_cols[2].number_input(
            "Brokerage per order (₹)",
            min_value=0.0,
            max_value=500.0,
            value=15.0,
            step=1.0,
            key=f"{prefix}_brokerage",
        )
        tax_cols = st.columns(3)
        stcg = tax_cols[0].number_input(
            "STCG rate (%)", min_value=0.0, max_value=50.0, value=20.0, step=0.5, key=f"{prefix}_stcg"
        )
        ltcg = tax_cols[1].number_input(
            "LTCG rate (%)", min_value=0.0, max_value=50.0, value=12.5, step=0.5, key=f"{prefix}_ltcg"
        )
        exemption = tax_cols[2].number_input(
            "LTCG exemption used here (₹)",
            min_value=0,
            max_value=1_000_000,
            value=0,
            step=25_000,
            key=f"{prefix}_ltcg_exemption",
            help="Keep at zero to conservatively assume the annual exemption is used by other investments.",
        )
    return (
        ReplayConfig(
            initial_capital=float(initial_capital),
            transaction_cost_pct=float(transaction_cost) / 100.0,
            brokerage_per_order=float(brokerage),
            stcg_rate=float(stcg) / 100.0,
            ltcg_rate=float(ltcg) / 100.0,
            ltcg_exemption_per_fy=float(exemption),
        ),
        float(initial_capital),
    )


def render_ha_ema(ticker: str) -> None:
    """Render weekly Heikin-Ashi plus EMA signals and replay."""

    if not ticker:
        st.markdown('<p class="hero">🕯️ HA No-Wick + EMA Trend</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="sub-hero">Enter an NSE symbol in the sidebar to inspect signals and replay ₹1 lakh.</p>',
            unsafe_allow_html=True,
        )
        return

    ticker, source = _load_single_stock_data(ticker)
    if source.empty:
        return

    with st.expander("Strategy settings", expanded=False):
        ema_cols = st.columns(2)
        fast = ema_cols[0].number_input("Fast EMA", min_value=1, max_value=100, value=10, step=1, key="qpm_ha_ema_fast")
        slow = ema_cols[1].number_input("Slow EMA", min_value=2, max_value=250, value=30, step=1, key="qpm_ha_ema_slow")
        strength_cols = st.columns(2)
        volume_length = strength_cols[0].number_input(
            "Average-volume weeks", min_value=1, max_value=104, value=30, step=1, key="qpm_ha_volume_length"
        )
        minimum_volume = strength_cols[1].number_input(
            "Minimum average volume",
            min_value=0,
            max_value=100_000_000,
            value=100_000,
            step=25_000,
            key="qpm_ha_min_volume",
        )
        return_cols = st.columns(2)
        return_length = return_cols[0].number_input(
            "Return lookback weeks", min_value=1, max_value=260, value=52, step=1, key="qpm_ha_return_length"
        )
        minimum_return = return_cols[1].number_input(
            "Minimum return (%)",
            min_value=0.0,
            max_value=500.0,
            value=50.0,
            step=5.0,
            key="qpm_ha_min_return",
        )
        tolerance = st.number_input(
            "No-wick tolerance (₹)",
            min_value=0.0,
            max_value=10.0,
            value=0.0,
            step=0.01,
            format="%.4f",
            key="qpm_ha_wick_tolerance",
            help="Yahoo does not expose TradingView's minimum tick. Zero uses exact synthetic-wick equality.",
        )

    config = ha_ema_engine.HAEMAStrategyConfig(
        ema_fast=int(fast),
        ema_slow=int(slow),
        average_volume_length=int(volume_length),
        minimum_average_volume=float(minimum_volume),
        return_length=int(return_length),
        minimum_return_pct=float(minimum_return),
        wick_tolerance=float(tolerance),
    )
    try:
        signals = _compute_ha_ema_signals(source, config)
    except ValueError as exc:
        st.error(f"❌ HA+EMA signals cannot be calculated: {exc}")
        return
    if signals.empty:
        st.warning(f"No valid OHLCV observations are available for **{ticker}**.")
        return

    state = ha_ema_engine.latest_ha_ema_state(signals, ticker, config)
    st.info(
        "**Weekly strategy:** HA candles, EMA alignment, volume and return filters use weekly bars. "
        "Completed-week decisions execute at the following week's first market Open."
    )
    chart_tab, replay_tab, rules_tab = st.tabs(["📈 Signal Chart", "💰 ₹1 Lakh Replay", "📖 Rules & Assumptions"])
    with chart_tab:
        status_cols = [*st.columns(2), *st.columns(2)]
        status_cols[0].metric("Current state", state["status"])
        annual_return = state.get("return_pct")
        status_cols[1].metric("1-year return", "—" if annual_return is None else f"{annual_return:.1f}%")
        average_volume = state.get("average_volume")
        status_cols[2].metric("30-week avg volume", "—" if average_volume is None else f"{average_volume:,.0f}")
        last_signal = state.get("last_signal")
        last_signal_text = "None" if not last_signal else f"{last_signal['type']} planned"
        status_cols[3].metric("Latest signal", last_signal_text)

        conditions = [
            {"Entry gate": name, "Status": "✅ Pass" if passed else "❌ Fail"}
            for name, passed in state["conditions"].items()
        ]
        condition_col, summary_col = st.columns([0.42, 0.58])
        with condition_col:
            st.markdown("#### Latest weekly setup")
            st.caption("Only a completed weekly candle can create an order.")
            st.dataframe(pd.DataFrame(conditions), hide_index=True, width="stretch")
        with summary_col:
            st.markdown("#### Current interpretation")
            st.write(build_ha_ema_summary(state))
            st.caption("Rule-based technical description; not investment advice.")

        controls = st.columns([0.72, 0.28])
        candle_style = controls[0].radio(
            "Candle view",
            ["Normal OHLC", "Heikin-Ashi"],
            horizontal=True,
            key="qpm_ha_candle_style",
            help="Display only: strategy signals remain based on the Heikin-Ashi rules.",
        )
        log_scale = controls[1].toggle("Log Y-Axis", value=True, key="qpm_ha_log_scale")
        st.plotly_chart(
            charts.ha_ema_chart_figure(signals, ticker, int(fast), int(slow), log_scale, _theme_type(), candle_style),
            width="stretch",
        )
        st.caption(
            "▲/▼ = following-week executions · hollow circles = weekend decision signals · "
            "green/red = completed gain/loss phase · blue = open phase"
        )

    with replay_tab:
        replay_config, initial_capital = _replay_config("qpm_ha")
        replay = replay_single_stock(signals, replay_config)
        if not replay["summary"].get("comparison_available"):
            reason = replay["summary"].get("reason", "No executable BUY was found.")
            st.info(f"No ₹1 lakh comparison is available. {reason}")
        else:
            _render_replay_output(replay, initial_capital, ticker, signals.index[-1], "ha_ema")

    with rules_tab:
        try:
            st.markdown((_DOCS_DIR / "ha_ema_trend.md").read_text(encoding="utf-8"))
        except OSError:
            st.error("The HA+EMA rules guide could not be loaded.")


def _ichi_key(name: str) -> str:
    return f"{_ICHI_STATE_PREFIX}{name}"


def _set_ichimoku_widgets(config: ichimoku_strategies.IchimokuStrategyConfig) -> None:
    values = {
        "entry_event": config.entry_event,
        "price_location": config.price_location,
        "cross_strength": config.cross_strength,
        "require_tk": config.require_tk_alignment,
        "require_forward": config.require_forward_bullish,
        "chikou": config.chikou_mode,
        "kijun_direction": config.kijun_direction,
        "kijun_lookback": config.kijun_lookback,
        "limit_extension": config.maximum_extension_pct is not None,
        "extension_pct": config.maximum_extension_pct or 10.0,
        "entry_confirmation": config.entry_confirmation,
        "exit_event": config.exit_event,
        "exit_confirmation": config.exit_confirmation,
        "fixed_stop": config.fixed_stop_loss_pct is not None,
        "fixed_stop_pct": config.fixed_stop_loss_pct or 10.0,
        "trailing_stop": config.trailing_stop_pct is not None,
        "trailing_stop_pct": config.trailing_stop_pct or 12.0,
        "maximum_hold": config.maximum_holding_periods is not None,
        "maximum_hold_periods": config.maximum_holding_periods or 100,
    }
    for name, value in values.items():
        st.session_state[_ichi_key(name)] = value


def _initialise_ichimoku_state() -> None:
    if _ichi_key("choice") not in st.session_state:
        st.session_state[_ichi_key("choice")] = "Balanced"
        _set_ichimoku_widgets(ichimoku_strategies.strategy_preset("Balanced"))


def _load_ichimoku_preset() -> None:
    choice = st.session_state.get(_ichi_key("choice"), "Balanced")
    if choice in ichimoku_strategies.PRESET_NAMES:
        st.session_state[_ichi_key("preset_load_guard")] = choice
        _set_ichimoku_widgets(ichimoku_strategies.strategy_preset(choice))


def _mark_ichimoku_custom() -> None:
    st.session_state[_ichi_key("choice")] = "Custom"


def _ichimoku_config_from_state() -> ichimoku_strategies.IchimokuStrategyConfig:
    state = st.session_state
    entry_event = state.get(_ichi_key("entry_event"), ichimoku_strategies.ENTRY_COMPLETE_STRUCTURE)
    if entry_event == ichimoku_strategies.ENTRY_TK_CROSS:
        price_location = state.get(_ichi_key("price_location"), "Inside or above cloud")
        require_tk = True
    else:
        price_location = "Above cloud"
        require_tk = bool(state.get(_ichi_key("require_tk"), True))
    return ichimoku_strategies.IchimokuStrategyConfig(
        name=state.get(_ichi_key("choice"), "Balanced"),
        entry_event=entry_event,
        price_location=price_location,
        cross_strength=state.get(_ichi_key("cross_strength"), "Strong and neutral"),
        require_tk_alignment=require_tk,
        require_forward_bullish=bool(state.get(_ichi_key("require_forward"), True)),
        chikou_mode=state.get(_ichi_key("chikou"), ichimoku_strategies.CHIKOU_HIGH),
        kijun_direction=state.get(_ichi_key("kijun_direction"), ichimoku_strategies.KIJUN_NONE),
        kijun_lookback=int(state.get(_ichi_key("kijun_lookback"), 1)),
        maximum_extension_pct=(
            float(state.get(_ichi_key("extension_pct"), 10.0))
            if state.get(_ichi_key("limit_extension"), False)
            else None
        ),
        entry_confirmation=int(state.get(_ichi_key("entry_confirmation"), 1)),
        exit_event=state.get(_ichi_key("exit_event"), ichimoku_strategies.EXIT_KUMO_ENTRY),
        exit_confirmation=int(state.get(_ichi_key("exit_confirmation"), 1)),
        fixed_stop_loss_pct=(
            float(state.get(_ichi_key("fixed_stop_pct"), 10.0)) if state.get(_ichi_key("fixed_stop"), False) else None
        ),
        trailing_stop_pct=(
            float(state.get(_ichi_key("trailing_stop_pct"), 12.0))
            if state.get(_ichi_key("trailing_stop"), False)
            else None
        ),
        maximum_holding_periods=(
            int(state.get(_ichi_key("maximum_hold_periods"), 100))
            if state.get(_ichi_key("maximum_hold"), False)
            else None
        ),
    )


def _render_ichimoku_controls() -> ichimoku_strategies.IchimokuStrategyConfig:
    guarded = st.session_state.pop(_ichi_key("preset_load_guard"), None)
    if guarded in ichimoku_strategies.PRESET_NAMES:
        st.session_state[_ichi_key("choice")] = guarded
    st.selectbox(
        "Strategy preset",
        [*ichimoku_strategies.PRESET_NAMES, "Custom"],
        key=_ichi_key("choice"),
        on_change=_load_ichimoku_preset,
        help="Changing any resolved rule below switches the strategy to Custom.",
    )
    entry_event = st.selectbox(
        "Entry event",
        [
            ichimoku_strategies.ENTRY_TK_CROSS,
            ichimoku_strategies.ENTRY_KUMO_BREAKOUT,
            ichimoku_strategies.ENTRY_COMPLETE_STRUCTURE,
        ],
        key=_ichi_key("entry_event"),
        on_change=_mark_ichimoku_custom,
    )
    if entry_event == ichimoku_strategies.ENTRY_TK_CROSS:
        entry_cols = st.columns(2)
        entry_cols[0].selectbox(
            "Eligible price location",
            ["Above cloud", "Inside or above cloud", "Any location"],
            key=_ichi_key("price_location"),
            on_change=_mark_ichimoku_custom,
        )
        entry_cols[1].selectbox(
            "Bullish cross strength",
            ["Strong only", "Strong and neutral", "Any bullish cross"],
            key=_ichi_key("cross_strength"),
            on_change=_mark_ichimoku_custom,
        )
    elif entry_event == ichimoku_strategies.ENTRY_KUMO_BREAKOUT:
        st.caption("A breakout requires the previous Close inside/below the Kumo and the current Close above it.")
    else:
        st.caption("Complete structure activates when all selected bullish gates first become valid above the Kumo.")

    qualifier_cols = st.columns(2)
    if entry_event != ichimoku_strategies.ENTRY_TK_CROSS:
        qualifier_cols[0].toggle(
            "Require Tenkan above Kijun", key=_ichi_key("require_tk"), on_change=_mark_ichimoku_custom
        )
    qualifier_cols[1].toggle(
        "Require bullish forward Kumo", key=_ichi_key("require_forward"), on_change=_mark_ichimoku_custom
    )
    detail_cols = st.columns(2)
    detail_cols[0].selectbox(
        "Chikou confirmation",
        [ichimoku_strategies.CHIKOU_NONE, ichimoku_strategies.CHIKOU_CLOSE, ichimoku_strategies.CHIKOU_HIGH],
        key=_ichi_key("chikou"),
        on_change=_mark_ichimoku_custom,
    )
    kijun_direction = detail_cols[1].selectbox(
        "Kijun direction",
        [ichimoku_strategies.KIJUN_NONE, ichimoku_strategies.KIJUN_NON_FALLING, ichimoku_strategies.KIJUN_RISING],
        key=_ichi_key("kijun_direction"),
        on_change=_mark_ichimoku_custom,
    )
    if kijun_direction != ichimoku_strategies.KIJUN_NONE:
        st.number_input(
            "Kijun direction lookback (periods)",
            min_value=1,
            max_value=26,
            step=1,
            key=_ichi_key("kijun_lookback"),
            on_change=_mark_ichimoku_custom,
        )
    extension = st.toggle(
        "Avoid entries extended too far above Kijun",
        key=_ichi_key("limit_extension"),
        on_change=_mark_ichimoku_custom,
    )
    if extension:
        st.number_input(
            "Maximum extension above Kijun (%)",
            min_value=0.1,
            max_value=100.0,
            step=0.5,
            key=_ichi_key("extension_pct"),
            on_change=_mark_ichimoku_custom,
        )
    st.slider(
        "Entry confirmation (consecutive qualifying closes)",
        min_value=1,
        max_value=3,
        key=_ichi_key("entry_confirmation"),
        on_change=_mark_ichimoku_custom,
    )

    st.markdown("#### Exit rules")
    exit_cols = st.columns(2)
    exit_cols[0].selectbox(
        "Primary technical exit",
        [
            ichimoku_strategies.EXIT_TENKAN,
            ichimoku_strategies.EXIT_KIJUN,
            ichimoku_strategies.EXIT_TK_CROSS,
            ichimoku_strategies.EXIT_KUMO_ENTRY,
            ichimoku_strategies.EXIT_KUMO_BOTTOM,
        ],
        key=_ichi_key("exit_event"),
        on_change=_mark_ichimoku_custom,
    )
    exit_cols[1].slider(
        "Exit confirmation (consecutive closes)",
        min_value=1,
        max_value=3,
        key=_ichi_key("exit_confirmation"),
        on_change=_mark_ichimoku_custom,
    )
    with st.expander("Optional safety exits", expanded=False):
        risk_cols = st.columns(3)
        fixed = risk_cols[0].toggle("Fixed stop-loss", key=_ichi_key("fixed_stop"), on_change=_mark_ichimoku_custom)
        trailing = risk_cols[1].toggle("Trailing stop", key=_ichi_key("trailing_stop"), on_change=_mark_ichimoku_custom)
        maximum_hold = risk_cols[2].toggle(
            "Maximum holding period", key=_ichi_key("maximum_hold"), on_change=_mark_ichimoku_custom
        )
        value_cols = st.columns(3)
        if fixed:
            value_cols[0].number_input(
                "Fixed stop (%)",
                min_value=0.1,
                max_value=90.0,
                step=0.5,
                key=_ichi_key("fixed_stop_pct"),
                on_change=_mark_ichimoku_custom,
            )
        if trailing:
            value_cols[1].number_input(
                "Trailing distance (%)",
                min_value=0.1,
                max_value=90.0,
                step=0.5,
                key=_ichi_key("trailing_stop_pct"),
                on_change=_mark_ichimoku_custom,
            )
        if maximum_hold:
            value_cols[2].number_input(
                "Maximum periods",
                min_value=1,
                max_value=1_000,
                step=1,
                key=_ichi_key("maximum_hold_periods"),
                on_change=_mark_ichimoku_custom,
            )
        st.caption("Safety exits use completed closes and execute at the following Open; the first valid exit wins.")
    return _ichimoku_config_from_state()


def _render_ichimoku_chart(
    data: pd.DataFrame,
    signals: pd.DataFrame,
    ticker: str,
    timeframe: str,
    log_scale: bool,
    show_chikou: bool,
    show_crossovers: bool,
    strategy_name: str,
) -> None:
    state = ichimoku_engine.latest_ichimoku_state(data, ticker, timeframe)
    metric_cols = [*st.columns(2), *st.columns(2)]
    price_position = str(state.get("price_position", "unavailable")).title()
    distance = state.get("distance_pct")
    distance_help = f"{float(distance):.1f}% from cloud." if distance is not None else None
    metric_cols[0].metric("Price vs Cloud", price_position, help=distance_help)
    metric_cols[1].metric("TK Alignment", str(state.get("tk_relation", "unavailable")).title())
    latest_cross = state.get("last_cross")
    metric_cols[2].metric("Latest TK Cross", "None" if not latest_cross else str(latest_cross["strength"]).title())
    metric_cols[3].metric("Projected Cloud", str(state.get("projected_cloud", "unavailable")).title())
    st.plotly_chart(
        charts.ichimoku_chart_figure(
            data,
            ticker,
            log_scale,
            show_chikou,
            show_crossovers,
            timeframe,
            theme=_theme_type(),
            strategy_signals=signals,
            strategy_name=strategy_name,
        ),
        width="stretch",
    )
    st.caption(
        "Hollow circles = strategy signals · solid triangles = next-open executions · "
        "cloud and TK markers retain the standard Ichimoku colors"
    )
    with st.container(border=True):
        st.markdown("#### Ichimoku Summary")
        st.write(build_ichimoku_summary(state))
        st.caption("Rule-based technical description; not investment advice.")


def _render_ichimoku_rules(config: ichimoku_strategies.IchimokuStrategyConfig) -> None:
    matched = ichimoku_strategies.matching_preset(config)
    st.markdown(f"### Active strategy: {matched or 'Custom'}")
    if config.name == "Custom" and matched:
        st.info(f"The current Custom settings exactly match the **{matched}** preset.")
    with st.container(border=True):
        st.write(ichimoku_strategies.describe_strategy(config))
    try:
        st.markdown((_DOCS_DIR / "ichimoku_strategy_replay.md").read_text(encoding="utf-8"))
    except OSError:
        st.error("The Ichimoku strategy guide could not be loaded.")


def _render_tutorial() -> None:
    try:
        tutorial = strip_cheat_sheet(strip_title(load_tutorial()))
    except OSError:
        st.error("The Ichimoku tutorial file could not be loaded.")
        return
    for kind, content in tutorial_parts(tutorial):
        if kind == "diagram":
            st.markdown(diagram_svg(content, _theme_type()), unsafe_allow_html=True)
        else:
            st.markdown(content)


def _render_cheat_sheet() -> None:
    try:
        groups = extract_cheat_sheet(load_tutorial())
    except OSError:
        groups = []
    if not groups:
        st.error("The Ichimoku cheat sheet could not be loaded.")
        return
    columns = st.columns(2)
    for index, (heading, entries) in enumerate(groups):
        with columns[index % 2]:
            with st.container(border=True):
                st.markdown(f"#### {heading}")
                st.markdown("\n".join(f"- {entry}" for entry in entries))


def render_enhanced_ichimoku(ticker: str) -> None:
    """Render enhanced Ichimoku signals, comparison, replay and guides."""

    _initialise_ichimoku_state()
    active_config = _ichimoku_config_from_state()
    tabs = st.tabs(["☁️ Chart", "💰 ₹1 Lakh Replay", "⚙️ Strategy Rules", "📖 Tutorial", "⚡ Cheat Sheet"])
    calculated = pd.DataFrame()
    resolved_ticker = ticker
    active_signals = pd.DataFrame()
    timeframe = st.session_state.get("qpm_ichimoku_timeframe", "Daily")

    with tabs[0]:
        if not ticker:
            st.markdown('<p class="hero">☁️ Enhanced Ichimoku</p>', unsafe_allow_html=True)
            st.markdown(
                '<p class="sub-hero">Enter an NSE symbol in the sidebar to load the strategy workspace.</p>',
                unsafe_allow_html=True,
            )
        else:
            controls = [*st.columns(2), *st.columns(2)]
            timeframe = controls[0].selectbox("Timeframe", ["Daily", "Weekly"], key="qpm_ichimoku_timeframe")
            log_scale = controls[1].toggle("Log Y-Axis", value=True, key="qpm_ichimoku_log_scale")
            show_chikou = controls[2].toggle("Show Chikou", value=True, key="qpm_ichimoku_chikou")
            show_crossovers = controls[3].toggle("Show Crosses", value=True, key="qpm_ichimoku_crosses")
            resolved_ticker, source = _load_single_stock_data(ticker)
            if not source.empty:
                try:
                    calculated = _compute_ichimoku(source, timeframe)
                    active_signals = ichimoku_strategies.compute_ichimoku_signals(calculated, active_config)
                except ValueError as exc:
                    st.error(f"❌ Ichimoku analysis cannot be calculated: {exc}")
                if not calculated.empty:
                    _render_ichimoku_chart(
                        calculated,
                        active_signals,
                        resolved_ticker,
                        timeframe,
                        log_scale,
                        show_chikou,
                        show_crossovers,
                        ichimoku_strategies.matching_preset(active_config) or "Custom",
                    )

    with tabs[1]:
        st.markdown("### Configurable long-only Ichimoku replay")
        with st.expander("Entry, exit and risk rules", expanded=True):
            active_config = _render_ichimoku_controls()
        label = ichimoku_strategies.matching_preset(active_config) or "Custom"
        with st.container(border=True):
            st.markdown(f"#### Active rules · {label}")
            st.write(ichimoku_strategies.describe_strategy(active_config))
        if not ticker or calculated.empty:
            st.info("Enter a valid NSE symbol in the sidebar to calculate the replay.")
        else:
            replay_config, initial_capital = _replay_config("qpm_ichimoku")
            comparison, replay_set, common_start = _compute_ichimoku_strategy_comparison(
                calculated, active_config, replay_config
            )
            st.markdown("#### Pre-tax strategy comparison")
            if comparison.empty or common_start is None:
                st.info("The loaded history is not long enough for a common Ichimoku strategy start.")
            else:
                selected_key = (
                    st.session_state.get(_ichi_key("choice"), "Balanced")
                    if st.session_state.get(_ichi_key("choice"), "Balanced") in ichimoku_strategies.PRESET_NAMES
                    else "Custom"
                )
                st.caption(
                    f"Common cash start: **{common_start.date()} close** · all curves begin with "
                    f"₹{initial_capital:,.0f} and stay in cash until their first execution."
                )
                st.plotly_chart(
                    charts.ichimoku_strategy_comparison_figure(comparison, initial_capital, selected_key),
                    width="stretch",
                )
                selected = replay_set[selected_key]
                selected_replay = selected["replay"]
                metadata = ichimoku_strategies.strategy_metadata(active_config)
                if not selected_replay["trades"].empty:
                    selected_replay = {**selected_replay, "trades": selected_replay["trades"].copy()}
                    selected_replay["trades"].insert(0, "Strategy", selected_key)
                    selected_replay["trades"].insert(1, "Strategy Fingerprint", metadata["fingerprint"])
                    selected_replay["trades"].insert(2, "Timeframe", timeframe)
                    reasons = selected["signals"]["Signal_Reason"]
                    selected_replay["trades"]["Entry Reason"] = selected_replay["trades"]["Entry Signal"].map(reasons)
                    selected_replay["trades"]["Exit Reason"] = selected_replay["trades"]["Exit Signal"].map(reasons)
                st.markdown(f"### {selected_key} replay")
                if not selected_replay["summary"].get("comparison_available"):
                    st.info(selected_replay["summary"].get("reason", "No executable BUY was found."))
                else:
                    _render_replay_output(
                        selected_replay,
                        initial_capital,
                        resolved_ticker,
                        selected["signals"].index[-1],
                        f"ichimoku_{selected_key.lower()}",
                    )

    with tabs[2]:
        _render_ichimoku_rules(active_config)
    with tabs[3]:
        _render_tutorial()
    with tabs[4]:
        _render_cheat_sheet()


def render_quant_portfolio_maker(page: str, ticker: str, portfolio_params: dict | None = None) -> None:
    """Dispatch one Quant-Portfolio-Maker page without owning the main app shell."""

    if page not in PAGE_LABELS:
        st.error("Unknown Quant-Portfolio-Maker page.")
        return
    st.caption("🧮 Quant-Portfolio-Maker · strategy research and portfolio construction")
    if page == ENHANCED_ICHIMOKU_PAGE:
        render_enhanced_ichimoku(ticker)
    elif page == HA_EMA_PAGE:
        render_ha_ema(ticker)
    elif page == PORTFOLIO_LAB_PAGE:
        import apps.quant_portfolio_maker.portfolio as portfolio_ui

        if getattr(portfolio_ui, "PORTFOLIO_UI_VERSION", 0) < 7:
            portfolio_ui = importlib.reload(portfolio_ui)

        portfolio_ui.render_portfolio_lab(portfolio_params or {})


def main() -> None:
    """Run Quant-Portfolio-Maker directly while retaining the shared repository modules."""

    st.set_page_config(page_title="Quant-Portfolio-Maker", page_icon="🧮", layout="wide")
    st.markdown(
        """
<style>
.hero { text-align: center; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
.sub-hero { text-align: center; opacity: 0.6; margin-top: -8px; }
</style>
""",
        unsafe_allow_html=True,
    )
    with st.sidebar:
        st.markdown("## 🧮 Quant-Portfolio-Maker")
        page = st.radio("Workspace", PAGE_LABELS, key="qpm_standalone_page")
        portfolio_params = {}
        if page == PORTFOLIO_LAB_PAGE:
            import apps.quant_portfolio_maker.portfolio as portfolio_ui

            if getattr(portfolio_ui, "PORTFOLIO_UI_VERSION", 0) < 7:
                portfolio_ui = importlib.reload(portfolio_ui)

            portfolio_params = portfolio_ui.render_portfolio_sidebar(list(data_access._load_constituents().keys()))
            ticker = ""
        else:
            ticker = st.text_input("NSE Symbol (e.g. RELIANCE)", key="qpm_standalone_ticker").strip().upper()
    render_quant_portfolio_maker(page, ticker, portfolio_params)


if __name__ == "__main__":
    main()
