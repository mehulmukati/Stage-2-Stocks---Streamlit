"""Compact, comparable technical charts and paginated PDF contact sheets."""

from __future__ import annotations

import io
import re
import textwrap
from datetime import datetime
from urllib.parse import quote

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle
from matplotlib.ticker import FuncFormatter

import data
import ha_ema_engine
import ichimoku_engine
from stage2_engine import compute_rolling_stage2, current_stage2_run, score_stage2

ONE_STOCK_PAGE = "🔎 One Stock, Three Views"
MANY_STOCKS_PAGE = "▦ Many Stocks, One View"
ANALYSES = ("Stage 2 Phase", "Ichimoku", "Heikin-Ashi + EMA")
RANGES = ("6 months", "1 year", "2 years", "All available")
_SYMBOL = re.compile(r"^[A-Z0-9&-]{1,20}$")
_FULL_VIEW_SLUGS = {
    "Stage 2 Phase": "phase",
    "Ichimoku": "ichimoku",
    "Heikin-Ashi + EMA": "ha-ema",
}


def _plain_price_tick(value: float, _position: int) -> str:
    """Keep log-axis tick labels in ordinary price notation."""
    return f"{value:,.6f}".rstrip("0").rstrip(".")


def full_view_link(analysis: str, ticker: str) -> str:
    """A same-app URL that opens the full chart with the current symbol."""
    slug = _FULL_VIEW_SLUGS[analysis]
    return f"[Open full {analysis} view](?multi_view={slug}&multi_ticker={quote(ticker, safe='')})"


def parse_tickers(uploaded: bytes | None, pasted: str) -> list[str]:
    """Read a one-column CSV and pasted symbols, keeping first occurrence order."""
    values: list[str] = []
    if uploaded:
        try:
            frame = pd.read_csv(io.BytesIO(uploaded), header=None, dtype=str, encoding="utf-8-sig")
        except (UnicodeError, pd.errors.ParserError, pd.errors.EmptyDataError) as exc:
            raise ValueError("The CSV could not be read. Use a UTF-8, one-column ticker list.") from exc
        if frame.shape[1] != 1:
            raise ValueError("The CSV must have one Symbol or Ticker column.")
        values.extend(frame.iloc[:, 0].dropna().tolist())
    values.extend(re.split(r"[,;\s]+", pasted))
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in values:
        symbol = str(raw).strip().upper()
        if not symbol or symbol in {"SYMBOL", "TICKER"}:
            continue
        if symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols


def _display_start(last: pd.Timestamp, choice: str) -> pd.Timestamp | None:
    months = {"6 months": 6, "1 year": 12, "2 years": 24}.get(choice)
    return last - pd.DateOffset(months=months) if months else None


@st.cache_data(ttl=3600, show_spinner=False)
def analyze(ticker: str, analysis: str, timeframe: str = "Daily", target_session: str | None = None) -> dict:
    """Calculate against full history; the selected visible range is applied later."""
    if not _SYMBOL.fullmatch(ticker):
        return {"ticker": ticker, "analysis": analysis, "error": "Invalid NSE symbol"}
    source = data.fetch_chart_data(ticker)
    if source.empty:
        return {"ticker": ticker, "analysis": analysis, "error": "No price history available"}
    try:
        if analysis == "Stage 2 Phase":
            calculated = compute_rolling_stage2(source)
            if score_stage2(source) is None:
                raise ValueError("At least 250 trading sessions are needed")
            run = current_stage2_run(source)
            score_value = int(calculated["Score"].iloc[-1])
            phase_name = str(calculated["Phase"].iloc[-1])
            summary = f"{phase_name}  ·  {score_value}/8  ·  {run['Stage 2 Days']} days"
            state = {"score": score_value, "phase": phase_name, **run}
        elif analysis == "Ichimoku":
            calculated = ichimoku_engine.compute_ichimoku(source, timeframe=timeframe)
            state = ichimoku_engine.latest_ichimoku_state(calculated, ticker, timeframe)
            if not state["sufficient_data"]:
                raise ValueError("Insufficient history for Ichimoku state")
            cross = state.get("last_cross")
            cross_text = "no cross" if not cross else f"{cross['direction']} cross ({cross['age_sessions']} bars ago)"
            summary = (
                f"Price {state['price_position']} cloud  ·  TK {state['tk_relation']}  ·  "
                f"{cross_text}  ·  forward cloud {state['projected_cloud']}"
            )
        elif analysis == "Heikin-Ashi + EMA":
            # Match the visible defaults on the full HA + EMA page.
            config = ha_ema_engine.HAEMAStrategyConfig(wick_tolerance=0.0)
            calculated = ha_ema_engine.compute_ha_ema_signals(source, config)
            state = ha_ema_engine.latest_ha_ema_state(calculated, ticker, config)
            if not state["sufficient_data"]:
                raise ValueError("At least 52 weeks are needed for the HA + EMA setup")
            last_signal = state.get("last_signal")
            signal_text = (
                "no signal" if not last_signal else f"last {last_signal['type']} {last_signal['age_weeks']} weeks ago"
            )
            summary = f"{state['status']}  ·  {signal_text}"
        else:
            raise ValueError("Unknown analysis")
    except (KeyError, ValueError, TypeError) as exc:
        return {"ticker": ticker, "analysis": analysis, "error": str(exc), "price_date": source.index[-1]}
    return {
        "ticker": ticker,
        "analysis": analysis,
        "timeframe": timeframe,
        "data": calculated,
        "state": state,
        "summary": summary,
        "price_date": pd.Timestamp(source.index[-1]),
    }


def analyze_current(ticker: str, analysis: str, timeframe: str = "Daily") -> dict:
    """Include the completed market session in the analysis cache key."""
    return analyze(ticker, analysis, timeframe, data._get_target_key())


def _draw_candles(ax, frame: pd.DataFrame, prefix: str = "") -> None:
    opens = frame[f"{prefix}Open"].astype(float)
    highs = frame[f"{prefix}High"].astype(float)
    lows = frame[f"{prefix}Low"].astype(float)
    closes = frame[f"{prefix}Close"].astype(float)
    width = 3.0
    for date, open_, high, low, close in zip(frame.index, opens, highs, lows, closes):
        x = mdates.date2num(date)
        color = "#16a34a" if close >= open_ else "#dc2626"
        ax.vlines(x, low, high, color=color, linewidth=0.65)
        ax.add_patch(
            Rectangle(
                (x - width / 2, min(open_, close)),
                width,
                max(abs(close - open_), 0.01),
                facecolor=color,
                edgecolor=color,
                linewidth=0.4,
            )
        )
    ax.xaxis_date()


def draw_tile(ax, record: dict, display_range: str) -> None:
    """The same compact drawing is used in Streamlit and in every PDF cell."""
    ticker = record["ticker"]
    ax.set_facecolor("#f8fafc")
    for spine in ax.spines.values():
        spine.set_color("#cbd5e1")
    ax.grid(axis="y", color="#e2e8f0", linewidth=0.5)
    ax.tick_params(labelsize=7, colors="#475569")
    if "error" in record:
        ax.set_title(ticker, loc="left", fontsize=11, fontweight="bold")
        ax.text(
            0.5,
            0.5,
            record["error"],
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=9,
            color="#b91c1c",
            wrap=True,
        )
        ax.set_xticks([])
        ax.set_yticks([])
        return

    frame = record["data"]
    last = record["price_date"]
    start = _display_start(last, display_range)
    visible = frame.loc[start:] if start is not None else frame
    analysis = record["analysis"]
    if analysis == "Stage 2 Phase":
        visible = visible.dropna(subset=["MA200"])
        colors = {"Strong Stage 2": "#dcfce7", "Likely Stage 2": "#fef9c3", "Early/Weak Stage 2": "#ffedd5"}
        phases = visible["Phase"].astype(str)
        groups = (phases != phases.shift()).cumsum()
        for _, segment in visible.groupby(groups, sort=False):
            color = colors.get(str(segment["Phase"].iloc[0]))
            if color:
                ax.axvspan(segment.index[0], segment.index[-1] + pd.Timedelta(days=1), color=color, linewidth=0)
        for column, color, width in (
            ("Close", "#0f172a", 1.2),
            ("MA50", "#2563eb", 0.8),
            ("MA150", "#9333ea", 0.8),
            ("MA200", "#dc2626", 0.8),
        ):
            ax.plot(visible.index, visible[column], color=color, linewidth=width, label=column)
        ax.legend(loc="upper left", fontsize=6, ncol=4, frameon=False)
    elif analysis == "Ichimoku":
        observed = visible[~visible["IsFuture"].astype(bool)]
        for column, color, width in (("Close", "#0f172a", 1.1), ("Tenkan", "#2563eb", 0.8), ("Kijun", "#dc2626", 0.8)):
            ax.plot(observed.index, observed[column].astype(float), color=color, linewidth=width, label=column)
        cloud = visible.dropna(subset=["Senkou_A", "Senkou_B"])
        if not cloud.empty:
            a, b = cloud["Senkou_A"].astype(float).to_numpy(), cloud["Senkou_B"].astype(float).to_numpy()
            ax.fill_between(cloud.index, a, b, where=a >= b, color="#22c55e", alpha=0.22)
            ax.fill_between(cloud.index, a, b, where=a < b, color="#ef4444", alpha=0.22)
        ax.legend(loc="upper left", fontsize=6, ncol=3, frameon=False)
    else:
        _draw_candles(ax, visible)
        ax.plot(visible.index, visible["EMA_Fast"].astype(float), color="#2563eb", linewidth=0.9, label="EMA 10")
        ax.plot(visible.index, visible["EMA_Slow"].astype(float), color="#9333ea", linewidth=0.9, label="EMA 30")
        for signal, marker, color in (("BUY", "^", "#16a34a"), ("EXIT", "v", "#dc2626")):
            points = visible[visible["Signal"] == signal]
            ax.scatter(
                points.index,
                points["Close"].astype(float),
                marker=marker,
                s=22,
                color=color,
                zorder=4,
                label=f"{signal} decision",
            )
        ax.legend(loc="upper left", fontsize=6, ncol=4, frameon=False)

    ax.set_title(ticker, loc="left", fontsize=11, fontweight="bold", pad=12)
    ax.text(
        1, 1.03, f"Data {last:%d %b %Y}", ha="right", va="bottom", transform=ax.transAxes, fontsize=7, color="#64748b"
    )
    ax.text(
        0,
        -0.12,
        textwrap.fill(record["summary"], width=48),
        transform=ax.transAxes,
        fontsize=7,
        color="#334155",
        va="top",
        clip_on=False,
    )
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=5))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    ax.margins(x=0.02)
    ax.set_yscale("log")
    price_formatter = FuncFormatter(_plain_price_tick)
    ax.yaxis.set_major_formatter(price_formatter)
    ax.yaxis.set_minor_formatter(price_formatter)


def tile_figure(record: dict, display_range: str):
    fig, ax = plt.subplots(figsize=(5.0, 3.1), dpi=120)
    draw_tile(ax, record, display_range)
    fig.subplots_adjust(left=0.09, right=0.97, top=0.88, bottom=0.27)
    return fig


def build_pdf(
    records: list[dict], analysis: str, display_range: str, rows: int, columns: int, timeframe: str = "Daily"
) -> bytes:
    """Export all pages with exactly the selected row and column layout."""
    buffer = io.BytesIO()
    per_page = rows * columns
    page_count = (len(records) + per_page - 1) // per_page
    with PdfPages(buffer) as pdf:
        for page in range(page_count):
            width = max(11.7, columns * 4.6 + 0.8)
            height = max(8.3, rows * 3.3 + 1.1)
            fig, axes = plt.subplots(rows, columns, figsize=(width, height), squeeze=False, dpi=120)
            for cell, ax in enumerate(axes.flat):
                index = page * per_page + cell
                if index < len(records):
                    draw_tile(ax, records[index], display_range)
                else:
                    ax.axis("off")
            label = f"{analysis} ({timeframe})" if analysis == "Ichimoku" else analysis
            fig.suptitle(
                f"Multi Charts  |  {label}  |  {display_range}", x=0.04, ha="left", fontsize=15, fontweight="bold"
            )
            fig.text(
                0.04,
                0.025,
                f"Generated {datetime.now():%d %b %Y %H:%M}  |  Page {page + 1} of {page_count}",
                fontsize=8,
                color="#64748b",
            )
            fig.subplots_adjust(left=0.05, right=0.98, top=0.91, bottom=0.09, wspace=0.22, hspace=0.58)
            pdf.savefig(fig)
            plt.close(fig)
    return buffer.getvalue()


def _show_tile(record: dict, display_range: str) -> None:
    with st.container(border=True):
        fig = tile_figure(record, display_range)
        st.pyplot(fig, clear_figure=False, width="stretch")
        plt.close(fig)


def render_one_stock(ticker: str) -> None:
    st.header("One Stock, Three Views")
    if not ticker:
        st.info("Enter an NSE symbol in the sidebar to compare the three analyses.")
        return
    shared = st.toggle("Use one display range for all three charts", value=True, key="multi_shared_range")
    if shared:
        selected = st.selectbox("Display range", RANGES, index=1, key="multi_shared_choice")
        ranges = dict.fromkeys(ANALYSES, selected)
    else:
        controls = st.columns(3)
        ranges = {
            name: controls[i].selectbox(f"{name} range", RANGES, index=1, key=f"multi_range_{i}")
            for i, name in enumerate(ANALYSES)
        }
    timeframe = st.selectbox("Ichimoku timeframe", ("Daily", "Weekly"), key="multi_one_ichi_timeframe")
    records = [analyze_current(ticker, name, timeframe if name == "Ichimoku" else "Daily") for name in ANALYSES]
    for pair in ((0, 1), (2, 3)):
        columns = st.columns(2)
        for column, index in zip(columns, pair):
            with column:
                if index == 3:
                    st.subheader("Analysis Snapshot")
                    with st.container(border=True):
                        for name, record in zip(ANALYSES, records):
                            st.markdown(f"**{name}**")
                            st.caption(record.get("summary", record.get("error", "Unavailable")))
                    continue
                name, record = ANALYSES[index], records[index]
                st.subheader(name)
                _show_tile(record, ranges[name])
                st.markdown(full_view_link(name, ticker))
    st.caption(
        "HA + EMA uses the full page's default weekly strategy and normal OHLC candle view. "
        "Calculations use full available history; range controls only change what is drawn."
    )


def _page_items(current: int, total: int) -> list[int | None]:
    """Visible page buttons, with gaps represented by non-clickable ellipses."""
    if total <= 7:
        return list(range(1, total + 1))
    shown = {1, total, *range(max(1, current - 1), min(total, current + 1) + 1)}
    if current <= 3:
        shown.update(range(1, 5))
    if current >= total - 2:
        shown.update(range(total - 3, total + 1))
    items: list[int | None] = []
    previous = 0
    for number in sorted(shown):
        if previous and number - previous > 1:
            items.append(None)
        items.append(number)
        previous = number
    return items


def _current_page(total: int) -> int:
    """Keep the selected page valid when the ticker list or grid size changes."""
    current = int(st.session_state.get("multi_page", 1))
    if current < 1 or current > total:
        current = 1
        st.session_state["multi_page"] = current
    return current


def _page_navigation(current: int, total: int) -> None:
    """Render a compact pagination bar below the chart grid."""
    with st.container(border=True, width="content", horizontal=True, wrap=False, gap="small"):
        if st.button("‹ Previous", key="multi_page_previous", disabled=current == 1, type="tertiary"):
            st.session_state["multi_page"] = current - 1
            st.rerun()
        for item in _page_items(current, total):
            if item is None:
                st.markdown("…")
            elif st.button(
                str(item), key=f"multi_page_number_{item}", type="primary" if item == current else "tertiary"
            ):
                st.session_state["multi_page"] = item
                st.rerun()
        if st.button("Next ›", key="multi_page_next", disabled=current == total, type="tertiary"):
            st.session_state["multi_page"] = current + 1
            st.rerun()


def render_many_stocks() -> None:
    st.header("Many Stocks, One View")
    analysis = st.selectbox("Analysis", ANALYSES, key="multi_many_analysis")
    uploaded = st.file_uploader("Upload one-column CSV (Symbol or Ticker)", type="csv", key="multi_csv")
    pasted = st.text_area("Or paste NSE symbols", placeholder="RELIANCE, HDFCBANK, TCS", key="multi_paste")
    controls = st.columns(4)
    rows = int(controls[0].number_input("Rows per page", min_value=1, max_value=4, value=2, key="multi_rows"))
    columns = int(controls[1].number_input("Columns per page", min_value=1, max_value=3, value=2, key="multi_cols"))
    display_range = controls[2].selectbox("Display range", RANGES, index=1, key="multi_many_range")
    timeframe = controls[3].selectbox(
        "Ichimoku timeframe", ("Daily", "Weekly"), key="multi_many_timeframe", disabled=analysis != "Ichimoku"
    )
    try:
        symbols = parse_tickers(uploaded.getvalue() if uploaded else None, pasted)
    except ValueError as exc:
        st.error(str(exc))
        return
    if not symbols:
        st.info("Upload a ticker CSV or paste symbols to build the chart grid.")
        return
    if columns == 3 and rows >= 3:
        st.warning("Dense grids may be hard to read. The PDF will retain these exact dimensions.")
    invalid = [symbol for symbol in symbols if not _SYMBOL.fullmatch(symbol)]
    if invalid:
        st.warning(f"Invalid symbols will appear as error tiles: {', '.join(invalid[:8])}")
    per_page = rows * columns
    pages = (len(symbols) + per_page - 1) // per_page
    page = _current_page(pages)
    visible = symbols[(page - 1) * per_page : page * per_page]
    for offset in range(0, len(visible), columns):
        cols = st.columns(columns)
        for column, symbol in zip(cols, visible[offset : offset + columns]):
            with column:
                record = analyze_current(symbol, analysis, timeframe)
                _show_tile(record, display_range)
    st.caption(f"{len(symbols)} tickers in input order  ·  Page {page} of {pages}")
    _page_navigation(page, pages)
    request_key = (tuple(symbols), analysis, timeframe, display_range, rows, columns)
    if st.session_state.get("multi_pdf_key") != request_key:
        st.session_state.pop("multi_pdf_bytes", None)
    if st.button("Prepare PDF of all pages", type="primary", key="multi_prepare_pdf"):
        progress = st.progress(0, text="Preparing all chart pages")
        records = []
        for index, symbol in enumerate(symbols, 1):
            records.append(analyze_current(symbol, analysis, timeframe))
            progress.progress(index / len(symbols), text=f"Analyzing {index} of {len(symbols)} tickers")
        try:
            st.session_state["multi_pdf_bytes"] = build_pdf(
                records, analysis, display_range, rows, columns, timeframe=timeframe
            )
            st.session_state["multi_pdf_key"] = request_key
        except Exception as exc:
            st.error(f"PDF generation failed: {exc}")
        progress.empty()
    if st.session_state.get("multi_pdf_bytes") and st.session_state.get("multi_pdf_key") == request_key:
        slug = re.sub(r"[^a-z0-9]+", "_", analysis.lower()).strip("_")
        st.download_button(
            "Download all-page PDF",
            data=st.session_state["multi_pdf_bytes"],
            file_name=f"multi_charts_{slug}.pdf",
            mime="application/pdf",
            key="multi_download_pdf",
        )
