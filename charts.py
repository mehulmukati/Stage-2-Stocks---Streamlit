import math

import pandas as pd
import plotly.colors
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from strategy_replay import resolve_signal_executions

ICHIMOKU_CHART_VERSION = 10
HA_EMA_CHART_VERSION = 5
STAGE2_BREADTH_CHART_VERSION = 1
PHASE_CHART_VERSION = 1

PHASE_COLORS = {
    "Strong Stage 2": "rgba(34, 197, 94, 0.25)",
    "Likely Stage 2": "rgba(234, 179, 8, 0.25)",
    "Early/Weak Stage 2": "rgba(249, 115, 22, 0.22)",
}

STAGE2_BREADTH_COLORS = {
    "Strong": "#22c55e",
    "Likely": "#eab308",
    "Early": "#f97316",
    "Not Stage 2": "#94a3b8",
}


def _add_breadth_index_overlays(fig: go.Figure, index_series: dict[str, pd.Series] | None) -> None:
    """Overlay local benchmark closes on a log-scaled right axis."""
    if not index_series:
        return
    colors = {"Nifty 50": "#2563eb", "Nifty 100": "#a855f7", "Nifty 500": "#ec4899"}
    for name, series in index_series.items():
        observed = series.dropna()
        if observed.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=observed.index,
                y=observed,
                name=name,
                mode="lines",
                yaxis="y2",
                line={"width": 1.6, "color": colors.get(name, "#0ea5e9")},
                hovertemplate=f"<b>{name}</b><br>%{{x|%d %b %Y}}<br>%{{y:,.2f}}<extra></extra>",
            )
        )
    fig.update_layout(
        yaxis2={"title": "Index level (log)", "type": "log", "overlaying": "y", "side": "right", "showgrid": False}
    )


def stage2_breadth_count_figure(daily: pd.DataFrame, index_series: dict[str, pd.Series] | None = None) -> go.Figure:
    """Stacked daily count view of Stage 2 breadth."""
    fig = go.Figure()
    for column in ("Strong", "Likely", "Early", "Not Stage 2"):
        fig.add_trace(
            go.Scatter(
                x=daily["date"],
                y=daily[column],
                name=column,
                stackgroup="breadth",
                mode="lines",
                line={"width": 0.6, "color": STAGE2_BREADTH_COLORS[column]},
                fillcolor=STAGE2_BREADTH_COLORS[column],
                customdata=daily[["Eligible", "Stage 2", "Stage 2 %", "Strong %"]],
                hovertemplate=(
                    "<b>%{x|%d %b %Y}</b><br>" + column + ": %{y:,}<br>Eligible: %{customdata[0]:,}<br>"
                    "Stage 2: %{customdata[1]:,} (%{customdata[2]:.1f}%)<br>"
                    "Strong: %{customdata[3]:.1f}%<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        title="Daily Stage 2 Breadth — Eligible Stock Count",
        yaxis_title="Stocks",
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.08},
        margin={"l": 30, "r": 20, "t": 60, "b": 25},
    )
    _add_breadth_index_overlays(fig, index_series)
    return fig


def stage2_breadth_percent_figure(daily: pd.DataFrame, index_series: dict[str, pd.Series] | None = None) -> go.Figure:
    """100%-stacked companion view, insensitive to changes in coverage size."""
    fig = go.Figure()
    eligible = daily["Eligible"].replace(0, pd.NA)
    for column in ("Strong", "Likely", "Early", "Not Stage 2"):
        fig.add_trace(
            go.Scatter(
                x=daily["date"],
                y=daily[column].div(eligible).fillna(0).mul(100),
                name=column,
                stackgroup="breadth_pct",
                groupnorm="percent",
                mode="lines",
                line={"width": 0.6, "color": STAGE2_BREADTH_COLORS[column]},
                fillcolor=STAGE2_BREADTH_COLORS[column],
                customdata=daily[["Eligible", "Stage 2 %", "Average Score"]],
                hovertemplate=(
                    "<b>%{x|%d %b %Y}</b><br>" + column + ": %{y:.1f}%<br>Eligible: %{customdata[0]:,}<br>"
                    "Stage 2 breadth: %{customdata[1]:.1f}%<br>Average score: %{customdata[2]:.2f}/8<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        title="Daily Stage 2 Breadth — Percentage of Eligible Stocks",
        yaxis={"title": "Universe share", "ticksuffix": "%", "range": [0, 100]},
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.08},
        margin={"l": 30, "r": 20, "t": 60, "b": 25},
    )
    _add_breadth_index_overlays(fig, index_series)
    return fig


# Strategy colors — mid-range saturation so they read on both light and dark backgrounds.
# Line dash encodes rebalance method; color encodes band rule.
BT_COLORS = {
    "Classic · Full": "#3b82f6",  # blue-500
    "Classic · Marginal": "#a78bfa",  # violet-400
    "Classic · Prop": "#2dd4bf",  # teal-400
    "Displacement · Full": "#f59e0b",  # amber-500
    "Displacement · Marginal": "#34d399",  # emerald-400
    "Displacement · Prop": "#f472b6",  # pink-400
    "NIFTY50": "#f87171",  # red-400
    "NIFTY500": "#fb923c",  # orange-400
    # legacy keys (pre-rename format)
    "Full Rebalance": "#3b82f6",
    "Marginal Rebalance": "#a78bfa",
    "Prop Rebalance": "#2dd4bf",
}


def _bt_line(col: str) -> dict:
    """Return line style dict for a backtest series column."""
    name = col.lower()
    if "prop" in name:
        return dict(color=BT_COLORS.get(col, "#94a3b8"), width=2, dash="dashdot")
    if "marginal" in name:
        return dict(color=BT_COLORS.get(col, "#94a3b8"), width=2, dash="dash")
    if "nifty" in name or "benchmark" in name:
        return dict(color=BT_COLORS.get(col, "#94a3b8"), width=1.5, dash="dot")
    return dict(color=BT_COLORS.get(col, "#94a3b8"), width=2.5, dash="solid")


_T = "rgba(0,0,0,0)"
_GRID = "rgba(128,128,128,0.2)"

ICHIMOKU_COLORS = {
    "tenkan": "#06b6d4",
    "kijun": "#d97706",
    "span_a": "#277a55",
    "span_b": "#713747",
    "chikou": "#e5cf35",
    "bullish_cloud": "rgba(39, 122, 85, 0.27)",
    "bearish_cloud": "rgba(113, 55, 71, 0.28)",
    "bullish_cross": "#4ade80",
    "bearish_cross": "#e879f9",
    "buy_signal": "#86efac",
    "exit_signal": "#fca5a5",
    "buy_execution": "#16a34a",
    "exit_execution": "#dc2626",
}

ICHIMOKU_STRATEGY_COLORS = {
    "Aggressive": "#f97316",
    "Balanced": "#2563eb",
    "Conservative": "#16a34a",
    "Custom": "#a855f7",
}

_ICHI_BG = "#0f1420"
_ICHI_GRID = "rgba(148, 163, 184, 0.10)"
_ICHI_TEXT = "#8b93a7"

_ICHIMOKU_THEMES = {
    "dark": {
        "background": _ICHI_BG,
        "grid": _ICHI_GRID,
        "text": _ICHI_TEXT,
        "title": "#d7dce5",
        "spike": "#667085",
        "hover_background": "#1a2030",
        "hover_border": "#3b4355",
        "hover_text": "#e5e7eb",
        "increasing_line": "#f8fafc",
        "increasing_fill": "#f8fafc",
        "marker_outline": _ICHI_BG,
        "chikou": ICHIMOKU_COLORS["chikou"],
        "bullish_cross": ICHIMOKU_COLORS["bullish_cross"],
        "bearish_cross": ICHIMOKU_COLORS["bearish_cross"],
    },
    "light": {
        "background": "#ffffff",
        "grid": "rgba(15, 23, 42, 0.10)",
        "text": "#475569",
        "title": "#0f172a",
        "spike": "#94a3b8",
        "hover_background": "#ffffff",
        "hover_border": "#cbd5e1",
        "hover_text": "#0f172a",
        "increasing_line": "#0f766e",
        "increasing_fill": "#ccfbf1",
        "marker_outline": "#ffffff",
        "chikou": "#a16207",
        "bullish_cross": "#16a34a",
        "bearish_cross": "#c026d3",
    },
}

HA_EMA_COLORS = {
    "fast_ema": "#2563eb",
    "slow_ema": "#f59e0b",
    "actual_close": "#94a3b8",
    "buy": "#16a34a",
    "exit": "#dc2626",
    "buy_signal": "#86efac",
    "exit_signal": "#fca5a5",
    "winning_phase": "rgba(34, 197, 94, 0.13)",
    "losing_phase": "rgba(239, 68, 68, 0.11)",
    "open_phase": "rgba(14, 165, 233, 0.11)",
    "pre_tax": "#2563eb",
    "post_tax": "#7c3aed",
    "buy_hold": "#f59e0b",
}


def _cloud_fill_segments(data) -> list[tuple[str, list, list[float], list[float]]]:
    """Split a cloud at interpolated Span A/B twists so fill colors meet cleanly."""
    valid = data.dropna(subset=["Senkou_A", "Senkou_B"])
    if valid.empty:
        return []

    points = [
        (idx, float(row["Senkou_A"]), float(row["Senkou_B"])) for idx, row in valid[["Senkou_A", "Senkou_B"]].iterrows()
    ]
    first_sign = 1 if points[0][1] >= points[0][2] else -1
    current_sign = first_sign
    current = ([points[0][0]], [points[0][1]], [points[0][2]])
    segments: list[tuple[str, list, list[float], list[float]]] = []

    for previous, point in zip(points, points[1:]):
        sign = 1 if point[1] >= point[2] else -1
        if sign == current_sign:
            current[0].append(point[0])
            current[1].append(point[1])
            current[2].append(point[2])
            continue

        previous_difference = previous[1] - previous[2]
        current_difference = point[1] - point[2]
        denominator = abs(previous_difference) + abs(current_difference)
        fraction = abs(previous_difference) / denominator if denominator else 0.5
        twist_date = previous[0] + (point[0] - previous[0]) * fraction
        twist_a = previous[1] + (point[1] - previous[1]) * fraction
        twist_b = previous[2] + (point[2] - previous[2]) * fraction
        twist_value = (twist_a + twist_b) / 2.0

        current[0].append(twist_date)
        current[1].append(twist_value)
        current[2].append(twist_value)
        regime = "bullish" if current_sign > 0 else "bearish"
        segments.append((regime, *current))

        current = ([twist_date, point[0]], [twist_value, point[1]], [twist_value, point[2]])
        current_sign = sign

    regime = "bullish" if current_sign > 0 else "bearish"
    segments.append((regime, *current))
    return segments


def ichimoku_chart_figure(
    data,
    ticker: str,
    use_log_scale: bool = True,
    show_chikou: bool = True,
    show_crossovers: bool = True,
    timeframe: str = "Daily",
    theme: str = "dark",
    strategy_signals=None,
    strategy_name: str | None = None,
) -> go.Figure:
    """Build a candlestick Ichimoku chart with regime-colored cloud segments."""
    fig = go.Figure()
    if data.empty:
        return fig

    chart_theme = _ICHIMOKU_THEMES["light" if theme.strip().lower() == "light" else "dark"]

    observed = data[(~data["IsFuture"].astype(bool)) & data["Close"].notna()]
    latest = observed.iloc[-1]
    previous_close = float(observed["Close"].iloc[-2]) if len(observed) > 1 else float(latest["Close"])
    change = float(latest["Close"]) - previous_close
    interval_code = "1W" if timeframe.strip().lower() == "weekly" else "1D"
    visible_bars = 80 if interval_code == "1W" else 260
    default_start = observed.index[max(0, len(observed) - visible_bars)]

    for regime, dates, span_a, span_b in _cloud_fill_segments(data):
        color = ICHIMOKU_COLORS[f"{regime}_cloud"]
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=span_a,
                mode="lines",
                line=dict(width=0),
                hoverinfo="skip",
                showlegend=False,
                legendgroup=f"{regime}_cloud",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=span_b,
                name=f"{regime.title()} cloud",
                mode="lines",
                line=dict(width=0),
                fill="tonexty",
                fillcolor=color,
                hoverinfo="skip",
                showlegend=False,
                legendgroup=f"{regime}_cloud",
            )
        )

    fig.add_trace(
        go.Candlestick(
            x=observed.index,
            open=observed["Open"],
            high=observed["High"],
            low=observed["Low"],
            close=observed["Close"],
            name=ticker,
            increasing_line_color=chart_theme["increasing_line"],
            increasing_fillcolor=chart_theme["increasing_fill"],
            decreasing_line_color="#c026d3",
            decreasing_fillcolor="#a21caf",
            whiskerwidth=0.35,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=observed.index,
            y=observed["Tenkan"],
            name="Tenkan (9)",
            line=dict(color=ICHIMOKU_COLORS["tenkan"], width=1.25),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=observed.index,
            y=observed["Kijun"],
            name="Kijun (26)",
            line=dict(color=ICHIMOKU_COLORS["kijun"], width=1.25),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data["Senkou_A"],
            name="Senkou A",
            line=dict(color=ICHIMOKU_COLORS["span_a"], width=1.0),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data["Senkou_B"],
            name="Senkou B",
            line=dict(color=ICHIMOKU_COLORS["span_b"], width=1.0),
        )
    )
    if show_chikou:
        fig.add_trace(
            go.Scatter(
                x=observed.index,
                y=observed["Chikou"],
                name="Chikou (−26)",
                line=dict(color=chart_theme["chikou"], width=1.2),
                opacity=0.9,
            )
        )

    if show_crossovers:
        for direction, symbol, color, multiplier in (
            ("bullish", "triangle-up", chart_theme["bullish_cross"], 0.985),
            ("bearish", "triangle-down", chart_theme["bearish_cross"], 1.015),
        ):
            crosses = observed[observed["TK_Cross"] == direction]
            anchor = crosses["Low"] if direction == "bullish" else crosses["High"]
            fig.add_trace(
                go.Scatter(
                    x=crosses.index,
                    y=anchor * multiplier,
                    name=f"{direction.title()} TK cross",
                    mode="markers",
                    marker=dict(
                        symbol=symbol,
                        size=9,
                        color=color,
                        line=dict(color=chart_theme["marker_outline"], width=1.0),
                    ),
                    customdata=list(zip(crosses["Cross_Strength"], crosses["Close"])),
                    hovertemplate=(
                        f"<b>{direction.title()} TK cross</b><br>"
                        "%{x|%d %b %Y}<br>%{customdata[0]}<br>Close: %{customdata[1]:.2f}<extra></extra>"
                    ),
                )
            )

    if strategy_signals is not None and not strategy_signals.empty and "Signal" in strategy_signals:
        strategy_label = strategy_name or "Strategy"
        for action, symbol, color, anchor_column, multiplier in (
            ("BUY", "circle-open", ICHIMOKU_COLORS["buy_signal"], "Low", 0.975),
            ("EXIT", "circle-open", ICHIMOKU_COLORS["exit_signal"], "High", 1.025),
        ):
            events = strategy_signals[strategy_signals["Signal"].eq(action)]
            fig.add_trace(
                go.Scatter(
                    x=events.index,
                    y=events[anchor_column] * multiplier,
                    name=f"{strategy_label} {action} signal",
                    mode="markers",
                    marker=dict(symbol=symbol, size=11, color=color, line=dict(color=color, width=2)),
                    customdata=events.get("Signal_Reason"),
                    hovertemplate=(
                        f"<b>{action} signal · {strategy_label}</b><br>" "%{x|%d %b %Y}<br>%{customdata}<extra></extra>"
                    ),
                )
            )

            executions = resolve_signal_executions(strategy_signals)
            executions = executions[executions["Signal"].eq(action)] if not executions.empty else executions
            fig.add_trace(
                go.Scatter(
                    x=executions.get("Execution_Date", []),
                    y=executions.get("Execution_Price", []),
                    name=f"{strategy_label} {action} execution",
                    mode="markers",
                    marker=dict(
                        symbol="triangle-up" if action == "BUY" else "triangle-down",
                        size=11,
                        color=(
                            ICHIMOKU_COLORS["buy_execution"] if action == "BUY" else ICHIMOKU_COLORS["exit_execution"]
                        ),
                        line=dict(color=chart_theme["marker_outline"], width=1),
                    ),
                    hovertemplate=(
                        f"<b>{action} at next Open · {strategy_label}</b><br>"
                        "%{x|%d %b %Y}<br>Open: ₹%{y:,.2f}<extra></extra>"
                    ),
                )
            )

    fig.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            name="Bullish cloud",
            mode="lines",
            line=dict(color=ICHIMOKU_COLORS["bullish_cloud"], width=8),
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            name="Bearish cloud",
            mode="lines",
            line=dict(color=ICHIMOKU_COLORS["bearish_cloud"], width=8),
            hoverinfo="skip",
        )
    )

    latest_close = float(latest["Close"])
    price_color = "#26a69a" if change >= 0 else "#a21caf"
    # Plotly annotations use log-axis coordinates rather than raw data values,
    # while shapes still use raw values.  Treating both overlay types alike
    # either turns a price such as 308 into 10^308 or places the horizontal
    # price line near log10(308), stretching the visible axis toward zero.

    def overlay_y(value: float) -> float:
        return math.log10(value) if use_log_scale else value

    fig.add_hline(
        y=latest_close,
        line_color=price_color,
        line_width=0.8,
        line_dash="dot",
        opacity=0.75,
    )
    for label, value in (("High", float(observed["High"].max())), ("Low", float(observed["Low"].min()))):
        fig.add_annotation(
            x=1.0,
            xref="paper",
            xanchor="left",
            y=overlay_y(value),
            yref="y",
            text=f"<b>{label}&nbsp;&nbsp; {value:,.1f}</b>",
            showarrow=False,
            font=dict(color="#e5e7eb", size=11),
            bgcolor="#173b73",
            borderpad=3,
        )
    fig.add_annotation(
        x=1.0,
        xref="paper",
        xanchor="left",
        y=overlay_y(latest_close),
        yref="y",
        text=f"<b>{latest_close:,.1f}</b>",
        showarrow=False,
        font=dict(color="white", size=11),
        bgcolor=price_color,
        borderpad=3,
    )

    fig.update_layout(
        title=dict(
            text=f"{ticker} — {timeframe.title()} Ichimoku Cloud (9, 26, 52)",
            x=0.01,
            xanchor="left",
            y=0.98,
            yanchor="top",
            font=dict(size=14, color=chart_theme["title"]),
        ),
        yaxis=dict(
            type="log" if use_log_scale else "linear",
            showgrid=True,
            gridcolor=chart_theme["grid"],
            zeroline=False,
            side="right",
            tickfont=dict(color=chart_theme["text"], size=11),
            title=None,
            showspikes=True,
            spikecolor=chart_theme["spike"],
            spikedash="dot",
            spikethickness=1,
        ),
        xaxis=dict(
            showgrid=False,
            range=[default_start, data.index[-1]],
            rangeslider=dict(visible=False),
            rangebreaks=[dict(bounds=["sat", "mon"])],
            tickfont=dict(color=chart_theme["text"], size=11),
            showspikes=True,
            spikecolor=chart_theme["spike"],
            spikedash="dot",
            spikethickness=1,
            spikesnap="cursor",
        ),
        height=640,
        margin=dict(l=14, r=86, t=58, b=72),
        legend=dict(
            orientation="h",
            x=0.01,
            xanchor="left",
            y=-0.12,
            yanchor="top",
            font=dict(color=chart_theme["text"], size=10),
            bgcolor="rgba(0,0,0,0)",
            itemclick="toggle",
            itemdoubleclick="toggleothers",
        ),
        hovermode="x",
        hoverlabel=dict(
            bgcolor=chart_theme["hover_background"],
            bordercolor=chart_theme["hover_border"],
            font=dict(color=chart_theme["hover_text"]),
        ),
        plot_bgcolor=chart_theme["background"],
        paper_bgcolor=chart_theme["background"],
        font=dict(color=chart_theme["text"]),
    )
    return fig


def phase_chart_figure(rolled, ticker: str, use_log_scale: bool = True) -> go.Figure:
    valid = rolled.dropna(subset=["MA200"])
    fig = go.Figure()

    if not valid.empty:
        phase_str = valid["Phase"].astype(str)
        seg_id = (phase_str != phase_str.shift()).cumsum()
        for _, grp in valid.groupby(seg_id, sort=False):
            phase = str(grp["Phase"].iloc[0])
            color = PHASE_COLORS.get(phase)
            if color is None:
                continue
            fig.add_vrect(x0=grp.index[0], x1=grp.index[-1], fillcolor=color, layer="below", line_width=0)
            midpoint = grp.index[0] + (grp.index[-1] - grp.index[0]) / 2
            short_label = {
                "Strong Stage 2": "Strong",
                "Likely Stage 2": "Likely",
                "Early/Weak Stage 2": "Early/Weak",
            }[phase]
            fig.add_annotation(
                x=midpoint,
                y=0.96,
                xref="x",
                yref="paper",
                xanchor="center",
                yanchor="top",
                text=f"<b>{short_label}</b><br>{len(grp)}d",
                showarrow=False,
                align="center",
                font=dict(size=10, color="#f8fafc"),
                bgcolor="rgba(15, 23, 42, 0.65)",
                bordercolor="rgba(226, 232, 240, 0.35)",
                borderwidth=1,
                borderpad=3,
            )

    fig.add_trace(
        go.Scatter(
            x=rolled.index, y=rolled["MA50"], name="MA50", line=dict(color="#3b82f6", width=1, dash="dot"), opacity=0.8
        )
    )
    fig.add_trace(
        go.Scatter(
            x=rolled.index,
            y=rolled["MA150"],
            name="MA150",
            line=dict(color="#a855f7", width=1, dash="dot"),
            opacity=0.8,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=rolled.index,
            y=rolled["MA200"],
            name="MA200",
            line=dict(color="#ef4444", width=1, dash="dot"),
            opacity=0.8,
        )
    )
    fig.add_trace(go.Scatter(x=rolled.index, y=rolled["Close"], name=ticker, line=dict(color="#38bdf8", width=2)))
    fig.add_trace(
        go.Scatter(
            x=valid.index,
            y=valid["Score"],
            name="Stage 2 Score (out of 8)",
            mode="lines",
            line=dict(color="#ec4899", width=2),
            yaxis="y2",
            hovertemplate="%{y:.0f} / 8 indicators satisfied<extra>Stage 2 Score</extra>",
        )
    )

    fig.update_layout(
        title=dict(text=f"{ticker} — Stage 2 Phase Map", font=dict(size=16)),
        yaxis=dict(
            type="log" if use_log_scale else "linear",
            showgrid=True,
            gridcolor=_GRID,
            title="Price (log)" if use_log_scale else "Price",
        ),
        yaxis2=dict(
            title="Stage 2 Score (out of 8)",
            overlaying="y",
            side="right",
            type="linear",
            range=[0, 8],
            tick0=0,
            dtick=1,
            showgrid=False,
            zeroline=False,
            fixedrange=True,
        ),
        xaxis=dict(showgrid=False),
        height=540,
        margin=dict(l=50, r=70, t=55, b=40),
        legend=dict(orientation="h", y=-0.13),
        hovermode="x unified",
        plot_bgcolor=_T,
        paper_bgcolor=_T,
    )
    return fig


def _ha_ema_executions(data) -> tuple[list[dict], list[dict]]:
    """Map close-confirmed signals to the following session's opening execution."""

    executions: list[dict] = []
    phases: list[dict] = []
    open_entry: dict | None = None
    resolved = resolve_signal_executions(data)
    for _, row in resolved.iterrows():
        signal = str(row["Signal"])
        signal_date = pd.Timestamp(row["Signal_Date"])
        event = {
            "type": signal,
            "signal_date": signal_date,
            "signal_close": float(data.loc[signal_date, "Close"]),
            "execution_date": pd.Timestamp(row["Execution_Date"]),
            "execution_price": float(row["Execution_Price"]),
            "trade_return_pct": None,
        }
        if signal == "BUY":
            open_entry = event
        elif open_entry is not None:
            trade_return = (event["execution_price"] / open_entry["execution_price"] - 1.0) * 100.0
            event["trade_return_pct"] = trade_return
            phases.append(
                {
                    "start": open_entry["execution_date"],
                    "end": event["execution_date"],
                    "outcome": "winning" if trade_return >= 0 else "losing",
                    "return_pct": trade_return,
                }
            )
            open_entry = None
        executions.append(event)

    if open_entry is not None:
        phases.append(
            {
                "start": open_entry["execution_date"],
                "end": data.index[-1],
                "outcome": "open",
                "return_pct": (float(data["Close"].iloc[-1]) / open_entry["execution_price"] - 1.0) * 100.0,
            }
        )
    return executions, phases


def ha_ema_chart_figure(
    data,
    ticker: str,
    fast_length: int = 10,
    slow_length: int = 30,
    use_log_scale: bool = True,
    theme: str = "dark",
    candle_style: str = "Normal OHLC",
) -> go.Figure:
    """Plot signal closes, next-open executions and execution-aligned phases."""

    fig = go.Figure()
    if data.empty:
        return fig
    chart_theme = _ICHIMOKU_THEMES["light" if theme.strip().lower() == "light" else "dark"]
    show_heikin_ashi = candle_style.strip().lower() in {"heikin-ashi", "heikin ashi", "ha"}

    executions, phases = _ha_ema_executions(data)
    for phase in phases:
        fig.add_vrect(
            x0=phase["start"],
            x1=phase["end"],
            fillcolor=HA_EMA_COLORS[f"{phase['outcome']}_phase"],
            layer="below",
            line_width=0,
        )

    fig.add_trace(
        go.Candlestick(
            x=data.index,
            open=data["HA_Open"] if show_heikin_ashi else data["Open"],
            high=data["HA_High"] if show_heikin_ashi else data["High"],
            low=data["HA_Low"] if show_heikin_ashi else data["Low"],
            close=data["HA_Close"] if show_heikin_ashi else data["Close"],
            name="Heikin-Ashi" if show_heikin_ashi else "Normal OHLC",
            increasing_line_color="#16a34a",
            increasing_fillcolor="#22c55e",
            decreasing_line_color="#dc2626",
            decreasing_fillcolor="#ef4444",
            whiskerwidth=0.35,
        )
    )
    if show_heikin_ashi:
        fig.add_trace(
            go.Scatter(
                x=data.index,
                y=data["Close"],
                name="Actual close",
                line=dict(color=HA_EMA_COLORS["actual_close"], width=1, dash="dot"),
                opacity=0.7,
            )
        )
    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data["EMA_Fast"],
            name=f"EMA {fast_length}",
            line=dict(color=HA_EMA_COLORS["fast_ema"], width=1.6),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data["EMA_Slow"],
            name=f"EMA {slow_length}",
            line=dict(color=HA_EMA_COLORS["slow_ema"], width=1.6),
        )
    )
    for signal, symbol, color in (
        ("BUY", "triangle-up", HA_EMA_COLORS["buy"]),
        ("EXIT", "triangle-down", HA_EMA_COLORS["exit"]),
    ):
        events = [event for event in executions if event["type"] == signal]
        fig.add_trace(
            go.Scatter(
                x=[event["execution_date"] for event in events],
                y=[event["execution_price"] for event in events],
                name=f"{signal} execution",
                mode="markers",
                marker=dict(
                    symbol=symbol,
                    size=11,
                    color=color,
                    line=dict(color=chart_theme["marker_outline"], width=1),
                ),
                customdata=[
                    [event["signal_date"], event["signal_close"], event["trade_return_pct"]] for event in events
                ],
                hovertemplate=(
                    f"<b>{signal} execution</b><br>"
                    "Execution: %{x|%d %b %Y} at ₹%{y:,.2f}<br>"
                    "Signal: %{customdata[0]|%d %b %Y} at close ₹%{customdata[1]:,.2f}<br>"
                    + ("Gross phase return: %{customdata[2]:+.2f}%<br>" if signal == "EXIT" else "")
                    + "<extra></extra>"
                ),
            )
        )

    for signal, color in (
        ("BUY", HA_EMA_COLORS["buy_signal"]),
        ("EXIT", HA_EMA_COLORS["exit_signal"]),
    ):
        signal_events = data[data["Signal"] == signal]
        fig.add_trace(
            go.Scatter(
                x=signal_events.index,
                y=signal_events["Close"],
                name=f"{signal} signal close",
                mode="markers",
                marker=dict(symbol="circle-open", size=7, color=color, line=dict(width=1.5)),
                customdata=list(zip(signal_events["Return_Pct"], signal_events["Average_Volume"])),
                hovertemplate=(
                    f"<b>{signal} signal confirmed</b><br>%{{x|%d %b %Y}}<br>"
                    "Close: ₹%{y:,.2f}<br>"
                    "1Y return: %{customdata[0]:.1f}%<br>"
                    "30-week avg volume: %{customdata[1]:,.0f}<extra></extra>"
                ),
            )
        )

    default_start = data.index[max(0, len(data) - 300)]
    fig.update_layout(
        title=dict(
            text=f"{ticker} — Weekly HA Turn + EMA Trend",
            x=0.01,
            font=dict(size=15, color=chart_theme["title"]),
        ),
        yaxis=dict(
            type="log" if use_log_scale else "linear",
            title="Price (₹)",
            side="right",
            showgrid=True,
            gridcolor=chart_theme["grid"],
            zeroline=False,
        ),
        xaxis=dict(
            range=[default_start, data.index[-1]],
            rangeslider=dict(visible=False),
            rangebreaks=[dict(bounds=["sat", "mon"])],
            showgrid=False,
        ),
        height=640,
        margin=dict(l=14, r=72, t=58, b=72),
        legend=dict(orientation="h", x=0.01, y=-0.13),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=chart_theme["hover_background"],
            bordercolor=chart_theme["hover_border"],
            font=dict(color=chart_theme["hover_text"]),
        ),
        plot_bgcolor=chart_theme["background"],
        paper_bgcolor=chart_theme["background"],
        font=dict(color=chart_theme["text"]),
    )
    return fig


def ha_ema_equity_figure(equity, initial_capital: float = 100_000.0) -> go.Figure:
    """Plot strategy values and the same-start buy-and-hold benchmark."""

    fig = go.Figure()
    if equity.empty:
        return fig
    fig.add_trace(
        go.Scatter(
            x=equity.index,
            y=equity["Pre_Tax_Value"],
            name="Pre-tax value",
            line=dict(color=HA_EMA_COLORS["pre_tax"], width=2.2),
        )
    )
    if "Buy_Hold_Value" in equity:
        fig.add_trace(
            go.Scatter(
                x=equity.index,
                y=equity["Buy_Hold_Value"],
                name="Buy & hold",
                line=dict(color=HA_EMA_COLORS["buy_hold"], width=1.8, dash="dot"),
            )
        )
    fig.add_trace(
        go.Scatter(
            x=equity.index,
            y=equity["Post_Tax_Realised_Value"],
            name="After realised-tax estimate",
            line=dict(color=HA_EMA_COLORS["post_tax"], width=1.8, dash="dash"),
        )
    )
    fig.add_hline(y=initial_capital, line_color="#94a3b8", line_dash="dot", line_width=1)
    fig.update_layout(
        title="₹1 lakh signal replay",
        height=390,
        margin=dict(l=55, r=25, t=48, b=55),
        yaxis=dict(title="Account value (₹)", showgrid=True, gridcolor=_GRID),
        xaxis=dict(showgrid=False, rangebreaks=[dict(bounds=["sat", "mon"])]),
        legend=dict(orientation="h", y=-0.18),
        hovermode="x unified",
        plot_bgcolor=_T,
        paper_bgcolor=_T,
    )
    return fig


def ichimoku_strategy_comparison_figure(
    comparison,
    initial_capital: float = 100_000.0,
    selected_strategy: str = "Balanced",
) -> go.Figure:
    """Compare pre-tax account values for the three presets and Custom."""

    fig = go.Figure()
    if comparison.empty:
        return fig
    for name in ("Aggressive", "Balanced", "Conservative", "Custom"):
        if name not in comparison:
            continue
        selected = name == selected_strategy
        values = comparison[name].astype(float)
        gain_pct = (values / float(initial_capital) - 1.0) * 100.0
        fig.add_trace(
            go.Scatter(
                x=comparison.index,
                y=values,
                name=name,
                line=dict(
                    color=ICHIMOKU_STRATEGY_COLORS[name],
                    width=3.2 if selected else 1.8,
                    dash="solid" if selected else "dot",
                ),
                opacity=1.0 if selected else 0.78,
                customdata=gain_pct,
                hovertemplate=(
                    f"<b>{name}</b><br>%{{x|%d %b %Y}}<br>"
                    "Value: ₹%{y:,.0f}<br>Gain: %{customdata:+.2f}%<extra></extra>"
                ),
            )
        )
    fig.add_hline(y=initial_capital, line_color="#94a3b8", line_dash="dot", line_width=1)
    fig.update_layout(
        title="Ichimoku strategies · pre-tax ₹1 lakh comparison",
        height=410,
        margin=dict(l=55, r=25, t=48, b=60),
        yaxis=dict(title="Account value (₹)", showgrid=True, gridcolor=_GRID),
        xaxis=dict(showgrid=False, rangebreaks=[dict(bounds=["sat", "mon"])]),
        legend=dict(orientation="h", y=-0.18),
        hovermode="x unified",
        plot_bgcolor=_T,
        paper_bgcolor=_T,
    )
    return fig


def nav_chart_figure(nav_df) -> go.Figure:
    fig = go.Figure()
    for col in nav_df.columns:
        s = nav_df[col].dropna()
        fig.add_trace(go.Scatter(x=s.index, y=s.values, name=col, line=_bt_line(col)))
    fig.update_layout(
        height=420,
        hovermode="x unified",
        yaxis=dict(title="NAV", showgrid=True, gridcolor=_GRID),
        xaxis=dict(showgrid=False),
        legend=dict(orientation="h", y=-0.15),
        margin=dict(l=50, r=20, t=30, b=50),
        plot_bgcolor=_T,
        paper_bgcolor=_T,
    )
    return fig


def rolling_returns_figure(roll_df) -> go.Figure:
    fig = go.Figure()
    for col in roll_df.columns:
        s = roll_df[col].dropna()
        fig.add_trace(go.Scatter(x=s.index, y=s.values, name=col, line=_bt_line(col)))
    fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8", line_width=1)
    fig.update_layout(
        height=360,
        hovermode="x unified",
        yaxis=dict(title="CAGR (%)", showgrid=True, gridcolor=_GRID),
        xaxis=dict(showgrid=False),
        legend=dict(orientation="h", y=-0.18),
        margin=dict(l=50, r=20, t=30, b=55),
        plot_bgcolor=_T,
        paper_bgcolor=_T,
    )
    return fig


def portfolio_churn_figure(holdings_log: dict) -> go.Figure:
    """Bar chart of entries/exits counts + turnover % lines per rebalance date."""
    _T = "rgba(0,0,0,0)"
    _GRID = "rgba(128,128,128,0.2)"
    _ENTRY_COLORS = {"Classic": "rgba(59,130,246,0.6)", "Displacement": "rgba(245,158,11,0.6)"}
    _EXIT_COLORS = {"Classic": "rgba(167,139,250,0.6)", "Displacement": "rgba(52,211,153,0.6)"}
    _FULL_COLORS = {"Classic": "#3b82f6", "Displacement": "#f59e0b"}
    _MARG_COLORS = {"Classic": "#a78bfa", "Displacement": "#34d399"}
    _PROP_COLORS = {"Classic": "#2dd4bf", "Displacement": "#f472b6"}

    rule_names = [r for r in ("Classic", "Displacement") if r in holdings_log]
    n_rows = len(rule_names)

    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        specs=[[{"secondary_y": True}] for _ in rule_names],
        subplot_titles=rule_names,
        vertical_spacing=0.14,
    )

    for row_idx, rule in enumerate(rule_names, start=1):
        log = holdings_log[rule]
        dates = [e["date"] for e in log]
        n_entries = [len(e["entries"]) for e in log]
        n_exits = [len(e["exits"]) for e in log]
        full_to = [e.get("full_turnover_pct", 0.0) for e in log]
        marg_to = [e.get("marg_turnover_pct", 0.0) for e in log]
        prop_to = [e.get("prop_turnover_pct", 0.0) for e in log]
        show_leg = row_idx == 1

        fig.add_trace(
            go.Bar(
                x=dates,
                y=n_entries,
                name="Entries",
                marker_color=_ENTRY_COLORS[rule],
                legendgroup="entries",
                showlegend=show_leg,
            ),
            row=row_idx,
            col=1,
            secondary_y=False,
        )
        fig.add_trace(
            go.Bar(
                x=dates,
                y=[-v for v in n_exits],
                name="Exits",
                marker_color=_EXIT_COLORS[rule],
                legendgroup="exits",
                showlegend=show_leg,
            ),
            row=row_idx,
            col=1,
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=full_to,
                name="Full Turnover %",
                mode="lines+markers",
                line=dict(color=_FULL_COLORS[rule], width=2),
                legendgroup="full_to",
                showlegend=show_leg,
            ),
            row=row_idx,
            col=1,
            secondary_y=True,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=marg_to,
                name="Marg Turnover %",
                mode="lines+markers",
                line=dict(color=_MARG_COLORS[rule], width=2, dash="dash"),
                legendgroup="marg_to",
                showlegend=show_leg,
            ),
            row=row_idx,
            col=1,
            secondary_y=True,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=prop_to,
                name="Prop Turnover %",
                mode="lines+markers",
                line=dict(color=_PROP_COLORS[rule], width=2, dash="dashdot"),
                legendgroup="prop_to",
                showlegend=show_leg,
            ),
            row=row_idx,
            col=1,
            secondary_y=True,
        )

    fig.update_layout(
        height=280 * n_rows,
        barmode="overlay",
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.12),
        margin=dict(l=50, r=60, t=50, b=55),
        plot_bgcolor=_T,
        paper_bgcolor=_T,
    )
    for row_idx in range(1, n_rows + 1):
        fig.update_yaxes(title_text="# Stocks", row=row_idx, col=1, secondary_y=False, showgrid=True, gridcolor=_GRID)
        fig.update_yaxes(title_text="Turnover %", row=row_idx, col=1, secondary_y=True, showgrid=False)
    fig.update_xaxes(showgrid=False)
    return fig


# ── Portfolio weights chart ────────────────────────────────────────────────────

_PALETTE = plotly.colors.qualitative.Light24 + plotly.colors.qualitative.Plotly
_TICKER_COLOR_CACHE: dict[str, str] = {}


def _ticker_color(ticker: str) -> str:
    """Return a consistent color for a ticker, cycling through _PALETTE."""
    if ticker not in _TICKER_COLOR_CACHE:
        _TICKER_COLOR_CACHE[ticker] = _PALETTE[len(_TICKER_COLOR_CACHE) % len(_PALETTE)]
    return _TICKER_COLOR_CACHE[ticker]


def portfolio_weights_figure(rule_entries: list[dict], rule_name: str, weight_type: str = "marg") -> go.Figure:
    """Stacked bar chart of one weight type per rebalance date.

    weight_type: "full" for equal weights, "marg" for momentum weights.
    Holdings are sorted ascending by average marg weight so the highest-avg
    ticker's trace is rendered last and sits on top of every bar.
    Hover text identifies the ticker and its weight for that date.
    """
    if not rule_entries:
        return go.Figure()

    if weight_type == "full":
        weight_key, weight_label = "full_weights", "Full (equal)"
    elif weight_type == "prop":
        weight_key, weight_label = "prop_weights", "Prop (prop-fill)"
    else:
        weight_key, weight_label = "marg_weights", "Marginal (slot-fill)"

    dates = [e["date"] for e in rule_entries]

    # Always sort by avg marg weight so order is consistent across both charts
    ticker_sums: dict[str, list[float]] = {}
    for entry in rule_entries:
        for ticker, w in entry.get("marg_weights", {}).items():
            ticker_sums.setdefault(ticker, []).append(w)

    # Ascending sort → highest-avg tickers added last → on top of stack
    tickers_sorted = sorted(ticker_sums, key=lambda t: sum(ticker_sums[t]) / len(ticker_sums[t]))

    fig = go.Figure()

    for ticker in tickers_sorted:
        y = [e.get(weight_key, {}).get(ticker) for e in rule_entries]
        fig.add_trace(
            go.Bar(
                x=dates,
                y=y,
                name=ticker,
                showlegend=False,
                marker=dict(color=_ticker_color(ticker)),
                customdata=[ticker] * len(dates),
                hovertemplate="<b>%{customdata}</b><br>%{y:.2f}%<extra></extra>",
            )
        )

    fig.update_layout(
        title=dict(
            text=f"{rule_name} · {weight_label} Weights<br><sup>Hover for ticker & weight</sup>",
            font=dict(size=14),
        ),
        barmode="stack",
        bargap=0.15,
        height=500,
        hovermode="closest",
        xaxis=dict(showgrid=False, type="date"),
        yaxis=dict(title="Weight (%)", showgrid=True, gridcolor=_GRID),
        showlegend=False,
        margin=dict(l=50, r=20, t=70, b=40),
        plot_bgcolor=_T,
        paper_bgcolor=_T,
    )
    return fig
