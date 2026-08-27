import math

import plotly.colors
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ICHIMOKU_CHART_VERSION = 8

PHASE_COLORS = {
    "Strong Stage 2": "rgba(34, 197, 94, 0.25)",
    "Likely Stage 2": "rgba(234, 179, 8, 0.25)",
    "Early/Weak Stage 2": "rgba(249, 115, 22, 0.22)",
}

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
            color = PHASE_COLORS.get(grp["Phase"].iloc[0])
            if color is None:
                continue
            fig.add_vrect(x0=grp.index[0], x1=grp.index[-1], fillcolor=color, layer="below", line_width=0)

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

    fig.update_layout(
        title=dict(text=f"{ticker} — Stage 2 Phase Map", font=dict(size=16)),
        yaxis=dict(
            type="log" if use_log_scale else "linear",
            showgrid=True,
            gridcolor=_GRID,
            title="Price (log)" if use_log_scale else "Price",
        ),
        xaxis=dict(showgrid=False),
        height=540,
        margin=dict(l=50, r=20, t=55, b=40),
        legend=dict(orientation="h", y=-0.13),
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
