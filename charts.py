import plotly.colors
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
    "Displacement · Full": "#f59e0b",  # amber-500
    "Displacement · Marginal": "#34d399",  # emerald-400
    "NIFTY50": "#f87171",  # red-400
    "NIFTY500": "#fb923c",  # orange-400
    # legacy keys (pre-rename format)
    "Full Rebalance": "#3b82f6",
    "Marginal Rebalance": "#a78bfa",
}


def _bt_line(col: str) -> dict:
    """Return line style dict for a backtest series column."""
    name = col.lower()
    if "marginal" in name:
        return dict(color=BT_COLORS.get(col, "#94a3b8"), width=2, dash="dash")
    if "nifty" in name or "benchmark" in name:
        return dict(color=BT_COLORS.get(col, "#94a3b8"), width=1.5, dash="dot")
    return dict(color=BT_COLORS.get(col, "#94a3b8"), width=2.5, dash="solid")


_T = "rgba(0,0,0,0)"
_GRID = "rgba(128,128,128,0.2)"


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

    weight_key = "full_weights" if weight_type == "full" else "marg_weights"
    weight_label = "Full (equal)" if weight_type == "full" else "Marginal (momentum)"

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
