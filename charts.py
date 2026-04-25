import plotly.graph_objects as go

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
