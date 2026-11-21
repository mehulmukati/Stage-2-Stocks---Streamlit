"""Read completed weekly hammer research runs from the local artifact directory."""

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

RUNS = Path(__file__).resolve().parents[2] / "outputs" / "weekly_hammer_reversal"
STUDIES = {"bounded": "Dated membership", "snapshots": "Observed snapshots"}
VARIANTS = {
    "A": "Strict hammer",
    "B": "A + RSI ≤ 35",
    "C": "B + RSI below its EMA",
    "D": "C + relative volume ≥ 1.5",
    "E": "D + prior decline ≥ 15%",
}


@st.cache_data
def _read_csv(path: Path, modified: int) -> pd.DataFrame:
    return pd.read_csv(path)


@st.cache_data
def _read_ledger(path: Path, modified: int, ids: tuple, keys: tuple) -> pd.DataFrame:
    return pd.read_parquet(path, filters=[(name, "in", list(values)) for name, values in zip(keys, ids)])


def _csv(path: Path) -> pd.DataFrame:
    return _read_csv(path, path.stat().st_mtime_ns)


def _table(frame, names, percentages=(), points=()):
    formatted = {names[column]: st.column_config.NumberColumn(format="percent") for column in percentages}
    formatted.update({names[column]: st.column_config.NumberColumn(format="%+.2f pp") for column in points})
    displayed = frame[list(names)].rename(columns=names).copy()
    for column in points:
        displayed[names[column]] *= 100
    st.dataframe(displayed, hide_index=True, column_config=formatted)


def _signal_detail(folder, signals, horizon):
    if signals.empty:
        st.info("This sample contains no matching signals.")
        return
    ticker = st.selectbox("Ticker", sorted(signals.ticker.dropna().unique()), index=None, key="hammer_ticker")
    if not ticker:
        return
    candidates = signals.loc[signals.ticker.eq(ticker)].sort_values("signal_date", ascending=False)
    labels = candidates.set_index("signal_id").apply(lambda row: f"{row.signal_date} · {row.security_id}", axis=1)
    signal_id = st.selectbox("Signal", candidates.signal_id.tolist(), format_func=labels.get, key="hammer_signal")
    signal = candidates.set_index("signal_id").loc[signal_id]
    outcome_path = folder / "outcome_ledger.parquet"
    outcomes = _read_ledger(outcome_path, outcome_path.stat().st_mtime_ns, ((signal_id,),), ("signal_id",)).sort_values(
        "holding_weeks"
    )
    selected = outcomes.loc[outcomes.holding_weeks.eq(horizon)].iloc[0]
    candle_path = folder / "stock_week_ledger.parquet"
    candles = _read_ledger(
        candle_path, candle_path.stat().st_mtime_ns, ((signal.security_id,),), ("security_id",)
    ).sort_values("signal_date")
    date = pd.Timestamp(signal.signal_date)
    window_end = max(date, selected.exit_date) if pd.notna(selected.exit_date) else date
    candles = candles.loc[candles.signal_date.between(date - pd.Timedelta(weeks=12), window_end)]
    figure = go.Figure(
        go.Candlestick(
            x=candles.signal_date,
            open=candles.Open,
            high=candles.High,
            low=candles.Low,
            close=candles.Close,
            name="Weekly OHLC",
        )
    )
    figure.add_scatter(x=[date], y=[signal.Close], name="Hammer", mode="markers", marker_symbol="diamond")
    if selected.status == "filled":
        for label, point, price in (
            ("Entry open", selected.entry_date, selected.entry_open),
            ("Exit open", selected.exit_date, selected.exit_open),
        ):
            figure.add_scatter(x=[point], y=[price], name=label, mode="markers", marker_size=10)
    figure.update_layout(yaxis_title="Study price basis", xaxis_rangeslider_visible=False)
    st.plotly_chart(figure)
    st.caption("Weekly candles shown where present in the saved study ledger.")
    st.line_chart(
        candles.set_index("signal_date")[["rsi", "rsi_ema"]].rename(columns={"rsi": "RSI", "rsi_ema": "RSI EMA"})
    )
    st.bar_chart(candles.set_index("signal_date")[["Volume"]])
    _table(
        outcomes,
        {
            "holding_weeks": "Weeks",
            "status": "Outcome",
            "entry_date": "Entry",
            "entry_open": "Entry open",
            "exit_date": "Exit",
            "exit_open": "Exit open",
            "gross_return": "Gross return",
            "benchmark_open_return": "Nifty 500 return",
        },
        percentages=("gross_return", "benchmark_open_return"),
    )


def render_weekly_hammer() -> None:
    st.title("Weekly Hammer Reversal")
    st.caption("Historical event-study results. Each signal is evaluated independently.")
    st.caption("For an index portfolio and rolling performance, select Weekly Hammer Reversal in Portfolio Lab.")
    try:
        _render_saved_run()
    except (OSError, ValueError, KeyError, IndexError, pd.errors.ParserError) as exc:
        st.error(f"The saved weekly hammer results could not be read. {exc}")


def _render_saved_run():
    import json

    runs = {}
    for status_path in RUNS.glob("*/run_status.json"):
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
            if status.get("status") == "complete":
                runs[status_path.parent.name] = status
        except (OSError, ValueError):
            continue
    if not runs:
        st.info("No completed baseline runs are available on this app instance.")
        st.code("python -m scripts.run_weekly_hammer_reversal --run-id NEW_ID --as-of 2026-11-18")
        return
    run_names = sorted(
        runs, key=lambda name: (RUNS.joinpath(name, "run_status.json").stat().st_mtime_ns, name), reverse=True
    )
    run_id = st.selectbox("Run", run_names, key="hammer_run")
    status = runs[run_id]
    studies = {item["label"]: item for item in status["studies"]}
    study_id = st.selectbox("Study", list(studies), format_func=STUDIES.get, key="hammer_study")
    study = studies[study_id]
    folder = RUNS / run_id / study_id
    st.caption(
        f"Signal closes {study['start']} through {study['end']} · "
        f"{study['eligible_stock_weeks']:,} eligible stock-weeks · price cutoff {study['price_cutoff']}"
    )
    if study_id == "snapshots":
        st.warning("Membership is carried forward from observed snapshots. Effective change dates may differ.")
    else:
        st.info("The dated membership study uses provisional, corporate-action-adjusted research prices.")
    st.caption(status["holdout_status"])
    summary = _csv(folder / "summary.csv")
    comparisons = _csv(folder / "comparisons.csv")
    cols = st.columns(3)
    segment = cols[0].selectbox("Research segment", sorted(summary.segment.unique()), key="hammer_segment")
    horizons = sorted(summary.horizon.unique())
    horizon = cols[1].selectbox("Holding weeks", horizons, index=horizons.index(8), key="hammer_horizon")
    variant = cols[2].selectbox("Signal variant", list(VARIANTS), format_func=VARIANTS.get, key="hammer_variant")
    tab_summary, tab_signals, tab_coverage = st.tabs(["Results", "Signal detail", "Coverage"])
    with tab_summary:
        rows = summary.loc[
            summary.segment.eq(segment) & summary.horizon.eq(horizon) & summary["sample"].eq("variant_ready")
        ]
        rows = rows.assign(cohort=rows.cohort.replace(VARIANTS))
        _table(
            rows,
            {
                "cohort": "Cohort",
                "observations": "Observations",
                "round_trips": "Round trips",
                "mean": "Gross mean",
                "median": "Median",
                "win_rate": "Win rate",
                "mean_relative": "Mean excess return",
            },
            percentages=("mean", "median", "win_rate", "mean_relative"),
        )
        st.subheader("Same-week comparisons")
        subset = comparisons.loc[
            comparisons.segment.eq(segment)
            & comparisons.horizon.eq(horizon)
            & comparisons["sample"].eq("common_ready")
            & comparisons.target.eq(variant)
        ]
        _table(
            subset,
            {
                "target": "Variant",
                "reference": "Control",
                "matched_weeks": "Matched weeks",
                "matched_delta": "Mean difference",
                "ci_lower": "95% lower",
                "ci_upper": "95% upper",
            },
            points=("matched_delta", "ci_lower", "ci_upper"),
        )
        st.caption("Comparisons use observations with all baseline features available. Sparse samples omit intervals.")
    with tab_signals:
        signals = _csv(folder / "signal_ledger.csv")
        signals = signals.loc[signals.segment.eq(segment) & signals[variant]].copy()
        _signal_detail(folder, signals, horizon)
    with tab_coverage:
        st.dataframe(_csv(folder / "coverage_by_year.csv"), hide_index=True)
        st.dataframe(_csv(folder / "execution_coverage.csv").query("holding_weeks == @horizon"), hide_index=True)
    for title, name in (
        ("Download summary CSV", "summary.csv"),
        ("Download matched comparisons CSV", "comparisons.csv"),
        ("Download signal ledger CSV", "signal_ledger.csv"),
    ):
        st.download_button(title, (folder / name).read_bytes(), file_name=name, mime="text/csv")
