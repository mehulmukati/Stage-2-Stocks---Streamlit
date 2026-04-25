"""
Pure worker functions for background job execution — no Streamlit calls allowed here.
Each function signature: (params: dict, emit: Callable, cancel_evt: Event) -> dict
"""

import threading
from typing import Callable

from backtest_engine import run_backtest

# Screener workers stay on the DB-backed pipeline.
from data import resolve_screener_data

# Backtest worker uses the parquet-backed pipeline — no DB dependency.
from data_backtest import (
    _load_constituents,
    load_benchmark_series,
    load_compositions,
    load_ohlcv_for_backtest,
    sync_benchmark_data,
)


def stage2_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(for_momentum=False, emit=emit)
    if df.empty:
        raise RuntimeError(
            "No Stage 2 data available. Yahoo Finance may be syncing — try again in 30 mins."
        )
    return {"df": df, "cache_date": cache_date, "source": source}


def momentum_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(for_momentum=True, emit=emit)
    if df.empty:
        raise RuntimeError(
            "No Momentum data available. Try again in a few minutes or check your internet connection."
        )
    return {"df": df, "cache_date": cache_date, "source": source}


def backtest_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    emit("info", "Syncing benchmark index data…")
    sync_benchmark_data()

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    symbol_data, ohlcv_date, ohlcv_source = load_ohlcv_for_backtest(emit=emit)
    if not symbol_data:
        raise RuntimeError(
            "Backtest parquet missing or unreadable. "
            "Run: python scripts/refresh_backtest_parquet.py"
        )

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    if params.get("universe"):
        constituents = _load_constituents()
        allowed = {
            s for idx, syms in constituents.items() if idx in params["universe"] for s in syms
        }
        symbol_data = {s: df for s, df in symbol_data.items() if s in allowed}

    compositions_df = load_compositions() if params.get("use_compositions") else None
    if compositions_df is not None and not compositions_df.empty:
        emit("info", "🛡️ Historical constituent filter active (survivorship-bias mitigation)")
    elif params.get("use_compositions"):
        emit("warning", "compositions.parquet not found — constituent filter disabled")

    benchmarks = load_benchmark_series()

    common_kwargs = dict(
        all_ohlcv=symbol_data,
        benchmarks=benchmarks,
        m=params["m"],
        n=params["n"],
        rebalance_freq=params["rebalance_freq"],
        sort_method=params["sort_method"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        compositions_df=compositions_df,
        index_names=params["universe"] or [],
        transaction_cost_pct=params["transaction_cost_pct"] / 100.0,
        min_history_days=params["min_history_days"],
        apply_volume_filter=True,
    )

    emit("info", f"Running Classic band rule ({params['rebalance_freq']}, M={params['m']}, N={params['n']})…")
    result_classic = run_backtest(**common_kwargs, band_rule="classic")
    if "error" in result_classic:
        raise RuntimeError(result_classic["error"])

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    emit("info", f"Running Displacement band rule ({params['rebalance_freq']}, M={params['m']}, N={params['n']})…")
    result_disp = run_backtest(**common_kwargs, band_rule="displacement")
    if "error" in result_disp:
        raise RuntimeError(result_disp["error"])

    import pandas as pd

    # ── merge NAV DataFrames ──
    nav_classic = result_classic["nav"].rename(columns={
        "Full Rebalance": "Classic · Full",
        "Marginal Rebalance": "Classic · Marginal",
    })
    nav_disp = result_disp["nav"][["Full Rebalance", "Marginal Rebalance"]].rename(columns={
        "Full Rebalance": "Displacement · Full",
        "Marginal Rebalance": "Displacement · Marginal",
    })
    nav_merged = nav_classic.join(nav_disp, how="outer")

    # ── merge stats DataFrames ──
    bench_rows = [r for r in result_classic["stats"].index
                  if r not in ("Full Rebalance", "Marginal Rebalance")]
    stats_classic = result_classic["stats"].rename(index={
        "Full Rebalance": "Classic · Full",
        "Marginal Rebalance": "Classic · Marginal",
    })
    stats_disp = result_disp["stats"].drop(index=bench_rows, errors="ignore").rename(index={
        "Full Rebalance": "Displacement · Full",
        "Marginal Rebalance": "Displacement · Marginal",
    })
    stats_merged = pd.concat([
        stats_classic.loc[["Classic · Full", "Classic · Marginal"]],
        stats_disp.loc[["Displacement · Full", "Displacement · Marginal"]],
        stats_classic.loc[bench_rows],
    ])

    return {
        "nav": nav_merged,
        "stats": stats_merged,
        "holdings_log": {
            "Classic": result_classic["holdings_log"],
            "Displacement": result_disp["holdings_log"],
        },
        "avg_turnover_pct": {
            "Classic": result_classic["avg_turnover_pct"],
            "Displacement": result_disp["avg_turnover_pct"],
        },
        "total_cost_drag_pct": {
            "Classic": result_classic["total_cost_drag_pct"],
            "Displacement": result_disp["total_cost_drag_pct"],
        },
        "rebalance_dates": result_classic["rebalance_dates"],
        "trading_days": result_classic["trading_days"],
        "ohlcv_date": ohlcv_date,
        "ohlcv_source": ohlcv_source,
        "m": params["m"],
    }
