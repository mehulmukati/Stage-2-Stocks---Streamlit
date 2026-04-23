"""
Pure worker functions for background job execution — no Streamlit calls allowed here.
Each function signature: (params: dict, emit: Callable, cancel_evt: Event) -> dict
"""

import threading
from typing import Callable

from backtest_engine import run_backtest
from data import (
    _load_constituents,
    load_benchmark_series,
    load_compositions,
    load_ohlcv_for_backtest,
    resolve_screener_data,
    sync_benchmark_data,
)


def stage2_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(False, for_momentum=False, emit=emit)
    if df.empty:
        raise RuntimeError(
            "No Stage 2 data available. Yahoo Finance may be syncing — try again in 30 mins."
        )
    return {"df": df, "cache_date": cache_date, "source": source}


def momentum_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(False, for_momentum=True, emit=emit)
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
            "No OHLCV data in database. Run the Momentum screener first to sync data."
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
        emit("warning", "compositions.csv not found — constituent filter disabled")

    benchmarks = load_benchmark_series()
    emit("info", f"Running simulation ({params['rebalance_freq']}, M={params['m']}, N={params['n']})…")

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    result = run_backtest(
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

    if "error" in result:
        raise RuntimeError(result["error"])

    result["ohlcv_date"] = ohlcv_date
    result["ohlcv_source"] = ohlcv_source
    result["m"] = params["m"]
    return result
