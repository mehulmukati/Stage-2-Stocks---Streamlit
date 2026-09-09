"""
Pure worker functions for background job execution — no Streamlit calls allowed here.
Each function signature: (params: dict, emit: Callable, cancel_evt: Event) -> dict
"""

import dataclasses
import importlib
import threading
from typing import Callable

import pandas as pd

import data_backtest as backtest_data
import quant_portfolio_engine as quant_portfolio
from backtest_engine import BacktestConfig, run_backtest

if getattr(backtest_data, "BACKTEST_DATA_VERSION", 0) < 4:
    backtest_data = importlib.reload(backtest_data)
if getattr(quant_portfolio, "QUANT_PORTFOLIO_ENGINE_VERSION", 0) < 4:
    quant_portfolio = importlib.reload(quant_portfolio)

QuantPortfolioConfig = quant_portfolio.QuantPortfolioConfig
run_quant_portfolio = quant_portfolio.run_quant_portfolio

# Screener workers stay on the DB-backed pipeline.
from data import resolve_screener_data

# Backtest worker uses the parquet-backed pipeline — no DB dependency.
_load_constituents = backtest_data._load_constituents
load_benchmark_series = backtest_data.load_benchmark_series
load_compositions = backtest_data.load_compositions
load_ohlcv_for_backtest = backtest_data.load_ohlcv_for_backtest
load_quant_ohlcv_snapshot = backtest_data.load_quant_ohlcv_snapshot
sync_benchmark_data = backtest_data.sync_benchmark_data

SCREENER_WORKER_VERSION = 7


def stage2_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(for_momentum=False, emit=emit)
    if df.empty:
        raise RuntimeError("No Stage 2 data available. Yahoo Finance may be syncing — try again in 30 mins.")
    return {"df": df, "cache_date": cache_date, "source": source}


def momentum_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(for_momentum=True, emit=emit)
    if df.empty:
        raise RuntimeError("No Momentum data available. Try again in a few minutes or check your internet connection.")
    return {"df": df, "cache_date": cache_date, "source": source}


def quant_portfolio_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    """Load the survivorship-aware universe and run one Quant Portfolio Lab simulation."""
    emit("info", "Loading the local portfolio-research dataset…")
    compositions = load_compositions() if params.get("use_compositions", True) else pd.DataFrame()
    # Candle strategies require Open and Low, which the long Momentum baseline
    # does not currently contain. Use the local full-OHLCV snapshot without a
    # thousand-symbol live refresh in the request path.
    loaded = load_quant_ohlcv_snapshot(emit=emit)
    if not loaded.symbol_data:
        raise RuntimeError("Backtest price history is missing or unreadable.")
    requested_start = pd.Timestamp(params["start_date"]) if params.get("start_date") else None
    available_start = pd.Timestamp(loaded.earliest_price_date) if loaded.earliest_price_date else None
    if requested_start is not None and available_start is not None and requested_start < available_start:
        raise RuntimeError(
            f"The requested start date {requested_start:%d %b %Y} cannot be honoured: complete OHLCV "
            f"history currently starts on {available_start:%d %b %Y}. The run was stopped instead of "
            "silently shortening the backtest. Rebuild the long candle baseline with "
            "`python scripts/refresh_backtest_parquet.py --full`, then run again."
        )
    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")
    emit("info", f"Preparing {len(loaded.symbol_data):,} symbol histories…")
    config = QuantPortfolioConfig(
        strategy=params["strategy"],
        strategy_settings=params.get("strategy_settings", {}),
        start_date=params.get("start_date"),
        end_date=params.get("end_date"),
        index_names=params.get("universe", []),
        compositions_df=compositions if not compositions.empty else None,
        max_holdings=params.get("max_holdings", 10),
        initial_capital=params.get("initial_capital", 1_000_000.0),
        transaction_cost_pct=params.get("transaction_cost_pct", 0.1) / 100.0,
        brokerage_per_order=params.get("brokerage_per_order", 0.0),
        stcg_rate=params.get("stcg_rate", 0.0) / 100.0,
        ltcg_rate=params.get("ltcg_rate", 0.0) / 100.0,
        min_history_days=params.get("min_history_days", 260),
        minimum_median_volume=params.get("minimum_median_volume", 100_000.0),
        ranking_method=params.get("ranking_method", "strategy_score"),
        ranking_lookback_sessions=params.get("ranking_lookback_sessions", 55),
        ranking_benchmark="Nifty 100",
    )
    result = run_quant_portfolio(
        loaded.symbol_data,
        load_benchmark_series(refresh=False),
        config,
        emit,
        cancel_evt,
    )
    if "error" in result:
        raise RuntimeError(result["error"])
    result["ohlcv_date"] = loaded.max_price_date or loaded.actual_latest_date
    result["ohlcv_start_date"] = loaded.earliest_price_date
    result["ohlcv_source"] = loaded.source
    return result


def backtest_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    emit("info", "Syncing benchmark index data…")
    sync_benchmark_data()

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    symbol_data, ohlcv_date, ohlcv_source = load_ohlcv_for_backtest(emit=emit)
    if not symbol_data:
        raise RuntimeError("Backtest parquet missing or unreadable. " "Run: python scripts/refresh_backtest_parquet.py")

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    if params.get("universe"):
        constituents = _load_constituents()
        allowed = {s for idx, syms in constituents.items() if idx in params["universe"] for s in syms}
        symbol_data = {s: df for s, df in symbol_data.items() if s in allowed}

    compositions_df = load_compositions() if params.get("use_compositions") else None
    if compositions_df is not None and not compositions_df.empty:
        emit("info", "🛡️ Historical constituent filter active (survivorship-bias mitigation)")
    elif params.get("use_compositions"):
        emit("warning", "compositions.parquet not found — constituent filter disabled")

    benchmarks = load_benchmark_series()

    base_config = BacktestConfig(
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
        brokerage_per_sale=params.get("brokerage_per_sale", 0.0),
        initial_capital=params.get("initial_capital", 1_000_000),
        ltcg_rate=params.get("ltcg_rate", 0.0),
        stcg_rate=params.get("stcg_rate", 0.0),
        max_position_pct=params.get("max_position_pct") or None,
        stage2_drop_exit=params.get("stage2_drop_exit", False),
        stage2_drop_threshold=params.get("stage2_drop_threshold", 2),
        stage2_entry_filter=params.get("stage2_entry_filter", False),
        stage2_entry_threshold=params.get("stage2_entry_threshold", 2),
        min_annual_return=params.get("min_annual_return", 0.0),
        pct_from_52w_high=params.get("pct_from_52w_high", 100.0),
        max_circuits=params.get("max_circuits", 999),
        close_above_100dma=params.get("close_above_100dma", False),
        close_above_200dma=params.get("close_above_200dma", False),
        pos_days_3m_min=params.get("pos_days_3m_min", 0.0),
        pos_days_6m_min=params.get("pos_days_6m_min", 0.0),
        pos_days_12m_min=params.get("pos_days_12m_min", 0.0),
    )

    emit("info", f"Running Classic band rule ({params['rebalance_freq']}, M={params['m']}, N={params['n']})…")
    result_classic = run_backtest(symbol_data, benchmarks, dataclasses.replace(base_config, band_rule="classic"))
    if "error" in result_classic:
        raise RuntimeError(result_classic["error"])

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    emit("info", f"Running Displacement band rule ({params['rebalance_freq']}, M={params['m']}, N={params['n']})…")
    result_disp = run_backtest(symbol_data, benchmarks, dataclasses.replace(base_config, band_rule="displacement"))
    if "error" in result_disp:
        raise RuntimeError(result_disp["error"])

    # ── merge NAV DataFrames ──
    nav_classic = result_classic["nav"].rename(
        columns={
            "Full Rebalance": "Classic · Full",
            "Marginal Rebalance": "Classic · Marginal",
            "Prop Rebalance": "Classic · Prop",
        }
    )
    nav_disp = result_disp["nav"][["Full Rebalance", "Marginal Rebalance", "Prop Rebalance"]].rename(
        columns={
            "Full Rebalance": "Displacement · Full",
            "Marginal Rebalance": "Displacement · Marginal",
            "Prop Rebalance": "Displacement · Prop",
        }
    )
    nav_merged = nav_classic.join(nav_disp, how="outer")

    # ── merge stats DataFrames ──
    _strategy_rows = ("Full Rebalance", "Marginal Rebalance", "Prop Rebalance")
    bench_rows = [r for r in result_classic["stats"].index if r not in _strategy_rows]
    stats_classic = result_classic["stats"].rename(
        index={
            "Full Rebalance": "Classic · Full",
            "Marginal Rebalance": "Classic · Marginal",
            "Prop Rebalance": "Classic · Prop",
        }
    )
    stats_disp = (
        result_disp["stats"]
        .drop(index=bench_rows, errors="ignore")
        .rename(
            index={
                "Full Rebalance": "Displacement · Full",
                "Marginal Rebalance": "Displacement · Marginal",
                "Prop Rebalance": "Displacement · Prop",
            }
        )
    )
    stats_merged = pd.concat(
        [
            stats_classic.loc[["Classic · Full", "Classic · Marginal", "Classic · Prop"]],
            stats_disp.loc[["Displacement · Full", "Displacement · Marginal", "Displacement · Prop"]],
            stats_classic.loc[bench_rows],
        ]
    )

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
        "avg_turnover_pct_marg": {
            "Classic": result_classic["avg_turnover_pct_marg"],
            "Displacement": result_disp["avg_turnover_pct_marg"],
        },
        "avg_turnover_pct_prop": {
            "Classic": result_classic["avg_turnover_pct_prop"],
            "Displacement": result_disp["avg_turnover_pct_prop"],
        },
        "total_cost_drag_pct": {
            "Classic": result_classic["total_cost_drag_pct"],
            "Displacement": result_disp["total_cost_drag_pct"],
        },
        "total_cost_drag_pct_marg": {
            "Classic": result_classic["total_cost_drag_pct_marg"],
            "Displacement": result_disp["total_cost_drag_pct_marg"],
        },
        "total_cost_drag_pct_prop": {
            "Classic": result_classic["total_cost_drag_pct_prop"],
            "Displacement": result_disp["total_cost_drag_pct_prop"],
        },
        "rebalance_dates": result_classic["rebalance_dates"],
        "trading_days": result_classic["trading_days"],
        "ohlcv_date": ohlcv_date,
        "ohlcv_source": ohlcv_source,
        "m": params["m"],
    }
