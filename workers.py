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
import market_data as shared_market
import quant_portfolio_engine as quant_portfolio
from backtest_engine import BacktestConfig, run_backtest

if getattr(backtest_data, "BACKTEST_DATA_VERSION", 0) < 7:
    backtest_data = importlib.reload(backtest_data)
if getattr(quant_portfolio, "QUANT_PORTFOLIO_ENGINE_VERSION", 0) < 7:
    quant_portfolio = importlib.reload(quant_portfolio)

QuantPortfolioConfig = quant_portfolio.QuantPortfolioConfig
run_quant_portfolio = quant_portfolio.run_quant_portfolio

# All workers use the shared market sources.
from data import resolve_screener_data

# Backtest worker uses the parquet-backed pipeline — no DB dependency.
_load_constituents = backtest_data._load_constituents
load_benchmark_series = backtest_data.load_benchmark_series
load_compositions = backtest_data.load_compositions
load_ohlcv_for_backtest = backtest_data.load_ohlcv_for_backtest
load_quant_ohlcv_snapshot = backtest_data.load_quant_ohlcv_snapshot
sync_benchmark_data = backtest_data.sync_benchmark_data

SCREENER_WORKER_VERSION = 12


def stage2_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(for_momentum=False, emit=emit)
    if df.empty:
        raise RuntimeError("No Stage 2 data available in the shared sources. Run the shared refresh.")
    return {"df": df, "cache_date": cache_date, "source": source}


def momentum_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    df, cache_date, source = resolve_screener_data(for_momentum=True, emit=emit, as_of_date=params.get("as_of_date"))
    if df.empty:
        raise RuntimeError("No Momentum data available in the shared sources. Run the shared refresh.")
    return {"df": df, "cache_date": cache_date, "source": source}


def quant_portfolio_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    """Load the survivorship-aware universe and run one Quant Portfolio Lab simulation."""
    emit("info", "Loading the local portfolio-research dataset…")
    snapshot = shared_market.load_snapshot(params.get("end_date"))
    compositions = load_compositions(snapshot=snapshot) if params.get("use_compositions", True) else pd.DataFrame()
    # Candle strategies require Open and Low, which the long Momentum baseline
    # does not currently contain. Use the local full-OHLCV snapshot without a
    # thousand-symbol live refresh in the request path.
    loaded = load_quant_ohlcv_snapshot(emit=emit, snapshot=snapshot)
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
    market_calendar = snapshot.trading_calendar()
    symbol_data = loaded.symbol_data
    benchmarks = load_benchmark_series(refresh=False, snapshot=snapshot)
    membership_coverage = []
    if params["strategy"] == "weekly_hammer":
        universe = params.get("universe") or []
        if not universe or compositions.empty:
            raise RuntimeError("Select at least one index with historical constituent data.")
        price_end = pd.Timestamp(loaded.max_price_date)
        market_calendar = market_calendar[market_calendar <= price_end]
        if market_calendar.empty:
            raise RuntimeError("No observed market sessions are available for the selected range.")
        allowed = set()
        for name in universe:
            key = shared_market.index_key(name)
            rows = compositions.loc[compositions.INDEX_NAME.eq(key) & compositions.TIME_STAMP.le(price_end)]
            if rows.empty:
                raise RuntimeError(f"No historical composition is available for {name} by {price_end:%d %b %Y}.")
            allowed.update(rows.SYMBOL)
            first, last = rows.TIME_STAMP.min(), rows.TIME_STAMP.max()
            membership_coverage.append(
                {
                    "Index": name,
                    "First snapshot": str(first.date()),
                    "Last snapshot used by price cutoff": str(last.date()),
                    "Age at price cutoff (days)": (price_end - last).days,
                    "Historical members": rows.SYMBOL.nunique(),
                    "Members with price history": rows.loc[rows.SYMBOL.isin(symbol_data), "SYMBOL"].nunique(),
                }
            )
        # Keep successor quotes for corporate actions on existing positions.
        from corporate_actions import load_corporate_actions

        for action in sorted(load_corporate_actions(), key=lambda entry: entry["effective_date"]):
            if action["old_symbol"] in allowed and action["effective_date"] <= price_end:
                allowed.add(action["successor_symbol"])
        symbol_data = {symbol: frame for symbol, frame in symbol_data.items() if symbol in allowed}
        emit("info", f"Replaying {len(symbol_data):,} priced symbols in the selected historical universe.")
        benchmarks.update(snapshot.series("index_price", universe))
    config = QuantPortfolioConfig(
        market_calendar=market_calendar,
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
        symbol_data,
        benchmarks,
        config,
        emit,
        cancel_evt,
    )
    if "error" in result:
        raise RuntimeError(result["error"])
    result["source_revisions"] = snapshot.revisions
    result["equity_policy_version"] = shared_market.EQUITY_POLICY_VERSION
    result["ohlcv_date"] = loaded.max_price_date or loaded.actual_latest_date
    result["ohlcv_start_date"] = loaded.earliest_price_date
    result["ohlcv_source"] = loaded.source
    if membership_coverage:
        result["membership_coverage"] = membership_coverage
    return result


def backtest_worker(params: dict, emit: Callable, cancel_evt: threading.Event) -> dict:
    emit("info", "Syncing benchmark index data…")
    sync_benchmark_data()

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    snapshot = shared_market.load_snapshot(params.get("end_date"))
    loaded = load_ohlcv_for_backtest(emit=emit, snapshot=snapshot)
    symbol_data, ohlcv_date, ohlcv_source = loaded
    if not symbol_data:
        raise RuntimeError("Backtest parquet missing or unreadable. " "Run: python scripts/refresh_backtest_parquet.py")

    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")

    if params.get("universe"):
        selected_keys = {shared_market.index_key(i) for i in params["universe"]}
        allowed = set(snapshot.membership.loc[snapshot.membership.INDEX_NAME.isin(selected_keys), "SYMBOL"])
        symbol_data = {s: df for s, df in symbol_data.items() if s in allowed}

    compositions_df = load_compositions(snapshot=snapshot) if params.get("use_compositions") else None
    if compositions_df is not None and not compositions_df.empty:
        emit("info", "🛡️ Historical constituent filter active (survivorship-bias mitigation)")
    elif params.get("use_compositions"):
        emit("warning", "Shared historical membership unavailable")

    benchmarks = load_benchmark_series(snapshot=snapshot)

    base_config = BacktestConfig(
        market_calendar=snapshot.trading_calendar(),
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
        minimum_median_volume=params.get("minimum_median_volume", 100_000),
        max_stale_sessions=params.get("max_stale_sessions", 3),
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

    emit(
        "info",
        f"Running Displacement band rule ({params['rebalance_freq']}, M={params['m']}, N={params['n']})…",
    )
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
        "source_revisions": snapshot.revisions,
        "equity_policy_version": shared_market.EQUITY_POLICY_VERSION,
        "ohlcv_source": ohlcv_source,
        "m": params["m"],
        "accounting_version": result_classic["accounting_version"],
        "policies": result_classic["policies"],
        "certification": result_classic["certification"],
        "coverage_report": result_classic["coverage_report"],
        "run_manifests": {"Classic": result_classic["run_manifest"], "Displacement": result_disp["run_manifest"]},
        "execution_ledgers": {
            "Classic": result_classic["execution_ledgers"],
            "Displacement": result_disp["execution_ledgers"],
        },
        "daily_ledgers": {"Classic": result_classic["daily_ledgers"], "Displacement": result_disp["daily_ledgers"]},
        "closed_lots": {"Classic": result_classic["closed_lots"], "Displacement": result_disp["closed_lots"]},
        "fill_failures": {"Classic": result_classic["fill_failures"], "Displacement": result_disp["fill_failures"]},
        "tax_events": {"Classic": result_classic["tax_events"], "Displacement": result_disp["tax_events"]},
        "accounting_totals": {
            "Classic": result_classic["accounting_totals"],
            "Displacement": result_disp["accounting_totals"],
        },
    }


def shared_market_refresh_worker(params, emit, cancel_evt):
    from scripts.refresh_constituents import refresh
    from scripts.refresh_market_data import refresh_prices

    emit("info", "Verifying shared constituents…")
    refresh()
    if cancel_evt.is_set():
        raise RuntimeError("Cancelled")
    report = refresh_prices(emit=emit, require_coverage=True)
    return dict(report=report, source_revisions=shared_market.source_revisions())
