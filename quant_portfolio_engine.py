"""Event-driven fixed-slot portfolio simulator for normalized quant signals."""

from __future__ import annotations

import importlib
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd

import quant_strategy_registry as strategy_registry
from backtest_engine import _compute_fy_tax, _financial_year, _prepare_compositions, _valid_symbols_at_date
from corporate_actions import load_corporate_actions
from quant_portfolio_metrics import build_quant_metrics

if getattr(strategy_registry, "QUANT_STRATEGY_REGISTRY_VERSION", 0) < 3:
    strategy_registry = importlib.reload(strategy_registry)

build_strategy = strategy_registry.build_strategy
strategy_metadata = strategy_registry.strategy_metadata

QUANT_PORTFOLIO_ENGINE_VERSION = 4
RANK_STRATEGY_SCORE = "strategy_score"
RANK_RELATIVE_STRENGTH = "relative_strength"
RANKING_LABELS = {
    RANK_STRATEGY_SCORE: "Strategy composite",
    RANK_RELATIVE_STRENGTH: "Relative strength vs Nifty 100",
}
_SIGNAL_CACHE_LIMIT = 2_000
_signal_cache: OrderedDict[tuple, pd.DataFrame] = OrderedDict()
_signal_cache_lock = threading.RLock()
_batch_signal_cache: OrderedDict[tuple, dict[str, pd.DataFrame]] = OrderedDict()


@dataclass(frozen=True)
class QuantPortfolioConfig:
    strategy: str = "ha_ema"
    strategy_settings: dict[str, Any] = field(default_factory=dict)
    start_date: str | None = None
    end_date: str | None = None
    index_names: list[str] = field(default_factory=list)
    compositions_df: pd.DataFrame | None = field(default=None, compare=False, repr=False)
    max_holdings: int = 10
    initial_capital: float = 1_000_000.0
    transaction_cost_pct: float = 0.001
    brokerage_per_order: float = 0.0
    stcg_rate: float = 0.0
    ltcg_rate: float = 0.0
    min_history_days: int = 260
    minimum_median_volume: float = 100_000.0
    ranking_method: str = RANK_STRATEGY_SCORE
    ranking_lookback_sessions: int = 55
    ranking_benchmark: str = "Nifty 100"


def _normalise_prices(data: pd.DataFrame) -> pd.DataFrame:
    frame = data.copy()
    frame.index = pd.DatetimeIndex(pd.to_datetime(frame.index, errors="coerce"))
    frame = frame[~frame.index.isna()].sort_index()
    if frame.index.tz is not None:
        frame.index = frame.index.tz_localize(None)
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in frame:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame[~frame.index.duplicated(keep="last")]


def _price(frame: pd.DataFrame, date: pd.Timestamp, column: str) -> float | None:
    if date not in frame.index or column not in frame:
        return None
    value = frame.at[date, column]
    return float(value) if pd.notna(value) and float(value) > 0 else None


def _returns_over_sessions(close: pd.Series, lookback: int) -> pd.Series:
    clean = pd.to_numeric(close, errors="coerce").copy()
    clean.index = pd.DatetimeIndex(pd.to_datetime(clean.index, errors="coerce")).tz_localize(None)
    clean = clean[~clean.index.isna()].sort_index()
    return clean.pct_change(periods=lookback, fill_method=None)


def _value_at_or_before(series: pd.Series, date: pd.Timestamp) -> float | None:
    eligible = series.loc[:date].dropna()
    if eligible.empty:
        return None
    value = float(eligible.iloc[-1])
    return value if np.isfinite(value) else None


def _apply_entry_ranking(
    signal_frames: dict[str, pd.DataFrame],
    prices: dict[str, pd.DataFrame],
    benchmarks: dict[str, pd.Series] | None,
    strategy,
    config: QuantPortfolioConfig,
) -> tuple[dict[str, pd.DataFrame], str | None]:
    """Apply a portfolio-level ranking overlay without changing strategy signals."""
    if config.ranking_method == RANK_STRATEGY_SCORE:
        return signal_frames, None
    if config.ranking_method != RANK_RELATIVE_STRENGTH:
        return {}, f"Unknown entry ranking method: {config.ranking_method}"
    if strategy.definition.key != "ha_ema":
        return {}, "Relative-strength entry ranking is available only for HA + EMA Trend."
    if config.ranking_lookback_sessions <= 0:
        return {}, "Relative-strength lookback must be positive."
    benchmark = (benchmarks or {}).get(config.ranking_benchmark)
    if benchmark is None or benchmark.empty:
        return {}, f"{config.ranking_benchmark} history is unavailable for relative-strength ranking."

    benchmark_returns = _returns_over_sessions(benchmark, config.ranking_lookback_sessions)
    ranked_frames: dict[str, pd.DataFrame] = {}
    for symbol, signals in signal_frames.items():
        ranked = signals.copy()
        entries = ranked.index[ranked["Entry_Event"].astype(bool)] if not ranked.empty else []
        if len(entries):
            stock_returns = _returns_over_sessions(prices[symbol]["Close"], config.ranking_lookback_sessions)
            for index in entries:
                decision = pd.Timestamp(ranked.at[index, "Decision_Date"])
                stock_return = _value_at_or_before(stock_returns, decision)
                benchmark_return = _value_at_or_before(benchmark_returns, decision)
                diagnostics = dict(ranked.at[index, "Diagnostics"] or {})
                diagnostics.update(
                    {
                        "Ranking_Method": RANKING_LABELS[RANK_RELATIVE_STRENGTH],
                        "Ranking_Lookback_Sessions": config.ranking_lookback_sessions,
                        "Stock_Return_Pct": None if stock_return is None else 100 * stock_return,
                        "Nifty_100_Return_Pct": None if benchmark_return is None else 100 * benchmark_return,
                    }
                )
                if stock_return is None or benchmark_return is None or 1 + benchmark_return <= 0:
                    ranked.at[index, "Score"] = -np.inf
                    diagnostics["Relative_Strength_Ratio"] = None
                else:
                    relative_strength = (1 + stock_return) / (1 + benchmark_return)
                    ranked.at[index, "Score"] = relative_strength
                    diagnostics["Relative_Strength_Ratio"] = relative_strength
                ranked.at[index, "Diagnostics"] = diagnostics
        ranked_frames[symbol] = ranked
    return ranked_frames, None


def _cached_strategy_frame(symbol: str, frame: pd.DataFrame, strategy, settings: dict[str, Any]) -> pd.DataFrame:
    """Cache indicator/event work independently of capital, costs and portfolio rules."""
    final_close = float(frame["Close"].dropna().iloc[-1]) if "Close" in frame and frame["Close"].notna().any() else None
    key = (
        symbol,
        strategy.definition.key,
        strategy.definition.timeframe,
        repr(sorted(settings.items())),
        len(frame),
        frame.index.min(),
        frame.index.max(),
        final_close,
    )
    with _signal_cache_lock:
        cached = _signal_cache.get(key)
        if cached is not None:
            _signal_cache.move_to_end(key)
            return cached.copy(deep=False)
    computed = strategy.compute(frame)
    with _signal_cache_lock:
        _signal_cache[key] = computed
        _signal_cache.move_to_end(key)
        while len(_signal_cache) > _SIGNAL_CACHE_LIMIT:
            _signal_cache.popitem(last=False)
    return computed.copy(deep=False)


def _cached_strategy_frames(prices: dict[str, pd.DataFrame], strategy, settings: dict[str, Any]):
    key = (
        strategy.definition.key,
        strategy.definition.timeframe,
        repr(sorted(settings.items())),
        len(prices),
        sum(len(frame) for frame in prices.values()),
        max(frame.index.max() for frame in prices.values()),
    )
    with _signal_cache_lock:
        cached = _batch_signal_cache.get(key)
        if cached is not None:
            _batch_signal_cache.move_to_end(key)
            return cached, True
    computed = strategy.compute_many(prices)
    with _signal_cache_lock:
        _batch_signal_cache[key] = computed
        _batch_signal_cache.move_to_end(key)
        while len(_batch_signal_cache) > 4:
            _batch_signal_cache.popitem(last=False)
    return computed, False


def run_quant_portfolio(
    all_ohlcv: dict[str, pd.DataFrame],
    benchmarks: dict[str, pd.Series] | None,
    config: QuantPortfolioConfig,
    emit: Callable[[str, str], None] | None = None,
    cancel_evt: threading.Event | None = None,
) -> dict[str, Any]:
    """Run one strategy across a historical universe, executing exits before ranked entries."""
    emit = emit or (lambda _level, _message: None)
    cancel_evt = cancel_evt or threading.Event()
    if config.max_holdings <= 0 or config.initial_capital <= 0:
        return {"error": "Max holdings and initial capital must be positive."}
    strategy = build_strategy(config.strategy, config.strategy_settings)
    required_price_columns = set(strategy.definition.required_price_columns)
    schema_failures = [
        {
            "Symbol": symbol,
            "Error": f"Missing OHLCV columns: {', '.join(sorted(required_price_columns - set(frame.columns)))}",
        }
        for symbol, frame in all_ohlcv.items()
        if required_price_columns.difference(frame.columns)
    ]
    prices = {
        symbol: frame if frame.attrs.get("quant_normalized") else _normalise_prices(frame)
        for symbol, frame in all_ohlcv.items()
        if not frame.empty and not required_price_columns.difference(frame.columns)
    }
    if not prices:
        return {"error": "No OHLCV data is available."}
    emit("info", f"Computing {strategy.definition.label} signals for {len(prices):,} stocks…")

    signal_frames: dict[str, pd.DataFrame] = {}
    failures: list[dict[str, str]] = list(schema_failures)
    if hasattr(strategy, "compute_many"):
        try:
            signal_frames, cache_hit = _cached_strategy_frames(prices, strategy, config.strategy_settings)
        except Exception as exc:
            return {"error": f"Batch strategy calculation failed: {exc}"}
        prefix = "Reused" if cache_hit else "Signals ready for"
        emit("info", f"{prefix} {len(signal_frames):,}/{len(prices):,} stocks")
    else:
        # These adapters are dominated by pandas operations. Multiple Python
        # threads contend on pandas internals and are slower for this workload.
        with ThreadPoolExecutor(max_workers=1) as pool:
            futures = {
                pool.submit(_cached_strategy_frame, symbol, frame, strategy, config.strategy_settings): symbol
                for symbol, frame in prices.items()
            }
            completed = 0
            progress_step = max(100, len(futures) // 5)
            for future in as_completed(futures):
                symbol = futures[future]
                if cancel_evt.is_set():
                    return {"error": "Cancelled"}
                try:
                    signal_frames[symbol] = future.result()
                except Exception as exc:
                    failures.append({"Symbol": symbol, "Error": str(exc)})
                completed += 1
                if completed % progress_step == 0 or completed == len(futures):
                    emit("info", f"Signals ready for {completed:,}/{len(futures):,} stocks")

    signal_frames, ranking_error = _apply_entry_ranking(signal_frames, prices, benchmarks, strategy, config)
    if ranking_error:
        return {"error": ranking_error}
    if config.ranking_method == RANK_RELATIVE_STRENGTH:
        emit(
            "info",
            f"Ranking entries by {config.ranking_lookback_sessions}-session relative strength "
            f"vs {config.ranking_benchmark}…",
        )

    all_dates = pd.DatetimeIndex(sorted(set().union(*(set(frame.index) for frame in prices.values()))))
    if all_dates.empty:
        return {"error": "No valid dated OHLCV rows are available."}
    start = pd.Timestamp(config.start_date) if config.start_date else all_dates.min()
    end = pd.Timestamp(config.end_date) if config.end_date else all_dates.max()
    calendar = all_dates[(all_dates >= start) & (all_dates <= end)]
    if len(calendar) < 2:
        return {"error": "The selected date range has insufficient trading sessions."}

    comp = _prepare_compositions(config.compositions_df, config.index_names)
    universe_cache: dict[pd.Timestamp, set[str] | None] = {}
    emit("info", "Ranking signals and replaying fixed-slot portfolio events…")
    events: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    decisions: list[dict[str, Any]] = []
    for symbol, frame in signal_frames.items():
        for row in frame.itertuples(index=False):
            if not (row.Entry_Event or row.Exit_Event):
                continue
            event = "BUY" if row.Entry_Event else "EXIT"
            record = {
                "Symbol": symbol,
                "Decision_Date": pd.Timestamp(row.Decision_Date),
                "Execution_Date": pd.Timestamp(row.Execution_Date) if pd.notna(row.Execution_Date) else pd.NaT,
                "Event": event,
                "Score": float(row.Score),
                "Reason": row.Entry_Reason if event == "BUY" else row.Exit_Reason,
                "Ready": bool(row.Ready),
                "Diagnostics": row.Diagnostics,
                "Outcome": "PENDING",
            }
            executable = pd.notna(record["Execution_Date"]) and start <= record["Execution_Date"] <= end
            pending_in_range = pd.isna(record["Execution_Date"]) and start <= record["Decision_Date"] <= end
            if not (executable or pending_in_range):
                continue
            decisions.append(record)
            if executable:
                events.setdefault(record["Execution_Date"], []).append(record)

    cash = float(config.initial_capital)
    holdings: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    holdings_log: list[dict[str, Any]] = []
    total_costs = total_taxes = 0.0
    slot_fraction = 1.0 / config.max_holdings
    realized: dict[int, dict[str, float]] = {}
    cf_st: list[tuple[int, float]] = []
    cf_lt: list[tuple[int, float]] = []
    settled_fys: set[int] = set()
    action_log: list[dict[str, Any]] = []
    corporate_actions = load_corporate_actions()

    def settle_tax(fy: int) -> float:
        nonlocal cash, total_taxes, cf_st, cf_lt
        if fy in settled_fys:
            return 0.0
        ledger = realized.get(fy, {})
        tax, cf_st, cf_lt = _compute_fy_tax(
            fy,
            ledger.get("st_gains", 0.0),
            ledger.get("st_losses", 0.0),
            ledger.get("lt_gains", 0.0),
            ledger.get("lt_losses", 0.0),
            cf_st,
            cf_lt,
            config.stcg_rate,
            config.ltcg_rate,
        )
        cash -= tax
        total_taxes += tax
        settled_fys.add(fy)
        return tax

    def marked_value(date: pd.Timestamp) -> tuple[float, dict[str, float]]:
        values = {}
        for symbol, position in holdings.items():
            history = prices[symbol].loc[:date, "Close"].dropna()
            if not history.empty:
                values[symbol] = position["Shares"] * float(history.iloc[-1])
        return cash + sum(values.values()), values

    previous_fy: int | None = None
    for date in calendar:
        if cancel_evt.is_set():
            return {"error": "Cancelled"}
        current_fy = _financial_year(date)
        if previous_fy is not None and current_fy != previous_fy:
            settle_tax(previous_fy)
        previous_fy = current_fy

        for action in corporate_actions:
            if action["effective_date"] != date or action["old_symbol"] not in holdings:
                continue
            old_symbol, successor = action["old_symbol"], action["successor_symbol"]
            if successor not in prices:
                action_log.append(
                    {
                        "Date": date,
                        "Event": action["event_type"],
                        "Old_Symbol": old_symbol,
                        "Successor_Symbol": successor,
                        "Share_Ratio": action["share_ratio"],
                        "Status": "SKIPPED_SUCCESSOR_PRICE_MISSING",
                    }
                )
                continue
            old = holdings.pop(old_symbol)
            converted_shares = old["Shares"] * action["share_ratio"]
            if successor in holdings:
                existing = holdings[successor]
                combined_shares = existing["Shares"] + converted_shares
                existing["Entry_Price"] = (existing["Cost_Basis"] + old["Cost_Basis"]) / combined_shares
                existing["Shares"] = combined_shares
                existing["Cost_Basis"] += old["Cost_Basis"]
                existing["Entry_Date"] = min(existing["Entry_Date"], old["Entry_Date"])
            else:
                holdings[successor] = {
                    **old,
                    "Shares": converted_shares,
                    "Entry_Price": old["Cost_Basis"] / converted_shares,
                    "Entry_Reason": f"Corporate action from {old_symbol}",
                }
            action_log.append(
                {
                    "Date": date,
                    "Event": action["event_type"],
                    "Old_Symbol": old_symbol,
                    "Successor_Symbol": successor,
                    "Share_Ratio": action["share_ratio"],
                    "Status": "APPLIED",
                }
            )
        todays = events.get(date, [])
        traded_value = 0.0
        exits = sorted((row for row in todays if row["Event"] == "EXIT"), key=lambda row: row["Symbol"])
        entries = sorted(
            (row for row in todays if row["Event"] == "BUY"), key=lambda row: (-row["Score"], row["Symbol"])
        )
        for event in exits:
            position = holdings.get(event["Symbol"])
            execution_price = _price(prices[event["Symbol"]], date, "Open")
            if position is None:
                event["Outcome"] = "SKIPPED_NOT_HELD"
                continue
            if execution_price is None:
                event["Outcome"] = "SKIPPED_NO_PRICE"
                continue
            gross = position["Shares"] * execution_price
            traded_value += gross
            cost = gross * config.transaction_cost_pct + config.brokerage_per_order
            pnl_before_tax = gross - cost - position["Cost_Basis"]
            holding_days = (date - position["Entry_Date"]).days
            category = "lt" if holding_days >= 365 else "st"
            bucket = realized.setdefault(_financial_year(date), {})
            ledger_key = f"{category}_{'gains' if pnl_before_tax >= 0 else 'losses'}"
            bucket[ledger_key] = bucket.get(ledger_key, 0.0) + abs(pnl_before_tax)
            cash += gross - cost
            total_costs += cost
            trades.append(
                {
                    "Symbol": event["Symbol"],
                    "Entry_Date": position["Entry_Date"],
                    "Exit_Date": date,
                    "Entry_Price": position["Entry_Price"],
                    "Exit_Price": execution_price,
                    "Shares": position["Shares"],
                    "PnL": pnl_before_tax,
                    "Return_Pct": 100 * pnl_before_tax / position["Cost_Basis"],
                    "Holding_Days": holding_days,
                    "Exit_Reason": event["Reason"],
                    "Status": "CLOSED",
                }
            )
            del holdings[event["Symbol"]]
            event["Outcome"] = "EXECUTED"

        portfolio_value, _ = marked_value(date)
        for event in entries:
            symbol = event["Symbol"]
            if symbol in holdings:
                event["Outcome"] = "SKIPPED_ALREADY_HELD"
                continue
            if not np.isfinite(event["Score"]):
                event["Outcome"] = "SKIPPED_RANKING_DATA"
                continue
            decision_date = event["Decision_Date"]
            if decision_date not in universe_cache:
                universe_cache[decision_date] = _valid_symbols_at_date(comp, config.index_names, decision_date)
            eligible_universe = universe_cache[decision_date]
            if eligible_universe is not None and symbol not in eligible_universe:
                event["Outcome"] = "SKIPPED_UNIVERSE"
                continue
            history = prices[symbol].loc[: event["Decision_Date"]]
            if len(history) < config.min_history_days:
                event["Outcome"] = "SKIPPED_HISTORY"
                continue
            if strategy.definition.uses_liquidity_filter:
                volume = history["Volume"].tail(20).median() if "Volume" in history else np.nan
                if pd.isna(volume) or volume < config.minimum_median_volume:
                    event["Outcome"] = "SKIPPED_LIQUIDITY"
                    continue
            if len(holdings) >= config.max_holdings:
                event["Outcome"] = "SKIPPED_FULL"
                continue
            execution_price = _price(prices[symbol], date, "Open")
            if execution_price is None:
                event["Outcome"] = "SKIPPED_NO_PRICE"
                continue
            budget = min(cash, portfolio_value * slot_fraction)
            shares = int((budget - config.brokerage_per_order) / (execution_price * (1 + config.transaction_cost_pct)))
            if shares <= 0:
                event["Outcome"] = "SKIPPED_CASH"
                continue
            gross = shares * execution_price
            traded_value += gross
            cost = gross * config.transaction_cost_pct + config.brokerage_per_order
            cash -= gross + cost
            total_costs += cost
            holdings[symbol] = {
                "Shares": shares,
                "Entry_Date": date,
                "Entry_Price": execution_price,
                "Cost_Basis": gross + cost,
                "Entry_Reason": event["Reason"],
            }
            event["Outcome"] = "EXECUTED"

        value, values = marked_value(date)
        invested = sum(values.values())
        max_weight = 100 * max(values.values()) / value if values and value else 0.0
        daily_rows.append(
            {
                "Date": date,
                "Portfolio_Value": value,
                "Cash": cash,
                "Invested": invested,
                "Market_Exposure_Pct": 100 * invested / value if value else 0.0,
                "Holdings": len(holdings),
                "Max_Weight_Pct": max_weight,
                "Traded_Value": traded_value,
                "Turnover_Pct": 100 * traded_value / value if value else 0.0,
            }
        )
        holdings_log.append(
            {"Date": date, "Holdings": sorted(holdings), "Weights": {s: v / value for s, v in values.items()}}
        )

    if previous_fy is not None:
        final_tax = settle_tax(previous_fy)
        if final_tax and daily_rows:
            daily_rows[-1]["Cash"] -= final_tax
            daily_rows[-1]["Portfolio_Value"] -= final_tax
            final_row_value = daily_rows[-1]["Portfolio_Value"]
            daily_rows[-1]["Market_Exposure_Pct"] = (
                100 * daily_rows[-1]["Invested"] / final_row_value if final_row_value else 0.0
            )
            if holdings_log and final_row_value:
                last_marks = marked_value(calendar[-1])[1]
                holdings_log[-1]["Weights"] = {symbol: value / final_row_value for symbol, value in last_marks.items()}

    final_date = calendar[-1]
    final_value, final_marks = marked_value(final_date)
    for symbol, position in holdings.items():
        mark = final_marks.get(symbol, position["Entry_Price"] * position["Shares"]) / position["Shares"]
        gross = position["Shares"] * mark
        trades.append(
            {
                "Symbol": symbol,
                "Entry_Date": position["Entry_Date"],
                "Exit_Date": pd.NaT,
                "Entry_Price": position["Entry_Price"],
                "Exit_Price": mark,
                "Shares": position["Shares"],
                "PnL": gross - position["Cost_Basis"],
                "Return_Pct": 100 * (gross - position["Cost_Basis"]) / position["Cost_Basis"],
                "Holding_Days": (final_date - position["Entry_Date"]).days,
                "Exit_Reason": "Open at backtest end",
                "Status": "OPEN",
            }
        )

    daily = pd.DataFrame(daily_rows).set_index("Date")
    nav = pd.DataFrame(index=daily.index)
    nav["Quant Portfolio"] = daily["Portfolio_Value"] / config.initial_capital * 100
    for name, series in (benchmarks or {}).items():
        aligned = pd.Series(series).reindex(nav.index).ffill().dropna()
        if not aligned.empty:
            nav[name] = aligned / aligned.iloc[0] * 100
    decision_df = pd.DataFrame(
        decisions,
        columns=[
            "Symbol",
            "Decision_Date",
            "Execution_Date",
            "Event",
            "Score",
            "Reason",
            "Ready",
            "Diagnostics",
            "Outcome",
        ],
    )
    trade_df = pd.DataFrame(
        trades,
        columns=[
            "Symbol",
            "Entry_Date",
            "Exit_Date",
            "Entry_Price",
            "Exit_Price",
            "Shares",
            "PnL",
            "Return_Pct",
            "Holding_Days",
            "Exit_Reason",
            "Status",
        ],
    )
    stats, metrics = build_quant_metrics(
        nav, daily, trade_df, decision_df, config.initial_capital, total_costs, total_taxes, config.max_holdings
    )
    exits = decision_df[decision_df["Event"].eq("EXIT")] if not decision_df.empty else decision_df
    emit("success", f"Portfolio replay complete · {len(calendar):,} sessions · {len(trade_df):,} trades")
    return {
        "nav": nav,
        "stats": stats,
        "metrics": metrics,
        "daily": daily,
        "trades": trade_df,
        "decision_log": decision_df,
        "holdings_log": holdings_log,
        "strategy": strategy_metadata(strategy),
        "ranking": {
            "method": config.ranking_method,
            "label": RANKING_LABELS.get(config.ranking_method, config.ranking_method),
            "lookback_sessions": config.ranking_lookback_sessions,
            "benchmark": config.ranking_benchmark if config.ranking_method == RANK_RELATIVE_STRENGTH else None,
        },
        "data_failures": pd.DataFrame(failures),
        "exit_reason_breakdown": (
            exits["Reason"].value_counts().rename_axis("Reason").to_frame("Count")
            if not exits.empty
            else pd.DataFrame()
        ),
        "total_costs": total_costs,
        "total_taxes": total_taxes,
        "final_value": final_value,
        "trading_days": list(calendar),
        "signal_failures": len(failures),
        "corporate_actions": pd.DataFrame(action_log),
    }
