"""Strategy-neutral next-open execution and single-stock cash replay."""

from __future__ import annotations

from dataclasses import dataclass
from math import floor
from typing import Any

import numpy as np
import pandas as pd

STRATEGY_REPLAY_VERSION = 1


@dataclass(frozen=True)
class ReplayConfig:
    """Execution and tax assumptions for an illustrative single-stock replay."""

    initial_capital: float = 100_000.0
    transaction_cost_pct: float = 0.001
    brokerage_per_order: float = 15.0
    stcg_rate: float = 0.20
    ltcg_rate: float = 0.125
    ltcg_exemption_per_fy: float = 0.0


@dataclass(frozen=True)
class ExecutionPolicy:
    """Describe how a completed-bar signal maps to a later opening fill."""

    next_bar_offset: int = 1
    price_column: str = "Open"
    execution_date_column: str | None = "Execution_Date"


def resolve_signal_executions(
    signals: pd.DataFrame,
    policy: ExecutionPolicy | None = None,
) -> pd.DataFrame:
    """Return authoritative next-open fills for every executable BUY/EXIT signal."""

    policy = policy or ExecutionPolicy()
    if policy.next_bar_offset <= 0:
        raise ValueError("Execution offset must be positive")
    required = {"Signal", policy.price_column}
    missing = required.difference(signals.columns)
    if missing:
        raise ValueError(f"Missing replay columns: {', '.join(sorted(missing))}")

    rows: list[dict[str, Any]] = []
    signal_positions = np.flatnonzero(signals["Signal"].isin(["BUY", "EXIT"]).to_numpy())
    for signal_position in signal_positions:
        execution_position = int(signal_position) + policy.next_bar_offset
        if execution_position >= len(signals):
            continue
        execution_row = signals.iloc[execution_position]
        execution_index = pd.Timestamp(signals.index[execution_position])
        execution_date = execution_index
        if policy.execution_date_column and policy.execution_date_column in signals:
            candidate = execution_row[policy.execution_date_column]
            if pd.notna(candidate):
                execution_date = pd.Timestamp(candidate)
        rows.append(
            {
                "Signal_Position": int(signal_position),
                "Signal": str(signals["Signal"].iloc[signal_position]),
                "Signal_Date": pd.Timestamp(signals.index[signal_position]),
                "Execution_Position": execution_position,
                "Execution_Index": execution_index,
                "Execution_Date": execution_date,
                "Execution_Price": float(execution_row[policy.price_column]),
            }
        )
    return pd.DataFrame(rows)


def _empty_replay(reason: str, unexecuted_signal: str | None = None) -> dict[str, Any]:
    return {
        "equity": pd.DataFrame(columns=["Pre_Tax_Value", "Post_Tax_Realised_Value", "Buy_Hold_Value", "Invested"]),
        "trades": pd.DataFrame(),
        "summary": {
            "comparison_available": False,
            "reason": reason,
            "unexecuted_signal": unexecuted_signal,
        },
    }


def _order_cost(gross_value: float, config: ReplayConfig) -> float:
    return gross_value * config.transaction_cost_pct + config.brokerage_per_order


def _financial_year(date: pd.Timestamp) -> int:
    return date.year if date.month >= 4 else date.year - 1


def _estimated_tax(trades: list[dict[str, Any]], config: ReplayConfig) -> float:
    """Estimate strategy-only Indian CGT with FY netting and loss carry-forward."""

    closed = [trade for trade in trades if trade.get("Exit Date") is not None]
    if not closed:
        return 0.0
    by_fy: dict[int, list[dict[str, Any]]] = {}
    for trade in closed:
        by_fy.setdefault(_financial_year(pd.Timestamp(trade["Exit Date"])), []).append(trade)

    carry_st = 0.0
    carry_lt = 0.0
    total_tax = 0.0
    for _, fy_trades in sorted(by_fy.items()):
        net_st = sum(float(trade["P&L"]) for trade in fy_trades if trade["Tax Class"] == "STCG")
        net_lt = sum(float(trade["P&L"]) for trade in fy_trades if trade["Tax Class"] == "LTCG")

        if net_st < 0:
            loss = -net_st
            net_st = 0.0
            offset = min(loss, max(net_lt, 0.0))
            net_lt -= offset
            carry_st += loss - offset
        if net_lt < 0:
            carry_lt += -net_lt
            net_lt = 0.0

        offset = min(carry_st, max(net_st, 0.0))
        net_st -= offset
        carry_st -= offset
        offset = min(carry_st, max(net_lt, 0.0))
        net_lt -= offset
        carry_st -= offset
        offset = min(carry_lt, max(net_lt, 0.0))
        net_lt -= offset
        carry_lt -= offset

        taxable_lt = max(0.0, net_lt - config.ltcg_exemption_per_fy)
        total_tax += max(0.0, net_st) * config.stcg_rate + taxable_lt * config.ltcg_rate
    return total_tax


def _trade_record(
    open_trade: dict[str, Any],
    exit_signal_date: pd.Timestamp,
    exit_date: pd.Timestamp,
    exit_price: float,
    exit_cost: float,
    net_proceeds: float,
) -> dict[str, Any]:
    pnl = net_proceeds - float(open_trade["Entry Outlay"])
    holding_days = int((exit_date - pd.Timestamp(open_trade["Entry Date"])).days)
    return {
        **open_trade,
        "Exit Signal": exit_signal_date,
        "Exit Date": exit_date,
        "Exit Price": exit_price,
        "Exit Cost": exit_cost,
        "Net Proceeds": net_proceeds,
        "Holding Days": holding_days,
        "P&L": pnl,
        "Return %": pnl / float(open_trade["Entry Outlay"]) * 100.0,
        "Tax Class": "LTCG" if holding_days > 365 else "STCG",
        "Status": "Closed",
    }


def replay_single_stock(
    signals: pd.DataFrame,
    config: ReplayConfig | None = None,
    execution_policy: ExecutionPolicy | None = None,
) -> dict[str, Any]:
    """Replay position-aware signals with a configurable following-bar Open policy."""

    config = config or ReplayConfig()
    policy = execution_policy or ExecutionPolicy()
    if config.initial_capital <= 0:
        raise ValueError("Initial capital must be positive")
    if config.transaction_cost_pct < 0 or config.brokerage_per_order < 0:
        raise ValueError("Trading costs cannot be negative")
    if min(config.stcg_rate, config.ltcg_rate, config.ltcg_exemption_per_fy) < 0:
        raise ValueError("Tax settings cannot be negative")
    if max(config.stcg_rate, config.ltcg_rate) > 1:
        raise ValueError("Tax rates must be decimal fractions between zero and one")
    if signals.empty:
        return _empty_replay("No price history is available.")

    executions = resolve_signal_executions(signals, policy)
    executable_buys = executions[executions["Signal"].eq("BUY")] if not executions.empty else pd.DataFrame()
    if executable_buys.empty:
        final_signal = str(signals["Signal"].iloc[-1])
        return _empty_replay(
            "No BUY signal has a following bar available for execution.",
            final_signal if final_signal == "BUY" else None,
        )

    first_buy = executable_buys.iloc[0]
    first_buy_position = int(first_buy["Signal_Position"])
    comparison_start_index = pd.Timestamp(first_buy["Execution_Index"])
    comparison_start_date = pd.Timestamp(first_buy["Execution_Date"])
    first_execution_price = float(first_buy["Execution_Price"])
    affordable_at_start = floor(
        max(0.0, config.initial_capital - config.brokerage_per_order)
        / (first_execution_price * (1.0 + config.transaction_cost_pct))
    )
    if affordable_at_start <= 0:
        return _empty_replay(
            "Initial capital is insufficient to buy one share at the first execution price.",
            "BUY",
        )

    signals = signals.iloc[first_buy_position:].copy()
    executions = resolve_signal_executions(signals, policy)
    execution_by_position = {int(event["Execution_Position"]): event for _, event in executions.iterrows()}

    cash = float(config.initial_capital)
    shares = 0
    open_trade: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []

    for position, (date, row) in enumerate(signals.iterrows()):
        date = pd.Timestamp(date)
        event = execution_by_position.get(position)
        if event is not None:
            action = str(event["Signal"])
            signal_date = pd.Timestamp(event["Signal_Date"])
            execution_price = float(event["Execution_Price"])
            execution_date = pd.Timestamp(event["Execution_Date"])
            if action == "BUY" and shares == 0:
                affordable = floor(
                    max(0.0, cash - config.brokerage_per_order)
                    / (execution_price * (1.0 + config.transaction_cost_pct))
                )
                if affordable > 0:
                    gross = affordable * execution_price
                    entry_cost = _order_cost(gross, config)
                    entry_outlay = gross + entry_cost
                    cash -= entry_outlay
                    shares = affordable
                    open_trade = {
                        "Entry Signal": signal_date,
                        "Entry Date": execution_date,
                        "Entry Price": execution_price,
                        "Shares": shares,
                        "Entry Cost": entry_cost,
                        "Entry Outlay": entry_outlay,
                    }
            elif action == "EXIT" and shares > 0 and open_trade is not None:
                gross = shares * execution_price
                exit_cost = _order_cost(gross, config)
                net_proceeds = gross - exit_cost
                cash += net_proceeds
                trades.append(
                    _trade_record(open_trade, signal_date, execution_date, execution_price, exit_cost, net_proceeds)
                )
                shares = 0
                open_trade = None

        market_value = shares * float(row["Close"])
        pre_tax_value = cash + market_value
        tax_liability = _estimated_tax(trades, config)
        equity_rows.append(
            {
                "Date": date,
                "Pre_Tax_Value": pre_tax_value,
                "Post_Tax_Realised_Value": pre_tax_value - tax_liability,
                "Invested": shares > 0,
            }
        )

    final_date = pd.Timestamp(signals.index[-1])
    final_close = float(signals["Close"].iloc[-1])
    realised_tax = _estimated_tax(trades, config)
    mark_to_market = cash + shares * final_close
    liquidation_trade = None
    liquidation_value = mark_to_market
    liquidation_tax = realised_tax
    if shares > 0 and open_trade is not None:
        gross = shares * final_close
        exit_cost = _order_cost(gross, config)
        net_proceeds = gross - exit_cost
        liquidation_trade = _trade_record(open_trade, final_date, final_date, final_close, exit_cost, net_proceeds)
        liquidation_trade["Status"] = "Hypothetical end-date liquidation"
        liquidation_value = cash + net_proceeds
        liquidation_tax = _estimated_tax([*trades, liquidation_trade], config)

    ledger_rows = [*trades]
    if open_trade is not None:
        unrealised_pnl = shares * final_close - float(open_trade["Entry Outlay"])
        ledger_rows.append(
            {
                **open_trade,
                "Exit Signal": None,
                "Exit Date": None,
                "Exit Price": None,
                "Exit Cost": None,
                "Net Proceeds": None,
                "Holding Days": int((final_date - pd.Timestamp(open_trade["Entry Date"])).days),
                "P&L": unrealised_pnl,
                "Return %": unrealised_pnl / float(open_trade["Entry Outlay"]) * 100.0,
                "Tax Class": "Unrealised",
                "Status": "Open",
            }
        )

    equity = pd.DataFrame(equity_rows).set_index("Date").loc[comparison_start_index:]
    running_peak = equity["Pre_Tax_Value"].cummax()
    drawdown = equity["Pre_Tax_Value"] / running_peak - 1.0
    elapsed_years = max((final_date - comparison_start_date).days / 365.25, 1 / 365.25)
    after_tax_liquidation = liquidation_value - liquidation_tax
    buy_hold_shares = floor(
        max(0.0, config.initial_capital - config.brokerage_per_order)
        / (first_execution_price * (1.0 + config.transaction_cost_pct))
    )
    buy_hold_entry_gross = buy_hold_shares * first_execution_price
    buy_hold_entry_cost = _order_cost(buy_hold_entry_gross, config) if buy_hold_shares else 0.0
    buy_hold_outlay = buy_hold_entry_gross + buy_hold_entry_cost
    buy_hold_cash = config.initial_capital - buy_hold_outlay
    equity["Buy_Hold_Value"] = buy_hold_cash + buy_hold_shares * signals.loc[equity.index, "Close"].astype(float)
    buy_hold_exit_gross = buy_hold_shares * final_close
    buy_hold_exit_cost = _order_cost(buy_hold_exit_gross, config) if buy_hold_shares else 0.0
    buy_hold_proceeds = buy_hold_exit_gross - buy_hold_exit_cost
    buy_hold_value = buy_hold_cash + buy_hold_proceeds
    buy_hold_trade = {
        "Exit Date": final_date,
        "P&L": buy_hold_proceeds - buy_hold_outlay,
        "Tax Class": "LTCG" if (final_date - comparison_start_date).days > 365 else "STCG",
    }
    buy_hold_tax = _estimated_tax([buy_hold_trade], config) if buy_hold_shares else 0.0
    buy_hold_after_tax_value = buy_hold_value - buy_hold_tax
    closed_returns = [float(trade["Return %"]) for trade in trades]
    resolved_signal_positions = set(executions["Signal_Position"].astype(int)) if not executions.empty else set()
    pending_signals = [
        str(signals["Signal"].iloc[position])
        for position in range(len(signals))
        if str(signals["Signal"].iloc[position]) in {"BUY", "EXIT"} and position not in resolved_signal_positions
    ]
    summary = {
        "comparison_available": True,
        "comparison_start_date": comparison_start_date,
        "first_buy_signal_date": pd.Timestamp(signals.index[0]),
        "initial_capital": config.initial_capital,
        "mark_to_market_value": mark_to_market,
        "pre_tax_liquidation_value": liquidation_value,
        "after_tax_liquidation_value": after_tax_liquidation,
        "pre_tax_return_pct": (liquidation_value / config.initial_capital - 1.0) * 100.0,
        "after_tax_return_pct": (after_tax_liquidation / config.initial_capital - 1.0) * 100.0,
        "estimated_realised_tax": realised_tax,
        "estimated_liquidation_tax": liquidation_tax,
        "open_position": shares > 0,
        "open_shares": shares,
        "unexecuted_signal": pending_signals[-1] if pending_signals else None,
        "closed_trades": len(trades),
        "win_rate_pct": (
            sum(value > 0 for value in closed_returns) / len(closed_returns) * 100.0 if closed_returns else None
        ),
        "exposure_pct": float(equity["Invested"].mean() * 100.0),
        "max_drawdown_pct": float(drawdown.min() * 100.0),
        "pre_tax_cagr_pct": ((liquidation_value / config.initial_capital) ** (1.0 / elapsed_years) - 1.0) * 100.0,
        "after_tax_cagr_pct": ((after_tax_liquidation / config.initial_capital) ** (1.0 / elapsed_years) - 1.0) * 100.0,
        "buy_hold_pre_tax_value": buy_hold_value,
        "buy_hold_pre_tax_return_pct": (buy_hold_value / config.initial_capital - 1.0) * 100.0,
        "buy_hold_after_tax_value": buy_hold_after_tax_value,
        "buy_hold_after_tax_return_pct": (buy_hold_after_tax_value / config.initial_capital - 1.0) * 100.0,
        "liquidation_trade": liquidation_trade,
    }
    return {"equity": equity, "trades": pd.DataFrame(ledger_rows), "summary": summary}
