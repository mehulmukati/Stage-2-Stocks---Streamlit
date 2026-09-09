"""Metrics for event-driven, fixed-slot quant portfolios."""

from __future__ import annotations

import numpy as np
import pandas as pd

from backtest_engine import _compute_summary_stats

QUANT_PORTFOLIO_METRICS_VERSION = 2


def slice_and_rebase_nav(
    nav: pd.DataFrame,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Slice NAV to a display period and independently rebase each series to 100."""
    period = nav.loc[pd.Timestamp(start) : pd.Timestamp(end)].copy()
    rebased = period.copy()
    for column in period.columns:
        valid = period[column].dropna()
        if valid.empty or valid.iloc[0] == 0:
            rebased[column] = np.nan
            continue
        rebased[column] = period[column].div(valid.iloc[0]).mul(100.0)
    return period, rebased


def build_quant_metrics(
    nav: pd.DataFrame,
    daily: pd.DataFrame,
    trades: pd.DataFrame,
    decisions: pd.DataFrame,
    initial_capital: float,
    costs: float,
    taxes: float,
    max_slots: int,
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    stats = _compute_summary_stats(nav)
    portfolio_name = "Quant Portfolio"
    completed = trades[trades["Status"].eq("CLOSED")] if not trades.empty else trades
    returns = completed["Return_Pct"] if not completed.empty else pd.Series(dtype=float)
    winners, losers = returns[returns > 0], returns[returns <= 0]
    gross_profit = completed.loc[completed["PnL"] > 0, "PnL"].sum() if not completed.empty else 0.0
    gross_loss = -completed.loc[completed["PnL"] < 0, "PnL"].sum() if not completed.empty else 0.0
    entries = decisions[decisions["Event"].eq("BUY")] if not decisions.empty else decisions
    executed = entries[entries["Outcome"].eq("EXECUTED")] if not entries.empty else entries
    exposure = float(daily["Market_Exposure_Pct"].mean()) if not daily.empty else 0.0
    active_turnover = (
        daily.loc[daily["Turnover_Pct"] > 0, "Turnover_Pct"] if not daily.empty else pd.Series(dtype=float)
    )
    candidate_counts = (
        entries.dropna(subset=["Execution_Date"]).groupby("Execution_Date").size()
        if not entries.empty
        else pd.Series(dtype=float)
    )
    portfolio_returns = nav[portfolio_name].pct_change().fillna(0.0)
    exposure_fraction = daily["Market_Exposure_Pct"].shift(1).div(100).replace(0, np.nan)
    invested_returns = portfolio_returns.div(exposure_fraction).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    invested_nav = (1 + invested_returns).cumprod() * 100
    quant = {
        "Trading Days": len(daily),
        "Signals": len(entries),
        "Executed Entries": len(executed),
        "Skipped · Portfolio Full": int(entries["Outcome"].eq("SKIPPED_FULL").sum()) if not entries.empty else 0,
        "Skipped · No Price": int(entries["Outcome"].eq("SKIPPED_NO_PRICE").sum()) if not entries.empty else 0,
        "Skipped · Ranking Data": (
            int(entries["Outcome"].eq("SKIPPED_RANKING_DATA").sum()) if not entries.empty else 0
        ),
        "Pending at End": int(entries["Outcome"].eq("PENDING").sum()) if not entries.empty else 0,
        "Signal Conversion (%)": 100 * len(executed) / len(entries) if len(entries) else 0.0,
        "Market Exposure (%)": exposure,
        "Average Cash (%)": 100 - exposure,
        "Capacity Utilization (%)": 100 * float(daily["Holdings"].mean()) / max_slots if not daily.empty else 0.0,
        "Fixed Slots": max_slots,
        "Average Holdings": float(daily["Holdings"].mean()) if not daily.empty else 0.0,
        "Max Holdings": int(daily["Holdings"].max()) if not daily.empty else 0,
        "Maximum Same-session Candidates": int(candidate_counts.max()) if not candidate_counts.empty else 0,
        "Average Turnover · Active Session (%)": float(active_turnover.mean()) if not active_turnover.empty else 0.0,
        "Annualized Turnover (%)": (
            100 * daily["Traded_Value"].sum() / daily["Portfolio_Value"].mean() * 252 / len(daily)
            if not daily.empty and daily["Portfolio_Value"].mean()
            else 0.0
        ),
        "Completed Trades": len(completed),
        "Win Rate (%)": 100 * len(winners) / len(returns) if len(returns) else 0.0,
        "Average Winner (%)": float(winners.mean()) if len(winners) else 0.0,
        "Average Loser (%)": float(losers.mean()) if len(losers) else 0.0,
        "Payoff Ratio": (
            abs(float(winners.mean() / losers.mean())) if len(winners) and len(losers) and losers.mean() else np.nan
        ),
        "Profit Factor": float(gross_profit / gross_loss) if gross_loss else np.nan,
        "Best Trade (%)": float(returns.max()) if len(returns) else 0.0,
        "Worst Trade (%)": float(returns.min()) if len(returns) else 0.0,
        "Average Trade (%)": float(returns.mean()) if len(returns) else 0.0,
        "Average Holding Days": float(completed["Holding_Days"].mean()) if not completed.empty else 0.0,
        "Median Holding Days": float(completed["Holding_Days"].median()) if not completed.empty else 0.0,
        "Cost Drag (%)": 100 * costs / initial_capital,
        "Tax Drag (%)": 100 * taxes / initial_capital,
        "Max Concentration (%)": float(daily["Max_Weight_Pct"].max()) if not daily.empty else 0.0,
        "Invested-period Final NAV": float(invested_nav.iloc[-1]) if not invested_nav.empty else 100.0,
    }
    if portfolio_name in stats.index:
        cagr = float(stats.at[portfolio_name, "CAGR (%)"])
        quant["Exposure-adjusted CAGR (%)"] = cagr / (exposure / 100) if exposure else np.nan
        stats.loc[portfolio_name, "Avg Holdings"] = round(quant["Average Holdings"], 2)
        stats.loc[portfolio_name, "Cost Drag (%)"] = round(quant["Cost Drag (%)"], 2)
        stats.loc[portfolio_name, "Tax Drag (%)"] = round(quant["Tax Drag (%)"], 2)
        stats.loc[portfolio_name, "Market Exposure (%)"] = round(exposure, 2)
    return stats, quant
