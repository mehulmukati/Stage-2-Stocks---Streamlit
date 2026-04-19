import numpy as np
import pandas as pd
from config import CIRCUIT_LEVELS, CIRCUIT_TOLERANCE


def _count_circuits(df: pd.DataFrame) -> int:
    """Count circuit-breaker closes (upper or lower) over the full history of df."""
    if len(df) < 2:
        return 0
    pct_change = df["Close"].pct_change() * 100
    circuit_count = 0
    for level in CIRCUIT_LEVELS:
        upper = (pct_change >= level - CIRCUIT_TOLERANCE) & (pct_change <= level + CIRCUIT_TOLERANCE)
        lower = (pct_change <= -level - CIRCUIT_TOLERANCE) & (pct_change >= -level + CIRCUIT_TOLERANCE)
        circuit_count += (upper | lower).sum()
    return int(circuit_count)


def _calculate_sharpe(df: pd.DataFrame, period_days: int) -> float | None:
    """Return annualized Sharpe ratio over the last period_days trading days, or None if insufficient data."""
    if len(df) < period_days:
        return None
    subset = df.tail(period_days)
    daily_returns = subset["Close"].pct_change().dropna()
    if len(daily_returns) == 0 or daily_returns.std() == 0:
        return None
    total_return = (subset["Close"].iloc[-1] / subset["Close"].iloc[0]) - 1
    trading_days_in_year = 252
    annualized_roc = ((1 + total_return) ** (trading_days_in_year / len(daily_returns))) - 1
    annualized_sd = daily_returns.std() * np.sqrt(trading_days_in_year)
    if annualized_sd == 0:
        return None
    return annualized_roc / annualized_sd


def _calculate_positive_days_pct(df: pd.DataFrame, months: int) -> float | None:
    """Return the percentage of up-close days over the last N months (approx 21 trading days/month)."""
    days_approx = int(months * 21)
    if len(df) < days_approx:
        return None
    subset = df.tail(days_approx)
    positive_days = (subset["Close"].diff() > 0).sum()
    total_days = len(subset) - 1
    if total_days == 0:
        return None
    return (positive_days / total_days) * 100


def score_momentum(df: pd.DataFrame) -> dict | None:
    """Compute momentum metrics (Sharpe, volatility, 52w stats, circuit count) for a single stock."""
    if len(df) < 250:
        return None

    c = df["Close"]
    v = df["Volume"]
    h = df["High"]

    close = c.iloc[-1]
    high_52w = h.rolling(252).max().iloc[-1]
    dma100 = c.rolling(100).mean().iloc[-1]
    dma200 = c.rolling(200).mean().iloc[-1]
    vol_median = v.rolling(252).median().iloc[-1]

    one_yr_change = ((c.iloc[-1] / c.iloc[-252]) - 1) * 100 if len(c) >= 252 else None
    pct_from_52w_high = ((close - high_52w) / high_52w) * 100 if high_52w else None
    circuit_count = _count_circuits(df)

    sharpe_3m = _calculate_sharpe(df, 63)
    sharpe_6m = _calculate_sharpe(df, 126)
    sharpe_9m = _calculate_sharpe(df, 189)
    sharpe_1y = _calculate_sharpe(df, 252)

    daily_returns = c.pct_change().dropna()
    volatility = daily_returns.std() * np.sqrt(252) if len(daily_returns) > 0 else None

    pos_days_3m = _calculate_positive_days_pct(df, 3)
    pos_days_6m = _calculate_positive_days_pct(df, 6)
    pos_days_12m = _calculate_positive_days_pct(df, 12)

    return {
        "Close": round(close, 2),
        "52w_High": round(high_52w, 2) if high_52w else None,
        "DMA100": round(dma100, 2),
        "DMA200": round(dma200, 2),
        "Vol_Median": int(vol_median) if vol_median else None,
        "1Y_Change": round(one_yr_change, 2) if one_yr_change else None,
        "Pct_From_52W_High": round(pct_from_52w_high, 2) if pct_from_52w_high else None,
        "Circuit_Count": circuit_count,
        "Sharpe_3M": round(sharpe_3m, 3) if sharpe_3m else None,
        "Sharpe_6M": round(sharpe_6m, 3) if sharpe_6m else None,
        "Sharpe_9M": round(sharpe_9m, 3) if sharpe_9m else None,
        "Sharpe_1Y": round(sharpe_1y, 3) if sharpe_1y else None,
        "Volatility": round(volatility * 100, 1) if volatility else None,
        "Pos_Days_3M": round(pos_days_3m, 0) if pos_days_3m else None,
        "Pos_Days_6M": round(pos_days_6m, 0) if pos_days_6m else None,
        "Pos_Days_12M": round(pos_days_12m, 0) if pos_days_12m else None,
    }


def _calculate_avg_sharpe(row, method: str) -> float | None:
    """Return a composite Sharpe score for a row based on the selected sort method."""
    sharpes = []
    if method in ["1 year", "1Y"]:
        return row.get("Sharpe_1Y")
    elif method in ["3 months", "3M"]:
        return row.get("Sharpe_3M")
    elif method in ["6 months", "6M"]:
        return row.get("Sharpe_6M")
    elif method in ["9 months", "9M"]:
        return row.get("Sharpe_9M")
    elif method == "Average of 3/6/9/12 months":
        for k in ["Sharpe_3M", "Sharpe_6M", "Sharpe_9M", "Sharpe_1Y"]:
            v = row.get(k)
            if v is not None:
                sharpes.append(v)
        return sum(sharpes) / len(sharpes) if sharpes else None
    elif method == "Average of 3/6 months":
        for k in ["Sharpe_3M", "Sharpe_6M"]:
            v = row.get(k)
            if v is not None:
                sharpes.append(v)
        return sum(sharpes) / len(sharpes) if sharpes else None
    return None
