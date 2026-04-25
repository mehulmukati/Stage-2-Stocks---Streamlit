import numpy as np
import pandas as pd

from config import CIRCUIT_LEVELS, CIRCUIT_TOLERANCE


def _count_circuits(df: pd.DataFrame) -> int:
    """Count circuit-breaker closes (upper or lower) over the last 252 trading days (1 year)."""
    subset = df.tail(252)
    if len(subset) < 2:
        return 0
    pct_change = subset["Close"].pct_change() * 100
    circuit_count = 0
    for level in CIRCUIT_LEVELS:
        upper = (pct_change >= level - CIRCUIT_TOLERANCE) & (pct_change <= level + CIRCUIT_TOLERANCE)
        lower = (pct_change <= -level - CIRCUIT_TOLERANCE) & (pct_change >= -level + CIRCUIT_TOLERANCE)
        circuit_count += (upper | lower).sum()
    return int(circuit_count)


def _calculate_sharpe(df: pd.DataFrame, period_days: int) -> float | None:
    """Return annualized Sharpe ratio (no risk-free rate) over the last period_days trading days."""
    if len(df) < period_days:
        return None
    subset = df.tail(period_days)
    daily_returns = subset["Close"].pct_change().dropna()
    if len(daily_returns) == 0 or daily_returns.std() == 0:
        return None
    return daily_returns.mean() / daily_returns.std() * np.sqrt(252)


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
        "Vol_Median": int(vol_median) if vol_median is not None and not np.isnan(vol_median) else None,
        "1Y_Change": round(one_yr_change, 2) if one_yr_change is not None else None,
        "Pct_From_52W_High": round(pct_from_52w_high, 2) if pct_from_52w_high is not None else None,
        "Circuit_Count": circuit_count,
        "Sharpe_3M": round(sharpe_3m, 3) if sharpe_3m is not None else None,
        "Sharpe_6M": round(sharpe_6m, 3) if sharpe_6m is not None else None,
        "Sharpe_9M": round(sharpe_9m, 3) if sharpe_9m is not None else None,
        "Sharpe_1Y": round(sharpe_1y, 3) if sharpe_1y is not None else None,
        "Volatility": round(volatility * 100, 1) if volatility is not None else None,
        "Pos_Days_3M": round(pos_days_3m, 0) if pos_days_3m is not None else None,
        "Pos_Days_6M": round(pos_days_6m, 0) if pos_days_6m is not None else None,
        "Pos_Days_12M": round(pos_days_12m, 0) if pos_days_12m is not None else None,
    }


def precompute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized equivalent of score_momentum for every date in df.

    Produces the same fields as score_momentum but for the full history in one pass —
    O(n) per symbol instead of O(n) per (rebalance × symbol). The backtest engine calls
    this once per symbol before the rebalance loop, then does O(log n) date lookups.

    Internal columns _count and _missing_rate mirror the len(sub) and isna().mean()
    filters that rank_universe_at_date applies on the sliced sub-DataFrame.
    """
    c = df["Close"]
    v = df["Volume"]
    h = df["High"]
    rets = c.pct_change()

    def _rolling_sharpe(window: int) -> pd.Series:
        # tail(window) → pct_change().dropna() gives window-1 returns → rolling(window-1)
        P = window - 1
        r_mean = rets.rolling(P, min_periods=P).mean()
        r_std = rets.rolling(P, min_periods=P).std()
        return (r_mean / r_std * np.sqrt(252)).where(r_std > 0)

    # Circuit hits: tail(252) → 251 pct_changes → rolling(252) includes 251 valid diffs
    pct_chg = c.pct_change() * 100
    circuit_hits = pd.Series(0.0, index=c.index)
    for level in CIRCUIT_LEVELS:
        upper = (pct_chg >= level - CIRCUIT_TOLERANCE) & (pct_chg <= level + CIRCUIT_TOLERANCE)
        lower = (pct_chg <= -level - CIRCUIT_TOLERANCE) & (pct_chg >= -level + CIRCUIT_TOLERANCE)
        circuit_hits += (upper | lower).astype(float)

    # Positive-days: tail(days_approx) → days_approx-1 diffs → rolling(days_approx) / (days_approx-1)
    pos = (c.diff() > 0).astype(float)  # NaN at index 0 → False=0, so no NaN in series
    high_52w = h.rolling(252, min_periods=252).max()

    # 1Y change: c.iloc[-1] / c.iloc[-252] − 1 = c / c.shift(251) − 1 = pct_change(251)
    return pd.DataFrame(
        {
            "Close": c.round(2),
            "52w_High": high_52w.round(2),
            "DMA100": c.rolling(100, min_periods=100).mean().round(2),
            "DMA200": c.rolling(200, min_periods=200).mean().round(2),
            "Vol_Median": v.rolling(252, min_periods=252).median(),
            "1Y_Change": (c.pct_change(251) * 100).round(2),
            "Pct_From_52W_High": ((c - high_52w) / high_52w * 100).round(2),
            # tail(252) → pct_change → NaN at row 0, 251 real diffs → rolling(251)
            "Circuit_Count": circuit_hits.rolling(251, min_periods=251).sum(),
            "Sharpe_3M": _rolling_sharpe(63).round(3),
            "Sharpe_6M": _rolling_sharpe(126).round(3),
            "Sharpe_9M": _rolling_sharpe(189).round(3),
            "Sharpe_1Y": _rolling_sharpe(252).round(3),
            "Volatility": (rets.expanding(min_periods=2).std() * np.sqrt(252) * 100).round(1),
            # tail(N).diff() skips row-0 NaN: N rows → N-1 real diffs → rolling(N-1) / (N-1)
            "Pos_Days_3M": (pos.rolling(62, min_periods=62).sum() / 62 * 100).round(0),
            "Pos_Days_6M": (pos.rolling(125, min_periods=125).sum() / 125 * 100).round(0),
            "Pos_Days_12M": (pos.rolling(251, min_periods=251).sum() / 251 * 100).round(0),
            "_count": pd.Series(range(1, len(c) + 1), index=c.index, dtype=int),
            "_missing_rate": c.isna().expanding().mean(),
        }
    )


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
            if v is not None and not pd.isna(v):
                sharpes.append(v)
        return sum(sharpes) / len(sharpes) if sharpes else None
    elif method == "Average of 3/6 months":
        for k in ["Sharpe_3M", "Sharpe_6M"]:
            v = row.get(k)
            if v is not None and not pd.isna(v):
                sharpes.append(v)
        return sum(sharpes) / len(sharpes) if sharpes else None
    return None
