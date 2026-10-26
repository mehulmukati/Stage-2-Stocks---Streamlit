import numpy as np
import pandas as pd

from config import CIRCUIT_LEVELS, CIRCUIT_TOLERANCE
from equity_data import session_frame

_VOL_WINDOW = 251
_VOL_MIN_PERIODS = 63


def _annualized_vol(returns: pd.Series) -> float | None:
    """Volatility over at most 251 consecutive session returns; never skip gaps."""
    values = returns.iloc[1:] if len(returns) and pd.isna(returns.iloc[0]) else returns
    tail = values.tail(_VOL_WINDOW)
    if len(tail) < _VOL_MIN_PERIODS or tail.isna().any():
        return None
    s = tail.std()
    return float(s * np.sqrt(252)) if s > 0 else None


def _count_circuits(df: pd.DataFrame, calendar=None) -> int | None:
    """Count circuit-breaker closes (upper or lower) over the last 252 trading days (1 year)."""
    subset = session_frame(df, calendar).tail(252)
    if subset.Close.isna().any():
        return None
    if len(subset) < 2:
        return 0
    pct_change = subset["Close"].pct_change(fill_method=None) * 100
    circuit_count = 0
    for level in CIRCUIT_LEVELS:
        upper = (pct_change >= level - CIRCUIT_TOLERANCE) & (pct_change <= level + CIRCUIT_TOLERANCE)
        lower = (pct_change <= -level - CIRCUIT_TOLERANCE) & (pct_change >= -level + CIRCUIT_TOLERANCE)
        circuit_count += (upper | lower).sum()
    return int(circuit_count)


def _calculate_sharpe(df: pd.DataFrame, period_days: int, calendar=None) -> float | None:
    """Return annualized Sharpe ratio (no risk-free rate) over the last period_days trading days."""
    df = session_frame(df, calendar)
    if len(df) < period_days:
        return None
    subset = df.tail(period_days)
    if subset.Close.isna().any():
        return None
    daily_returns = subset["Close"].pct_change(fill_method=None).iloc[1:]
    if len(daily_returns) == 0 or daily_returns.std() == 0:
        return None
    return daily_returns.mean() / daily_returns.std() * np.sqrt(252)


def _calculate_positive_days_pct(df: pd.DataFrame, months: int, calendar=None) -> float | None:
    """Return the percentage of up-close days over the last N months (approx 21 trading days/month)."""
    days_approx = int(months * 21)
    df = session_frame(df, calendar)
    if len(df) < days_approx:
        return None
    subset = df.tail(days_approx)
    if subset.Close.isna().any():
        return None
    positive_days = (subset["Close"].diff() > 0).sum()
    total_days = len(subset) - 1
    if total_days == 0:
        return None
    return (positive_days / total_days) * 100


def score_momentum(df: pd.DataFrame, calendar=None) -> dict | None:
    """Scalar public view of the same fixed-session metrics used by replay."""
    metrics = precompute_metrics(df, calendar)
    if metrics.empty or metrics.iloc[-1]["_count"] < 250:
        return None
    row = metrics.iloc[-1]
    keys = [key for key in metrics if not key.startswith("_") and key != "Sharpe_1M"]
    result = {key: (None if pd.isna(row[key]) else float(row[key])) for key in keys}
    if result["Close"] is not None:
        result["Close"] = round(result["Close"], 2)
    if result["Vol_Median"] is not None:
        result["Vol_Median"] = int(result["Vol_Median"])
    if result["Circuit_Count"] is not None:
        result["Circuit_Count"] = int(result["Circuit_Count"])
    return result


def precompute_metrics(df: pd.DataFrame, calendar=None) -> pd.DataFrame:
    """
    Vectorized equivalent of score_momentum for every date in df.

    Produces the same fields as score_momentum but for the full history in one pass —
    O(n) per symbol instead of O(n) per (rebalance × symbol). The backtest engine calls
    this once per symbol before the rebalance loop, then does O(log n) date lookups.

    _count counts supported price sessions; _missing_rate measures unsupported
    closes in the latest 252 exchange sessions, including absent source rows.
    """
    df = session_frame(df, calendar)
    c = df["Close"]
    v = df["Volume"]
    h = df["High"]
    rets = c.pct_change(fill_method=None)

    def _rolling_sharpe(window: int) -> pd.Series:
        # tail(window) → pct_change().dropna() gives window-1 returns → rolling(window-1)
        P = window - 1
        r_mean = rets.rolling(P, min_periods=P).mean()
        r_std = rets.rolling(P, min_periods=P).std()
        return (r_mean / r_std * np.sqrt(252)).where(r_std > 0)

    # Circuit hits: tail(252) → 251 pct_changes → rolling(252) includes 251 valid diffs
    pct_chg = c.pct_change(fill_method=None) * 100
    circuit_hits = pd.Series(0.0, index=c.index).where(pct_chg.notna())
    for level in CIRCUIT_LEVELS:
        upper = (pct_chg >= level - CIRCUIT_TOLERANCE) & (pct_chg <= level + CIRCUIT_TOLERANCE)
        lower = (pct_chg <= -level - CIRCUIT_TOLERANCE) & (pct_chg >= -level + CIRCUIT_TOLERANCE)
        circuit_hits += (upper | lower).astype(float)

    # Positive-days: tail(days_approx) → days_approx-1 diffs → rolling(days_approx) / (days_approx-1)
    pos = (c.diff() > 0).astype(float).where(c.notna() & c.shift().notna())
    high_52w = h.rolling(252, min_periods=1).max().where(pd.Series(range(1, len(c) + 1), index=c.index).ge(252))
    incomplete_high = (df.is_tradable & h.isna()) | c.isna()
    high_52w = high_52w.where(incomplete_high.rolling(252, min_periods=1).sum().eq(0))
    positions = pd.Series(np.arange(len(c)), index=c.index)
    last_trade = positions.where(df.is_tradable).ffill()
    endpoints = c.where((positions - last_trade).le(3))

    # 1Y change: c.iloc[-1] / c.iloc[-252] − 1 = c / c.shift(251) − 1 = pct_change(251)
    return pd.DataFrame(
        {
            "Close": c,
            "52w_High": high_52w.round(2),
            "DMA100": c.rolling(100, min_periods=100).mean().round(2),
            "DMA200": c.rolling(200, min_periods=200).mean().round(2),
            "Vol_Median": v.rolling(252, min_periods=252).median(),
            "1Y_Change": (endpoints.pct_change(251, fill_method=None) * 100).round(2),
            "Pct_From_52W_High": ((c - high_52w) / high_52w * 100).round(2),
            # tail(252) → pct_change → NaN at row 0, 251 real diffs → rolling(251)
            "Circuit_Count": circuit_hits.rolling(251, min_periods=251).sum(),
            "Sharpe_1M": _rolling_sharpe(21).round(3),
            "Sharpe_3M": _rolling_sharpe(63).round(3),
            "Sharpe_6M": _rolling_sharpe(126).round(3),
            "Sharpe_9M": _rolling_sharpe(189).round(3),
            "Sharpe_1Y": _rolling_sharpe(252).round(3),
            "Volatility": (rets.rolling(251, min_periods=63).std() * np.sqrt(252) * 100)
            .where(rets.rolling(251, min_periods=1).count().eq(pd.Series(range(len(c)), index=c.index).clip(upper=251)))
            .round(1),
            # tail(N).diff() skips row-0 NaN: N rows → N-1 real diffs → rolling(N-1) / (N-1)
            "Pos_Days_3M": (pos.rolling(62, min_periods=62).sum() / 62 * 100).round(0),
            "Pos_Days_6M": (pos.rolling(125, min_periods=125).sum() / 125 * 100).round(0),
            "Pos_Days_12M": (pos.rolling(251, min_periods=251).sum() / 251 * 100).round(0),
            "_count": c.notna().cumsum(),
            "_missing_rate": c.isna().rolling(252, min_periods=1).mean(),
        }
    )


# Maps each sort-method label to the Sharpe columns it averages.
# Single-element lists return that column directly; multi-element lists are averaged.
_SHARPE_METHOD_KEYS: dict[str, list[str]] = {
    "1 year": ["Sharpe_1Y"],
    "1Y": ["Sharpe_1Y"],
    "3 months": ["Sharpe_3M"],
    "3M": ["Sharpe_3M"],
    "6 months": ["Sharpe_6M"],
    "6M": ["Sharpe_6M"],
    "9 months": ["Sharpe_9M"],
    "9M": ["Sharpe_9M"],
    "Average of 3/6/9/12 months": ["Sharpe_3M", "Sharpe_6M", "Sharpe_9M", "Sharpe_1Y"],
    "Average of 3/6 months": ["Sharpe_3M", "Sharpe_6M"],
    "Average of 1/3/6/12 months": ["Sharpe_1M", "Sharpe_3M", "Sharpe_6M", "Sharpe_1Y"],
    "Average of 1/3/12 months": ["Sharpe_1M", "Sharpe_3M", "Sharpe_1Y"],
    "Average of 1/3/6/9/12 months": ["Sharpe_1M", "Sharpe_3M", "Sharpe_6M", "Sharpe_9M", "Sharpe_1Y"],
}


def _calculate_avg_sharpe(row, method: str) -> float | None:
    """Return a composite Sharpe score for a row based on the selected sort method."""
    keys = _SHARPE_METHOD_KEYS.get(method)
    if keys is None:
        return None
    vals = [v for k in keys if (v := row.get(k)) is not None and not pd.isna(v)]
    import math

    return math.fsum(float(value) for value in vals) / len(vals) if len(vals) == len(keys) else None
