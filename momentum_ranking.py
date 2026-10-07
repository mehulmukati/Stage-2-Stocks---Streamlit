"""Eligibility and stable ordering shared by Momentum Screener and portfolio ranking."""

import pandas as pd

from config import MIN_VOLUME
from momentum_engine import _calculate_avg_sharpe


def fails_quality_filters(
    row,
    min_annual_return=0,
    pct_from_52w_high=100,
    max_circuits=999,
    close_above_100dma=False,
    close_above_200dma=False,
    pos_days_3m_min=0,
    pos_days_6m_min=0,
    pos_days_12m_min=0,
):
    for column, threshold in [
        ("1Y_Change", min_annual_return),
        ("Pos_Days_3M", pos_days_3m_min),
        ("Pos_Days_6M", pos_days_6m_min),
        ("Pos_Days_12M", pos_days_12m_min),
    ]:
        if threshold > 0 and (pd.isna(row.get(column)) or row[column] < threshold):
            return True
    if pct_from_52w_high < 100 and (
        pd.isna(row.get("Pct_From_52W_High")) or row["Pct_From_52W_High"] < -pct_from_52w_high
    ):
        return True
    if max_circuits < 999 and (pd.isna(row.get("Circuit_Count")) or row["Circuit_Count"] > max_circuits):
        return True
    for enabled, column in [(close_above_100dma, "DMA100"), (close_above_200dma, "DMA200")]:
        if enabled and (pd.isna(row.get("Close")) or pd.isna(row.get(column)) or row["Close"] <= row[column]):
            return True
    return False


def eligibility_reason(row, min_history_days=252, minimum_median_volume=MIN_VOLUME, max_stale_sessions=3, **quality):
    if not row.get("Tradable", True):
        return "no_tradable_data"
    if row.get("_count", 0) < min_history_days:
        return "insufficient_history"
    if row.get("_missing_rate", 0) > 0.05:
        return "missing_data"
    if row.get("Stale Sessions", 0) > max_stale_sessions >= 0:
        return "stale_price"
    if minimum_median_volume > 0 and (pd.isna(row.get("Vol_Median")) or row["Vol_Median"] < minimum_median_volume):
        return "low_volume"
    if fails_quality_filters(row, **quality):
        return "quality_filter"
    return None


def rank_momentum_frame(
    frame, sort_method, min_history_days=252, minimum_median_volume=MIN_VOLUME, max_stale_sessions=3, **quality
):
    """Filter full-precision shared metrics, then sort score descending / symbol ascending."""
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    reasons = result.apply(
        lambda r: eligibility_reason(r, min_history_days, minimum_median_volume, max_stale_sessions, **quality),
        axis=1,
    )
    result = result[reasons.isna()].copy()
    result["Avg_Sharpe"] = result.apply(lambda r: _calculate_avg_sharpe(r, sort_method), axis=1)
    return (
        result[result.Avg_Sharpe.notna()]
        .sort_values(["Avg_Sharpe", "Symbol"], ascending=[False, True])
        .reset_index(drop=True)
    )
