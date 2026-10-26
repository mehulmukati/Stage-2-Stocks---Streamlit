"""In-memory equity observation policy; source observations are never rewritten."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

EQUITY_POLICY_VERSION = 1
RECENT_SESSION_WINDOW = 252
MIN_RECENT_COVERAGE = 0.95


def accepted_equity_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Exclude placeholders and unknown-volume quotes from equity calculations."""
    close = pd.to_numeric(frame["Close"], errors="coerce")
    volume = pd.to_numeric(frame["Volume"], errors="coerce")
    valid = np.isfinite(close) & close.gt(0) & np.isfinite(volume) & volume.gt(0)
    return frame.loc[valid].copy()


def observed_sessions(frames) -> pd.DatetimeIndex:
    """Market-wide observed sessions, including genuine weekend trading.

    A zero-volume holiday row cannot introduce a session into this calendar.
    Callers must use the whole available market, not one stock's own dates.
    """
    indices = [accepted_equity_rows(frame).index for frame in frames]
    if not indices:
        return pd.DatetimeIndex([])
    return pd.DatetimeIndex(np.concatenate(indices)).normalize().sort_values().unique()


@lru_cache(maxsize=1)
def _cash_holidays() -> frozenset:
    path = Path(__file__).with_name("nse_holidays.json")
    if not path.exists():
        return frozenset()
    entries = json.loads(path.read_text(encoding="utf-8")).get("CM", [])
    return frozenset(
        pd.to_datetime(entry.get("tradingDate", entry.get("tradingDate ", "")).strip(), format="%d-%b-%Y")
        for entry in entries
    )


def extend_sessions(calendar: pd.DatetimeIndex, through) -> pd.DatetimeIndex:
    """Count expected sessions after the latest observed market-wide session too."""
    if calendar.empty or through is None or pd.Timestamp(through) <= calendar[-1]:
        return calendar
    tail = pd.bdate_range(calendar[-1] + pd.Timedelta(days=1), through)
    return calendar.union(tail[~tail.isin(_cash_holidays())])


def recent_coverage(
    dates: pd.DatetimeIndex, as_of, calendar: pd.DatetimeIndex, window: int = RECENT_SESSION_WINDOW
) -> float:
    """Fraction of recent market sessions observed since a stock's first trade.

    History length is a separate gate. A new listing is not charged for dates
    before listing; a resumed old history is charged for its recent gaps.
    Both the calendar and observations are cut at as_of to avoid look-ahead.
    """
    if dates.empty or calendar.empty:
        return 0.0
    end = calendar.searchsorted(pd.Timestamp(as_of), side="right")
    start = max(0, end - window, calendar.searchsorted(dates[0], side="left"))
    if end <= start:
        return 0.0
    observed = dates.searchsorted(calendar[end - 1], side="right") - dates.searchsorted(calendar[start], side="left")
    return min(1.0, float(observed / (end - start)))


def coverage_by_date(dates: pd.DatetimeIndex, calendar: pd.DatetimeIndex) -> pd.Series:
    """Vectorized historical coverage at each observed bar, without future data."""
    if dates.empty:
        return pd.Series(index=dates, dtype=float)
    ends = calendar.searchsorted(dates, side="right")
    starts = np.maximum(np.maximum(0, ends - RECENT_SESSION_WINDOW), calendar.searchsorted(dates[0]))
    first_dates = calendar.take(starts)
    counts = np.arange(1, len(dates) + 1) - dates.searchsorted(first_dates)
    return pd.Series(np.minimum(1.0, counts / np.maximum(1, ends - starts)), index=dates)


def observation_diagnostics(prices: pd.DataFrame) -> dict:
    """Explain the raw rows excluded by readers; no change to publication content."""
    equities = prices[prices.series_type.eq("equity")]
    accepted = accepted_equity_rows(equities)
    zero = equities[equities.Volume.eq(0)]
    nonflat = zero[["Open", "High", "Low", "Close"]].nunique(axis=1).gt(1)
    return {
        "policy_version": EQUITY_POLICY_VERSION,
        "raw_rows": len(equities),
        "accepted_rows": len(accepted),
        "excluded_rows": len(equities) - len(accepted),
        "zero_volume_rows": len(zero),
        "unknown_volume_rows": int(equities.Volume.isna().sum()),
        "nonflat_zero_volume_rows": int(nonflat.sum()),
    }
