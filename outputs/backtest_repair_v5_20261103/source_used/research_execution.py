"""Shared research execution convention for daily and intraday quote sources."""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FillQuote:
    price: float
    available_at: pd.Timestamp
    source: str
    assumed: bool
    age: pd.Timedelta


def daily_close_quotes(frame, source):
    """Daily exchange closes are available at 15:30 IST, never at session open."""
    dates = pd.DatetimeIndex(frame.index)
    dates = dates.tz_localize("Asia/Kolkata") if dates.tz is None else dates.tz_convert("Asia/Kolkata")
    return intraday_quotes(frame, dates.normalize() + pd.Timedelta(hours=15, minutes=30), source)


def intraday_quotes(frame, available_at, source):
    """MoMo callers supply candle completion timestamps, not candle start times."""
    times = pd.DatetimeIndex(available_at)
    if times.tz is None or times.hasnans or times.has_duplicates or len(times) != len(frame):
        raise ValueError("Quote availability requires one unique timezone-aware timestamp per row")
    if not source:
        raise ValueError("Quote source is required")
    return pd.DataFrame(
        {
            "Close": frame.Close.to_numpy(),
            "Volume": frame.Volume.to_numpy(),
            "tradable": frame.get("is_tradable", pd.Series(True, index=frame.index)).fillna(False).to_numpy(),
            "source": source,
        },
        index=times.tz_convert("UTC"),
    ).sort_index()


def resolve_quote(quotes, cutoff, side, *, legally_tradable):
    """Current quote first; an ordinary missing sale may use a disclosed assumption.

    A locked or obsolete security cannot use the assumption. An assumed quote
    belongs to this sale alone; never write it into the market data or mark book.
    The caller passes the resulting price to its normal fee and tax ledger.
    """
    cutoff = pd.Timestamp(cutoff)
    if cutoff.tz is None or side not in {"BUY", "SELL"}:
        raise ValueError("Timezone-aware cutoff and explicit side required")
    if not legally_tradable:
        return None
    rows = quotes.loc[:cutoff]
    rows = rows.loc[
        rows.tradable & np.isfinite(rows.Close) & rows.Close.gt(0) & np.isfinite(rows.Volume) & rows.Volume.gt(0)
    ]
    if rows.empty:
        return None
    time, row = rows.index[-1], rows.iloc[-1]
    age = cutoff - time
    if age and side == "BUY":
        return None
    return FillQuote(float(row.Close), time, row.source, bool(age), age)
