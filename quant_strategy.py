"""Stable strategy-to-portfolio contract used by Quant-Portfolio-Maker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd

SIGNAL_COLUMNS = (
    "Decision_Date",
    "Execution_Date",
    "Ready",
    "Entry_Event",
    "Exit_Event",
    "Hold_Eligible",
    "Score",
    "Entry_Reason",
    "Exit_Reason",
    "Diagnostics",
)


@dataclass(frozen=True)
class StrategyDefinition:
    key: str
    label: str
    timeframe: str
    description: str
    required_price_columns: tuple[str, ...] = ("Open", "High", "Low", "Close")
    uses_liquidity_filter: bool = False
    warmup_periods: int = 0


class QuantStrategy(Protocol):
    definition: StrategyDefinition

    def compute(self, ohlcv: pd.DataFrame) -> pd.DataFrame: ...


def next_session_dates(decision_dates: pd.Index, daily_dates: pd.Index) -> pd.Series:
    """Map each completed decision candle to that symbol's next observed session."""
    decisions = pd.DatetimeIndex(pd.to_datetime(decision_dates)).tz_localize(None)
    sessions = pd.DatetimeIndex(pd.to_datetime(daily_dates)).tz_localize(None).sort_values().unique()
    positions = sessions.searchsorted(decisions, side="right")
    values = [sessions[pos] if pos < len(sessions) else pd.NaT for pos in positions]
    return pd.Series(values, index=decisions, dtype="datetime64[ns]")


def validate_signal_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = set(SIGNAL_COLUMNS).difference(frame.columns)
    if missing:
        raise ValueError(f"Strategy adapter omitted: {', '.join(sorted(missing))}")
    clean = frame.loc[:, SIGNAL_COLUMNS].copy()
    clean["Decision_Date"] = pd.to_datetime(clean["Decision_Date"])
    clean["Execution_Date"] = pd.to_datetime(clean["Execution_Date"])
    clean = clean.sort_values(["Decision_Date", "Execution_Date"], na_position="last")
    return clean.reset_index(drop=True)


def diagnostic_dict(row: pd.Series, fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: row.get(field) for field in fields}
