"""Heikin-Ashi no-wick trend signals with weekend-only decisions."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import numpy as np
import pandas as pd

from strategy_replay import resolve_signal_executions

HA_EMA_ENGINE_VERSION = 6


@dataclass(frozen=True)
class HAEMAStrategyConfig:
    """Indicator and entry-filter settings from the reference Pine strategy."""

    ema_fast: int = 10
    ema_slow: int = 30
    average_volume_length: int = 30
    minimum_average_volume: float = 100_000.0
    return_length: int = 52
    minimum_return_pct: float = 50.0
    wick_tolerance: float = 1e-9


_SIGNAL_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Execution_Date",
    "HA_Open",
    "HA_High",
    "HA_Low",
    "HA_Close",
    "EMA_Fast",
    "EMA_Slow",
    "Average_Volume",
    "Return_Pct",
    "Bullish_HA",
    "No_Lower_Wick",
    "No_Upper_Wick",
    "Has_Lower_Wick",
    "Has_Upper_Wick",
    "Two_Wicks",
    "Previous_Two_Wicks",
    "Above_Fast_EMA",
    "Above_Slow_EMA",
    "EMA_Aligned",
    "Volume_Filter",
    "Return_Filter",
    "Long_Condition",
    "Exit_Condition",
    "Decision_Eligible",
    "Signal",
    "Position",
]


def _normalise_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    required = {"Open", "High", "Low", "Close", "Volume"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required OHLCV columns: {', '.join(sorted(missing))}")

    clean = df[list(required)].copy()
    clean.index = pd.DatetimeIndex(pd.to_datetime(clean.index, errors="coerce"))
    clean = clean[~clean.index.isna()].sort_index()
    clean = clean[~clean.index.duplicated(keep="last")]
    for column in required:
        clean[column] = pd.to_numeric(clean[column], errors="coerce")
    clean = clean.dropna(subset=["Open", "High", "Low", "Close"])
    clean = clean[(clean[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
    if clean.index.tz is not None:
        clean.index = clean.index.tz_localize(None)
    return clean[["Open", "High", "Low", "Close", "Volume"]]


def _validate_strategy_config(config: HAEMAStrategyConfig) -> None:
    periods = (config.ema_fast, config.ema_slow, config.average_volume_length, config.return_length)
    if min(periods) <= 0:
        raise ValueError("EMA, volume and return periods must be positive")
    if config.ema_fast >= config.ema_slow:
        raise ValueError("Fast EMA must be shorter than slow EMA")
    if config.minimum_average_volume < 0 or config.minimum_return_pct < 0 or config.wick_tolerance < 0:
        raise ValueError("Filter thresholds and wick tolerance cannot be negative")


@lru_cache(maxsize=8)
def _cash_market_holidays(path: str | None = None) -> frozenset[pd.Timestamp]:
    holiday_path = path or os.path.join(os.path.dirname(__file__), "nse_holidays.json")
    try:
        with open(holiday_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, TypeError, ValueError):
        return frozenset()
    rows = payload.get("CM", []) if isinstance(payload, dict) else []
    holidays: set[pd.Timestamp] = set()
    for row in rows:
        raw = row.get("tradingDate") if isinstance(row, dict) else None
        parsed = pd.to_datetime(raw, format="%d-%b-%Y", errors="coerce") if raw else pd.NaT
        if pd.notna(parsed):
            holidays.add(pd.Timestamp(parsed).normalize())
    return frozenset(holidays)


def weekly_decision_mask(
    index: pd.DatetimeIndex,
    as_of_date: str | pd.Timestamp | None = None,
    holidays_path: str | None = None,
) -> pd.Series:
    """Mark the final completed NSE observation of each Friday-ending week."""

    dates = pd.DatetimeIndex(pd.to_datetime(index)).tz_localize(None).normalize()
    mask = pd.Series(False, index=index, dtype=bool)
    if dates.empty:
        return mask
    raw_as_of = pd.Timestamp.now() if as_of_date is None else pd.Timestamp(as_of_date)
    as_of = raw_as_of.tz_localize(None).normalize()
    holidays = _cash_market_holidays(holidays_path)
    periods = dates.to_period("W-FRI")
    for period in periods.unique():
        positions = np.flatnonzero(periods == period)
        observed_last = dates[positions[-1]]
        calendar_friday = pd.Timestamp(period.end_time).normalize()
        expected_last = calendar_friday
        while expected_last.weekday() >= 5 or expected_last in holidays:
            expected_last -= pd.Timedelta(days=1)

        has_later_observation = positions[-1] < len(dates) - 1
        week_complete = has_later_observation or (as_of >= expected_last and observed_last >= expected_last)
        if week_complete:
            mask.iloc[positions[-1]] = True
    return mask


def resample_weekly_ohlcv(
    df: pd.DataFrame,
    as_of_date: str | pd.Timestamp | None = None,
    holidays_path: str | None = None,
) -> pd.DataFrame:
    """Aggregate daily observations into weekly bars with actual first-session dates."""

    daily = _normalise_ohlcv(df)
    if daily.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume", "Execution_Date", "Decision_Eligible"])
    source = daily.assign(_Period=daily.index.to_period("W-FRI"), _Observed_Date=daily.index)
    grouped = source.groupby("_Period", sort=True, observed=True)
    result = grouped.agg(
        Open=("Open", "first"),
        High=("High", "max"),
        Low=("Low", "min"),
        Close=("Close", "last"),
        Volume=("Volume", "sum"),
        Execution_Date=("_Observed_Date", "first"),
        Decision_Date=("_Observed_Date", "last"),
    )
    result.index = pd.DatetimeIndex(result.pop("Decision_Date"))
    result.index.name = None
    result["Decision_Eligible"] = True

    # Every non-final group is known complete because a later observation exists.
    # The final group is complete only when its expected final NSE session exists.
    final_date = pd.Timestamp(result.index[-1]).normalize()
    calendar_friday = pd.Timestamp(source["_Period"].iloc[-1].end_time).normalize()
    expected_last = calendar_friday
    holidays = _cash_market_holidays(holidays_path)
    while expected_last.weekday() >= 5 or expected_last in holidays:
        expected_last -= pd.Timedelta(days=1)
    raw_as_of = pd.Timestamp.now() if as_of_date is None else pd.Timestamp(as_of_date)
    as_of = raw_as_of.tz_localize(None).normalize()
    result.iloc[-1, result.columns.get_loc("Decision_Eligible")] = bool(
        as_of >= expected_last and final_date >= expected_last
    )
    return result


def compute_ha_ema_signals(
    df: pd.DataFrame,
    config: HAEMAStrategyConfig | None = None,
    as_of_date: str | pd.Timestamp | None = None,
    holidays_path: str | None = None,
) -> pd.DataFrame:
    """Calculate weekly indicators and emit completed-week transition signals."""

    config = config or HAEMAStrategyConfig()
    _validate_strategy_config(config)
    if df.empty:
        return pd.DataFrame(columns=_SIGNAL_COLUMNS)

    result = resample_weekly_ohlcv(df, as_of_date, holidays_path)
    if result.empty:
        return pd.DataFrame(columns=_SIGNAL_COLUMNS)

    ha_close = result[["Open", "High", "Low", "Close"]].mean(axis=1)
    ha_open_values = np.empty(len(result), dtype=float)
    ha_open_values[0] = (float(result["Open"].iloc[0]) + float(result["Close"].iloc[0])) / 2.0
    for position in range(1, len(result)):
        ha_open_values[position] = (ha_open_values[position - 1] + float(ha_close.iloc[position - 1])) / 2.0
    ha_open = pd.Series(ha_open_values, index=result.index)

    result["HA_Open"] = ha_open
    result["HA_Close"] = ha_close
    result["HA_High"] = pd.concat([result["High"], ha_open, ha_close], axis=1).max(axis=1)
    result["HA_Low"] = pd.concat([result["Low"], ha_open, ha_close], axis=1).min(axis=1)
    result["EMA_Fast"] = result["Close"].ewm(span=config.ema_fast, adjust=False).mean()
    result["EMA_Slow"] = result["Close"].ewm(span=config.ema_slow, adjust=False).mean()
    result["Average_Volume"] = (
        result["Volume"].rolling(config.average_volume_length, min_periods=config.average_volume_length).mean()
    )
    result["Return_Pct"] = (result["Close"] / result["Close"].shift(config.return_length) - 1.0) * 100.0

    result["Bullish_HA"] = result["HA_Close"] > result["HA_Open"]
    result["No_Lower_Wick"] = (result["HA_Open"] - result["HA_Low"]).abs() <= config.wick_tolerance
    result["No_Upper_Wick"] = (result["HA_Open"] - result["HA_High"]).abs() <= config.wick_tolerance
    body_top = result[["HA_Open", "HA_Close"]].max(axis=1)
    body_bottom = result[["HA_Open", "HA_Close"]].min(axis=1)
    result["Has_Lower_Wick"] = result["HA_Low"] < body_bottom - config.wick_tolerance
    result["Has_Upper_Wick"] = result["HA_High"] > body_top + config.wick_tolerance
    result["Two_Wicks"] = result["Has_Lower_Wick"] & result["Has_Upper_Wick"]
    result["Previous_Two_Wicks"] = result["Two_Wicks"].shift(1, fill_value=False)
    result["Above_Fast_EMA"] = result["Close"] > result["EMA_Fast"]
    result["Above_Slow_EMA"] = result["Close"] > result["EMA_Slow"]
    result["EMA_Aligned"] = result["EMA_Fast"] > result["EMA_Slow"]
    result["Volume_Filter"] = result["Average_Volume"] > config.minimum_average_volume
    result["Return_Filter"] = result["Return_Pct"].notna() & (result["Return_Pct"] > config.minimum_return_pct)
    result["Long_Condition"] = (
        result["Bullish_HA"]
        & result["No_Lower_Wick"]
        & result["Previous_Two_Wicks"]
        & result["Above_Fast_EMA"]
        & result["Above_Slow_EMA"]
        & result["EMA_Aligned"]
        & result["Volume_Filter"]
        & result["Return_Filter"]
    )
    result["Exit_Condition"] = result["No_Upper_Wick"]
    held = False
    signals: list[str] = []
    positions: list[bool] = []
    for decision_eligible, long_condition, exit_condition in zip(
        result["Decision_Eligible"], result["Long_Condition"], result["Exit_Condition"]
    ):
        signal = ""
        if decision_eligible:
            if held and bool(exit_condition):
                signal = "EXIT"
                held = False
            elif not held and bool(long_condition):
                signal = "BUY"
                held = True
        signals.append(signal)
        positions.append(held)
    result["Signal"] = signals
    result["Position"] = positions
    return result[_SIGNAL_COLUMNS]


def latest_ha_ema_state(
    data: pd.DataFrame,
    ticker: str,
    config: HAEMAStrategyConfig | None = None,
) -> dict[str, Any]:
    """Extract the latest facts used by status cards and the rule-based summary."""

    config = config or HAEMAStrategyConfig()
    base: dict[str, Any] = {
        "ticker": ticker,
        "sufficient_data": False,
        "status": "Unavailable",
        "position": False,
        "last_signal": None,
        "conditions": {},
    }
    if data.empty:
        return base

    completed = data[data["Decision_Eligible"].astype(bool)]
    latest = completed.iloc[-1] if not completed.empty else data.iloc[-1]
    latest_date = pd.Timestamp(completed.index[-1] if not completed.empty else data.index[-1])
    conditions = {
        "Previous weekly HA has two wicks": bool(latest["Previous_Two_Wicks"]),
        "Bullish HA": bool(latest["Bullish_HA"]),
        "No lower wick": bool(latest["No_Lower_Wick"]),
        f"Close > EMA {config.ema_fast}": bool(latest["Above_Fast_EMA"]),
        f"Close > EMA {config.ema_slow}": bool(latest["Above_Slow_EMA"]),
        f"EMA {config.ema_fast} > EMA {config.ema_slow}": bool(latest["EMA_Aligned"]),
        "Volume gate": bool(latest["Volume_Filter"]),
        "1-year return gate": bool(latest["Return_Filter"]),
    }
    signal_rows = data[data["Signal"].ne("")]
    last_signal = None
    if not signal_rows.empty:
        event = signal_rows.iloc[-1]
        signal_date = pd.Timestamp(signal_rows.index[-1])
        resolved = resolve_signal_executions(data.loc[signal_date:])
        execution_date = None
        if not resolved.empty and pd.Timestamp(resolved.iloc[0]["Signal_Date"]) == signal_date:
            execution_date = pd.Timestamp(resolved.iloc[0]["Execution_Date"])
        last_signal = {
            "type": str(event["Signal"]),
            "date": signal_date,
            "age_weeks": int(len(data) - 1 - data.index.get_loc(signal_date)),
            "execution_date": execution_date,
        }

    position = bool(data["Position"].iloc[-1])
    if str(data["Signal"].iloc[-1]) == "BUY":
        status = "BUY planned"
    elif str(data["Signal"].iloc[-1]) == "EXIT":
        status = "EXIT planned"
    elif position:
        status = "Long / Hold"
    elif bool(latest["Long_Condition"]):
        status = "Entry setup"
    else:
        status = "Waiting"
    base.update(
        {
            "sufficient_data": pd.notna(latest["Return_Pct"]),
            "status": status,
            "position": position,
            "last_signal": last_signal,
            "conditions": conditions,
            "latest_date": latest_date,
            "decision_eligible": bool(latest["Decision_Eligible"]),
            "close": float(latest["Close"]),
            "average_volume": None if pd.isna(latest["Average_Volume"]) else float(latest["Average_Volume"]),
            "return_pct": None if pd.isna(latest["Return_Pct"]) else float(latest["Return_Pct"]),
            "ema_fast": float(latest["EMA_Fast"]),
            "ema_slow": float(latest["EMA_Slow"]),
        }
    )
    return base
