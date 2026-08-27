"""Ichimoku calculations and factual state extraction for a single stock."""

from __future__ import annotations

import json
import os
from typing import Any

import numpy as np
import pandas as pd

ICHIMOKU_ENGINE_VERSION = 2

ICHIMOKU_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Tenkan",
    "Kijun",
    "Senkou_A",
    "Senkou_B",
    "Chikou",
    "TK_Cross",
    "Cross_Strength",
    "IsFuture",
]


def _nse_holidays(path: str | None) -> list[pd.Timestamp]:
    """Load cash-market holidays, returning an empty list when unavailable."""
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "nse_holidays.json")
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError, TypeError):
        return []

    rows = payload.get("CM", []) if isinstance(payload, dict) else []
    holidays: list[pd.Timestamp] = []
    for row in rows:
        raw = row.get("tradingDate") if isinstance(row, dict) else None
        if not raw:
            continue
        parsed = pd.to_datetime(raw, format="%d-%b-%Y", errors="coerce")
        if not pd.isna(parsed):
            holidays.append(pd.Timestamp(parsed).normalize())
    return holidays


def _future_trading_dates(last_date: pd.Timestamp, periods: int, holidays_path: str | None) -> pd.DatetimeIndex:
    if periods <= 0:
        return pd.DatetimeIndex([])
    offset = pd.offsets.CustomBusinessDay(holidays=_nse_holidays(holidays_path))
    return pd.date_range(start=last_date + offset, periods=periods, freq=offset)


def _future_weekly_dates(last_date: pd.Timestamp, periods: int) -> pd.DatetimeIndex:
    """Return future Friday week-end labels for projected weekly cloud bars."""
    if periods <= 0:
        return pd.DatetimeIndex([])
    first_friday = last_date + pd.offsets.Week(weekday=4)
    return pd.date_range(start=first_friday, periods=periods, freq="W-FRI")


def _normalise_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    missing = {"High", "Low", "Close"}.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required OHLC columns: {', '.join(sorted(missing))}")

    clean = df.copy()
    clean.index = pd.DatetimeIndex(pd.to_datetime(clean.index, errors="coerce"))
    clean = clean[~clean.index.isna()].sort_index()
    clean = clean[~clean.index.duplicated(keep="last")]
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in clean:
            clean[column] = pd.to_numeric(clean[column], errors="coerce")
    clean = clean.dropna(subset=["High", "Low", "Close"])
    if clean.index.tz is not None:
        clean.index = clean.index.tz_localize(None)
    return clean


def resample_ohlcv(df: pd.DataFrame, timeframe: str = "Daily") -> pd.DataFrame:
    """Normalize daily OHLCV or aggregate it into actual-date weekly candles."""
    clean = _normalise_ohlc(df)
    normalized_timeframe = timeframe.strip().lower()
    if normalized_timeframe == "daily":
        return clean
    if normalized_timeframe != "weekly":
        raise ValueError("Timeframe must be 'Daily' or 'Weekly'")
    if clean.empty:
        return clean

    weekly_groups = clean.groupby(clean.index.to_period("W-FRI"), sort=True)
    rows: list[dict[str, float]] = []
    dates: list[pd.Timestamp] = []
    for _, group in weekly_groups:
        row = {
            "Open": float(group["Open"].iloc[0]) if "Open" in group else float(group["Close"].iloc[0]),
            "High": float(group["High"].max()),
            "Low": float(group["Low"].min()),
            "Close": float(group["Close"].iloc[-1]),
        }
        if "Volume" in group:
            row["Volume"] = float(group["Volume"].sum(min_count=1))
        rows.append(row)
        dates.append(pd.Timestamp(group.index[-1]))
    return pd.DataFrame(rows, index=pd.DatetimeIndex(dates))


def _crossovers(tenkan: pd.Series, kijun: pd.Series) -> pd.Series:
    """Return genuine side changes, carrying the last side through equality plateaus."""
    difference = tenkan - kijun
    raw_sign = pd.Series(np.sign(difference), index=difference.index, dtype=float)
    effective_sign = raw_sign.replace(0.0, np.nan).ffill()
    previous_sign = effective_sign.shift(1)

    signals = pd.Series(pd.NA, index=difference.index, dtype="object")
    valid_change = raw_sign.ne(0.0) & previous_sign.notna() & effective_sign.ne(previous_sign)
    signals.loc[valid_change & effective_sign.gt(0)] = "bullish"
    signals.loc[valid_change & effective_sign.lt(0)] = "bearish"
    return signals


def _classify_cross(
    direction: str,
    price: float,
    span_a: float,
    span_b: float,
) -> str:
    if pd.isna(span_a) or pd.isna(span_b):
        return "unclassified"
    cloud_top, cloud_bottom = max(span_a, span_b), min(span_a, span_b)
    if direction == "bullish":
        if price > cloud_top:
            return "strong bullish"
        if price < cloud_bottom:
            return "weak bullish"
        return "neutral bullish"
    if price < cloud_bottom:
        return "strong bearish"
    if price > cloud_top:
        return "weak bearish"
    return "neutral bearish"


def compute_ichimoku(
    df: pd.DataFrame,
    conversion_period: int = 9,
    base_period: int = 26,
    span_b_period: int = 52,
    displacement: int = 26,
    holidays_path: str | None = None,
    timeframe: str = "Daily",
) -> pd.DataFrame:
    """Return observed OHLC plus Ichimoku lines and a forward cloud projection."""
    if min(conversion_period, base_period, span_b_period, displacement) <= 0:
        raise ValueError("Ichimoku periods and displacement must be positive")
    if df.empty:
        return pd.DataFrame(columns=ICHIMOKU_COLUMNS)

    observed = resample_ohlcv(df, timeframe)
    if observed.empty:
        return pd.DataFrame(columns=ICHIMOKU_COLUMNS)

    high, low, close = observed["High"], observed["Low"], observed["Close"]
    tenkan = (high.rolling(conversion_period).max() + low.rolling(conversion_period).min()) / 2.0
    kijun = (high.rolling(base_period).max() + low.rolling(base_period).min()) / 2.0
    span_a_source = (tenkan + kijun) / 2.0
    span_b_source = (high.rolling(span_b_period).max() + low.rolling(span_b_period).min()) / 2.0

    if timeframe.strip().lower() == "weekly":
        future_index = _future_weekly_dates(observed.index[-1], displacement)
    else:
        future_index = _future_trading_dates(observed.index[-1], displacement, holidays_path)
    full_index = observed.index.append(future_index)
    result = pd.DataFrame(index=full_index, columns=ICHIMOKU_COLUMNS)
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in observed:
            result.loc[observed.index, column] = observed[column].astype(float)

    result.loc[observed.index, "Tenkan"] = tenkan
    result.loc[observed.index, "Kijun"] = kijun
    result.iloc[displacement : displacement + len(observed), result.columns.get_loc("Senkou_A")] = (
        span_a_source.to_numpy()
    )
    result.iloc[displacement : displacement + len(observed), result.columns.get_loc("Senkou_B")] = (
        span_b_source.to_numpy()
    )
    if len(observed) > displacement:
        result.iloc[: len(observed) - displacement, result.columns.get_loc("Chikou")] = close.iloc[
            displacement:
        ].to_numpy()

    result["IsFuture"] = False
    result.loc[future_index, "IsFuture"] = True
    signals = _crossovers(tenkan, kijun)
    result.loc[observed.index, "TK_Cross"] = signals
    for date, direction in signals.dropna().items():
        result.at[date, "Cross_Strength"] = _classify_cross(
            str(direction),
            float(result.at[date, "Close"]),
            float(result.at[date, "Senkou_A"]),
            float(result.at[date, "Senkou_B"]),
        )

    numeric_columns = [c for c in ICHIMOKU_COLUMNS if c not in ("TK_Cross", "Cross_Strength", "IsFuture")]
    result[numeric_columns] = result[numeric_columns].apply(pd.to_numeric, errors="coerce")
    return result


def _cloud_regime(span_a: Any, span_b: Any) -> str:
    if pd.isna(span_a) or pd.isna(span_b):
        return "unavailable"
    if np.isclose(float(span_a), float(span_b), rtol=1e-9, atol=1e-12):
        return "flat"
    return "bullish" if float(span_a) > float(span_b) else "bearish"


def latest_ichimoku_state(data: pd.DataFrame, ticker: str, timeframe: str = "Daily") -> dict[str, Any]:
    """Extract facts used by status cards and the deterministic description."""
    base: dict[str, Any] = {
        "ticker": ticker,
        "timeframe": timeframe.title(),
        "sufficient_data": False,
        "price_position": "unavailable",
        "displayed_cloud": "unavailable",
        "projected_cloud": "unavailable",
        "tk_relation": "unavailable",
        "distance_pct": None,
        "last_cross": None,
    }
    if data.empty or "Close" not in data:
        return base

    observed = data[(~data["IsFuture"].astype(bool)) & data["Close"].notna()]
    if observed.empty:
        return base
    latest = observed.iloc[-1]
    base.update(
        {
            "latest_date": pd.Timestamp(observed.index[-1]),
            "close": float(latest["Close"]),
            "displayed_cloud": _cloud_regime(latest["Senkou_A"], latest["Senkou_B"]),
        }
    )

    if pd.notna(latest["Tenkan"]) and pd.notna(latest["Kijun"]):
        if np.isclose(float(latest["Tenkan"]), float(latest["Kijun"]), rtol=1e-9, atol=1e-12):
            base["tk_relation"] = "equal"
        else:
            base["tk_relation"] = "above" if latest["Tenkan"] > latest["Kijun"] else "below"

    if base["displayed_cloud"] != "unavailable":
        cloud_top = max(float(latest["Senkou_A"]), float(latest["Senkou_B"]))
        cloud_bottom = min(float(latest["Senkou_A"]), float(latest["Senkou_B"]))
        if base["close"] > cloud_top:
            base["price_position"] = "above"
            base["distance_pct"] = (base["close"] / cloud_top - 1.0) * 100.0
        elif base["close"] < cloud_bottom:
            base["price_position"] = "below"
            base["distance_pct"] = (cloud_bottom / base["close"] - 1.0) * 100.0
        else:
            base["price_position"] = "inside"

    projected = data[data["IsFuture"].astype(bool)].dropna(subset=["Senkou_A", "Senkou_B"])
    if not projected.empty:
        final_projection = projected.iloc[-1]
        base["projected_cloud"] = _cloud_regime(final_projection["Senkou_A"], final_projection["Senkou_B"])
        base["projection_date"] = pd.Timestamp(projected.index[-1])

    crosses = observed.dropna(subset=["TK_Cross"])
    if not crosses.empty:
        cross = crosses.iloc[-1]
        cross_date = pd.Timestamp(crosses.index[-1])
        cross_position = observed.index.get_loc(cross_date)
        base["last_cross"] = {
            "direction": str(cross["TK_Cross"]),
            "strength": str(cross["Cross_Strength"]),
            "date": cross_date,
            "age_sessions": int(len(observed) - 1 - cross_position),
            "price": float(cross["Close"]),
        }

    base["sufficient_data"] = base["tk_relation"] != "unavailable"
    return base
