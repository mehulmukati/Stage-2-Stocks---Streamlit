"""Built-in quant strategies and the extensible portfolio adapter registry."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd

from ha_ema_engine import HAEMAStrategyConfig, _cash_market_holidays, compute_ha_ema_signals
from ichimoku_engine import compute_ichimoku
from ichimoku_strategy import IchimokuStrategyConfig, compute_ichimoku_signals, strategy_preset
from quant_strategy import (
    SIGNAL_COLUMNS,
    StrategyDefinition,
    diagnostic_dict,
    next_session_dates,
    validate_signal_frame,
)

QUANT_STRATEGY_REGISTRY_VERSION = 3


def _clean_ohlcv(data: pd.DataFrame) -> pd.DataFrame:
    clean = data.copy()
    clean.index = pd.DatetimeIndex(pd.to_datetime(clean.index, errors="coerce"))
    clean = clean[~clean.index.isna()].sort_index()
    if clean.index.tz is not None:
        clean.index = clean.index.tz_localize(None)
    return clean[~clean.index.duplicated(keep="last")]


class HAEMAAdapter:
    def __init__(self, config: HAEMAStrategyConfig | None = None):
        self.config = config or HAEMAStrategyConfig()
        self.definition = StrategyDefinition(
            "ha_ema",
            "HA + EMA Trend",
            "Weekly",
            "Heikin-Ashi no-wick entry with EMA, volume and return gates.",
            ("Open", "High", "Low", "Close", "Volume"),
            True,
            max(self.config.ema_slow, self.config.average_volume_length, self.config.return_length),
        )

    def compute(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        daily = _clean_ohlcv(ohlcv)
        signals = compute_ha_ema_signals(daily, self.config)
        execution = next_session_dates(signals.index, daily.index)
        close = signals["Close"].replace(0, np.nan)
        score = (
            signals["Return_Pct"].fillna(-1e6)
            + 100 * (signals["EMA_Fast"] - signals["EMA_Slow"]) / close
            + 25 * (signals["HA_Close"] - signals["HA_Open"]) / close
        )
        diagnostic_fields = (
            "Bullish_HA",
            "No_Lower_Wick",
            "Previous_Two_Wicks",
            "Above_Fast_EMA",
            "Above_Slow_EMA",
            "EMA_Aligned",
            "Volume_Filter",
            "Return_Filter",
        )
        rows = []
        events = signals[signals["Signal"].ne("")]
        for decision, row in events.iterrows():
            rows.append(
                {
                    "Decision_Date": decision,
                    "Execution_Date": execution.loc[pd.Timestamp(decision)],
                    "Ready": bool(pd.notna(row["Return_Pct"]) and pd.notna(row["Average_Volume"])),
                    "Entry_Event": row["Signal"] == "BUY",
                    "Exit_Event": row["Signal"] == "EXIT",
                    "Hold_Eligible": bool(row["Position"]),
                    "Score": float(score.loc[decision]) if pd.notna(score.loc[decision]) else -1e6,
                    "Entry_Reason": "HA no-lower-wick trend + aligned EMAs + volume/return gates",
                    "Exit_Reason": "Heikin-Ashi candle has no upper wick",
                    "Diagnostics": diagnostic_dict(row, diagnostic_fields),
                }
            )
        return validate_signal_frame(pd.DataFrame(rows, columns=SIGNAL_COLUMNS))

    def compute_many(self, all_ohlcv: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Vectorized weekly calculation for portfolio-sized universes."""
        if not all_ohlcv:
            return {}
        pieces = []
        for symbol, frame in all_ohlcv.items():
            clean = _clean_ohlcv(frame)
            if clean.empty:
                continue
            piece = clean[["Open", "High", "Low", "Close", "Volume"]].copy()
            piece["Symbol"] = symbol
            piece["Decision_Date"] = piece.index
            piece["Period"] = piece.index.to_period("W-FRI")
            pieces.append(piece.reset_index(drop=True))
        if not pieces:
            return {symbol: pd.DataFrame(columns=SIGNAL_COLUMNS) for symbol in all_ohlcv}

        daily = pd.concat(pieces, ignore_index=True)
        weekly = (
            daily.groupby(["Symbol", "Period"], sort=True, observed=True)
            .agg(
                Open=("Open", "first"),
                High=("High", "max"),
                Low=("Low", "min"),
                Close=("Close", "last"),
                Volume=("Volume", "sum"),
                Decision_Date=("Decision_Date", "last"),
            )
            .reset_index()
        )
        grouped = weekly.groupby("Symbol", sort=False)
        weekly["Decision_Eligible"] = True
        final_mask = ~weekly["Symbol"].duplicated(keep="last")
        holidays = _cash_market_holidays()
        as_of = pd.Timestamp.now().tz_localize(None).normalize()
        for index in weekly.index[final_mask]:
            expected = pd.Timestamp(weekly.at[index, "Period"].end_time).normalize()
            while expected.weekday() >= 5 or expected in holidays:
                expected -= pd.Timedelta(days=1)
            observed = pd.Timestamp(weekly.at[index, "Decision_Date"]).normalize()
            weekly.at[index, "Decision_Eligible"] = as_of >= expected and observed >= expected

        weekly["HA_Close"] = weekly[["Open", "High", "Low", "Close"]].mean(axis=1)
        ha_open = np.empty(len(weekly), dtype=float)
        open_values = weekly["Open"].to_numpy(dtype=float)
        close_values = weekly["Close"].to_numpy(dtype=float)
        ha_close_values = weekly["HA_Close"].to_numpy(dtype=float)
        for indices in grouped.indices.values():
            positions = np.asarray(indices, dtype=int)
            first = positions[0]
            ha_open[first] = (open_values[first] + close_values[first]) / 2.0
            for previous, current in zip(positions[:-1], positions[1:]):
                ha_open[current] = (ha_open[previous] + ha_close_values[previous]) / 2.0
        weekly["HA_Open"] = ha_open
        weekly["HA_High"] = weekly[["High", "HA_Open", "HA_Close"]].max(axis=1)
        weekly["HA_Low"] = weekly[["Low", "HA_Open", "HA_Close"]].min(axis=1)
        weekly["EMA_Fast"] = grouped["Close"].transform(
            lambda series: series.ewm(span=self.config.ema_fast, adjust=False).mean()
        )
        weekly["EMA_Slow"] = grouped["Close"].transform(
            lambda series: series.ewm(span=self.config.ema_slow, adjust=False).mean()
        )
        weekly["Average_Volume"] = grouped["Volume"].transform(
            lambda series: series.rolling(
                self.config.average_volume_length,
                min_periods=self.config.average_volume_length,
            ).mean()
        )
        shifted_close = grouped["Close"].shift(self.config.return_length)
        weekly["Return_Pct"] = (weekly["Close"] / shifted_close - 1.0) * 100.0
        weekly["Bullish_HA"] = weekly["HA_Close"] > weekly["HA_Open"]
        weekly["No_Lower_Wick"] = (weekly["HA_Open"] - weekly["HA_Low"]).abs() <= self.config.wick_tolerance
        weekly["No_Upper_Wick"] = (weekly["HA_Open"] - weekly["HA_High"]).abs() <= self.config.wick_tolerance
        body_top = weekly[["HA_Open", "HA_Close"]].max(axis=1)
        body_bottom = weekly[["HA_Open", "HA_Close"]].min(axis=1)
        weekly["Has_Lower_Wick"] = weekly["HA_Low"] < body_bottom - self.config.wick_tolerance
        weekly["Has_Upper_Wick"] = weekly["HA_High"] > body_top + self.config.wick_tolerance
        weekly["Two_Wicks"] = weekly["Has_Lower_Wick"] & weekly["Has_Upper_Wick"]
        weekly["Previous_Two_Wicks"] = grouped["Two_Wicks"].shift(1, fill_value=False)
        weekly["Above_Fast_EMA"] = weekly["Close"] > weekly["EMA_Fast"]
        weekly["Above_Slow_EMA"] = weekly["Close"] > weekly["EMA_Slow"]
        weekly["EMA_Aligned"] = weekly["EMA_Fast"] > weekly["EMA_Slow"]
        weekly["Volume_Filter"] = weekly["Average_Volume"] > self.config.minimum_average_volume
        weekly["Return_Filter"] = weekly["Return_Pct"].gt(self.config.minimum_return_pct).fillna(False)
        weekly["Long_Condition"] = (
            weekly["Bullish_HA"]
            & weekly["No_Lower_Wick"]
            & weekly["Previous_Two_Wicks"]
            & weekly["Above_Fast_EMA"]
            & weekly["Above_Slow_EMA"]
            & weekly["EMA_Aligned"]
            & weekly["Volume_Filter"]
            & weekly["Return_Filter"]
        )

        signal = np.full(len(weekly), "", dtype=object)
        position = np.zeros(len(weekly), dtype=bool)
        eligible_values = weekly["Decision_Eligible"].to_numpy(dtype=bool)
        long_values = weekly["Long_Condition"].to_numpy(dtype=bool)
        exit_values = weekly["No_Upper_Wick"].to_numpy(dtype=bool)
        for indices in grouped.indices.values():
            held = False
            for index in indices:
                if eligible_values[index]:
                    if held and exit_values[index]:
                        signal[index] = "EXIT"
                        held = False
                    elif not held and long_values[index]:
                        signal[index] = "BUY"
                        held = True
                position[index] = held
        weekly["Signal"] = signal
        weekly["Position"] = position

        diagnostics = (
            "Bullish_HA",
            "No_Lower_Wick",
            "Previous_Two_Wicks",
            "Above_Fast_EMA",
            "Above_Slow_EMA",
            "EMA_Aligned",
            "Volume_Filter",
            "Return_Filter",
        )
        output = {symbol: pd.DataFrame(columns=SIGNAL_COLUMNS) for symbol in all_ohlcv}
        events = weekly[weekly["Signal"].ne("")]
        for symbol, symbol_events in events.groupby("Symbol", sort=False):
            source_dates = all_ohlcv[symbol].index
            executions = next_session_dates(symbol_events["Decision_Date"], source_dates)
            rows = []
            for _, row in symbol_events.iterrows():
                decision = pd.Timestamp(row["Decision_Date"])
                close = float(row["Close"])
                score = (
                    float(row["Return_Pct"])
                    + 100 * (float(row["EMA_Fast"]) - float(row["EMA_Slow"])) / close
                    + 25 * (float(row["HA_Close"]) - float(row["HA_Open"])) / close
                )
                rows.append(
                    {
                        "Decision_Date": decision,
                        "Execution_Date": executions.loc[decision],
                        "Ready": bool(pd.notna(row["Return_Pct"]) and pd.notna(row["Average_Volume"])),
                        "Entry_Event": row["Signal"] == "BUY",
                        "Exit_Event": row["Signal"] == "EXIT",
                        "Hold_Eligible": bool(row["Position"]),
                        "Score": score,
                        "Entry_Reason": "HA no-lower-wick trend + aligned EMAs + volume/return gates",
                        "Exit_Reason": "Heikin-Ashi candle has no upper wick",
                        "Diagnostics": diagnostic_dict(row, diagnostics),
                    }
                )
            output[symbol] = validate_signal_frame(pd.DataFrame(rows, columns=SIGNAL_COLUMNS))
        return output


class IchimokuAdapter:
    def __init__(
        self, preset: str = "Balanced", timeframe: str = "Weekly", config: IchimokuStrategyConfig | None = None
    ):
        self.config = config or strategy_preset(preset)
        self.timeframe = timeframe.title()
        self.definition = StrategyDefinition(
            "ichimoku",
            f"Ichimoku · {self.config.name}",
            self.timeframe,
            "Point-in-time Ichimoku structure with next-session execution.",
            ("Open", "High", "Low", "Close"),
            False,
            self.config.span_b_period + self.config.displacement,
        )

    def compute(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        daily = _clean_ohlcv(ohlcv)
        indicator = compute_ichimoku(
            daily,
            span_b_period=self.config.span_b_period,
            displacement=self.config.displacement,
            timeframe=self.timeframe,
        )
        signals = compute_ichimoku_signals(indicator, self.config)
        execution = next_session_dates(signals.index, daily.index)
        close = signals["Close"].replace(0, np.nan)
        cloud_top = signals["Cloud_Top"]
        score = (
            100 * (signals["Close"] - cloud_top) / close
            + 50 * (signals["Tenkan"] - signals["Kijun"]) / close
            + 25 * (signals["Forward_Senkou_A"] - signals["Forward_Senkou_B"]) / close
        ).fillna(-1e6)
        diagnostic_fields = (
            "Price_Position",
            "TK_Cross",
            "Cross_Strength",
            "Entry_Qualifiers",
            "Strategy_Ready",
            "Chikou_Above_Close",
            "Chikou_Above_High",
            "Extension_From_Kijun_Pct",
        )
        rows = []
        events = signals[signals["Signal"].ne("")]
        for decision, row in events.iterrows():
            rows.append(
                {
                    "Decision_Date": decision,
                    "Execution_Date": execution.loc[pd.Timestamp(decision)],
                    "Ready": bool(row["Strategy_Ready"]),
                    "Entry_Event": row["Signal"] == "BUY",
                    "Exit_Event": row["Signal"] == "EXIT",
                    "Hold_Eligible": bool(row["Position"]),
                    "Score": float(score.loc[decision]),
                    "Entry_Reason": str(row["Signal_Reason"]) if row["Signal"] == "BUY" else self.config.entry_event,
                    "Exit_Reason": str(row["Signal_Reason"]) if row["Signal"] == "EXIT" else self.config.exit_event,
                    "Diagnostics": diagnostic_dict(row, diagnostic_fields),
                }
            )
        return validate_signal_frame(pd.DataFrame(rows, columns=SIGNAL_COLUMNS))


STRATEGY_KEYS = {"HA + EMA Trend": "ha_ema", "Ichimoku": "ichimoku"}


def build_strategy(key: str, settings: dict[str, Any] | None = None):
    settings = settings or {}
    normalized = STRATEGY_KEYS.get(key, key)
    if normalized == "ha_ema":
        allowed = {field.name for field in __import__("dataclasses").fields(HAEMAStrategyConfig)}
        config = HAEMAStrategyConfig(**{k: v for k, v in settings.items() if k in allowed})
        return HAEMAAdapter(config)
    if normalized == "ichimoku":
        return IchimokuAdapter(preset=settings.get("preset", "Balanced"), timeframe=settings.get("timeframe", "Weekly"))
    raise ValueError(f"Unknown quant strategy: {key}")


def strategy_metadata(strategy) -> dict[str, Any]:
    return {"definition": asdict(strategy.definition), "config": asdict(strategy.config)}
