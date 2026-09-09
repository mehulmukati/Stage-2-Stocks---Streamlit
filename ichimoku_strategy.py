"""Configurable, point-in-time Ichimoku signals and replay comparisons."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from typing import Any

import numpy as np
import pandas as pd

from strategy_replay import ExecutionPolicy, ReplayConfig, replay_single_stock

ICHIMOKU_STRATEGY_VERSION = 1

ENTRY_TK_CROSS = "Bullish TK cross"
ENTRY_KUMO_BREAKOUT = "Bullish Kumo breakout"
ENTRY_COMPLETE_STRUCTURE = "Complete bullish structure"

EXIT_TENKAN = "Close below Tenkan"
EXIT_KIJUN = "Close below Kijun"
EXIT_TK_CROSS = "Bearish TK cross"
EXIT_KUMO_ENTRY = "Close enters Kumo"
EXIT_KUMO_BOTTOM = "Close below entire Kumo"

CHIKOU_NONE = "Not required"
CHIKOU_CLOSE = "Above historical Close"
CHIKOU_HIGH = "Above historical High"

KIJUN_NONE = "Not required"
KIJUN_NON_FALLING = "Non-falling"
KIJUN_RISING = "Rising"

PRESET_NAMES = ("Aggressive", "Balanced", "Conservative")


@dataclass(frozen=True)
class IchimokuStrategyConfig:
    """Resolved rules for one long-only Ichimoku strategy."""

    name: str = "Balanced"
    entry_event: str = ENTRY_COMPLETE_STRUCTURE
    price_location: str = "Above cloud"
    cross_strength: str = "Strong and neutral"
    require_tk_alignment: bool = True
    require_forward_bullish: bool = True
    chikou_mode: str = CHIKOU_HIGH
    kijun_direction: str = KIJUN_NONE
    kijun_lookback: int = 1
    maximum_extension_pct: float | None = None
    entry_confirmation: int = 1
    exit_event: str = EXIT_KUMO_ENTRY
    exit_confirmation: int = 1
    fixed_stop_loss_pct: float | None = None
    trailing_stop_pct: float | None = None
    maximum_holding_periods: int | None = None
    displacement: int = 26
    span_b_period: int = 52


def strategy_preset(name: str) -> IchimokuStrategyConfig:
    """Return one canonical preset."""

    if name == "Aggressive":
        return IchimokuStrategyConfig(
            name=name,
            entry_event=ENTRY_TK_CROSS,
            price_location="Inside or above cloud",
            cross_strength="Strong and neutral",
            require_tk_alignment=True,
            require_forward_bullish=False,
            chikou_mode=CHIKOU_NONE,
            kijun_direction=KIJUN_NONE,
            entry_confirmation=1,
            exit_event=EXIT_KIJUN,
            exit_confirmation=1,
        )
    if name == "Conservative":
        return IchimokuStrategyConfig(
            name=name,
            entry_event=ENTRY_KUMO_BREAKOUT,
            price_location="Above cloud",
            require_tk_alignment=True,
            require_forward_bullish=True,
            chikou_mode=CHIKOU_HIGH,
            kijun_direction=KIJUN_NON_FALLING,
            kijun_lookback=1,
            entry_confirmation=2,
            exit_event=EXIT_KUMO_BOTTOM,
            exit_confirmation=1,
        )
    if name != "Balanced":
        raise ValueError(f"Unknown Ichimoku strategy preset: {name}")
    return IchimokuStrategyConfig()


def _comparable_config(config: IchimokuStrategyConfig) -> dict[str, Any]:
    values = asdict(config)
    values.pop("name", None)
    return values


def matching_preset(config: IchimokuStrategyConfig) -> str | None:
    """Return a canonical preset name when every resolved rule matches."""

    values = _comparable_config(config)
    for name in PRESET_NAMES:
        if values == _comparable_config(strategy_preset(name)):
            return name
    return None


def strategy_fingerprint(config: IchimokuStrategyConfig) -> str:
    payload = json.dumps(_comparable_config(config), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def resolved_strategy_name(config: IchimokuStrategyConfig) -> str:
    return matching_preset(config) or "Custom"


def _validate_config(config: IchimokuStrategyConfig) -> None:
    if config.entry_event not in {ENTRY_TK_CROSS, ENTRY_KUMO_BREAKOUT, ENTRY_COMPLETE_STRUCTURE}:
        raise ValueError("Unsupported Ichimoku entry event")
    if config.exit_event not in {EXIT_TENKAN, EXIT_KIJUN, EXIT_TK_CROSS, EXIT_KUMO_ENTRY, EXIT_KUMO_BOTTOM}:
        raise ValueError("Unsupported Ichimoku exit event")
    if config.chikou_mode not in {CHIKOU_NONE, CHIKOU_CLOSE, CHIKOU_HIGH}:
        raise ValueError("Unsupported Chikou confirmation")
    if config.kijun_direction not in {KIJUN_NONE, KIJUN_NON_FALLING, KIJUN_RISING}:
        raise ValueError("Unsupported Kijun direction")
    if min(config.entry_confirmation, config.exit_confirmation, config.kijun_lookback) <= 0:
        raise ValueError("Confirmation periods and Kijun lookback must be positive")
    for value in (config.maximum_extension_pct, config.fixed_stop_loss_pct, config.trailing_stop_pct):
        if value is not None and value <= 0:
            raise ValueError("Percentage filters and safety exits must be positive")
    if config.maximum_holding_periods is not None and config.maximum_holding_periods <= 0:
        raise ValueError("Maximum holding periods must be positive")


def _observed(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data.copy()
    if "IsFuture" in data:
        return data[(~data["IsFuture"].fillna(False).astype(bool)) & data["Close"].notna()].copy()
    return data[data["Close"].notna()].copy()


def _cross_allowed(data: pd.DataFrame, config: IchimokuStrategyConfig) -> pd.Series:
    bullish = data["TK_Cross"].eq("bullish")
    if config.cross_strength == "Strong only":
        return bullish & data["Cross_Strength"].eq("strong bullish")
    if config.cross_strength == "Any bullish cross":
        return bullish
    return bullish & data["Cross_Strength"].isin(["strong bullish", "neutral bullish"])


def _entry_qualifiers(data: pd.DataFrame, config: IchimokuStrategyConfig) -> pd.Series:
    qualifies = pd.Series(True, index=data.index, dtype=bool)
    if config.price_location == "Above cloud":
        qualifies &= data["Price_Position"].eq("above")
    elif config.price_location == "Inside or above cloud":
        qualifies &= data["Price_Position"].isin(["inside", "above"])
    if config.require_tk_alignment:
        qualifies &= data["Tenkan"].gt(data["Kijun"])
    if config.require_forward_bullish:
        qualifies &= data["Forward_Senkou_A"].gt(data["Forward_Senkou_B"])
    if config.chikou_mode == CHIKOU_CLOSE:
        qualifies &= data["Chikou_Above_Close"]
    elif config.chikou_mode == CHIKOU_HIGH:
        qualifies &= data["Chikou_Above_High"]
    if config.kijun_direction == KIJUN_NON_FALLING:
        qualifies &= data["Kijun"].ge(data["Kijun"].shift(config.kijun_lookback))
    elif config.kijun_direction == KIJUN_RISING:
        qualifies &= data["Kijun"].gt(data["Kijun"].shift(config.kijun_lookback))
    if config.maximum_extension_pct is not None:
        qualifies &= data["Extension_From_Kijun_Pct"].le(config.maximum_extension_pct)
    return qualifies.fillna(False)


def _technical_exit(row: pd.Series, config: IchimokuStrategyConfig) -> tuple[bool, str]:
    close = float(row["Close"])
    if config.exit_event == EXIT_TENKAN:
        return pd.notna(row["Tenkan"]) and close < float(row["Tenkan"]), EXIT_TENKAN
    if config.exit_event == EXIT_KIJUN:
        return pd.notna(row["Kijun"]) and close < float(row["Kijun"]), EXIT_KIJUN
    if config.exit_event == EXIT_TK_CROSS:
        return row["TK_Cross"] == "bearish", EXIT_TK_CROSS
    if config.exit_event == EXIT_KUMO_ENTRY:
        return pd.notna(row["Cloud_Top"]) and close <= float(row["Cloud_Top"]), EXIT_KUMO_ENTRY
    return pd.notna(row["Cloud_Bottom"]) and close < float(row["Cloud_Bottom"]), EXIT_KUMO_BOTTOM


def compute_ichimoku_signals(
    data: pd.DataFrame,
    config: IchimokuStrategyConfig | None = None,
) -> pd.DataFrame:
    """Build position-aware BUY/EXIT events without reading future observations."""

    config = config or strategy_preset("Balanced")
    _validate_config(config)
    result = _observed(data)
    if result.empty:
        return result

    required = {"Open", "High", "Low", "Close", "Tenkan", "Kijun", "Senkou_A", "Senkou_B", "TK_Cross"}
    missing = required.difference(result.columns)
    if missing:
        raise ValueError(f"Missing Ichimoku columns: {', '.join(sorted(missing))}")

    result["Cloud_Top"] = result[["Senkou_A", "Senkou_B"]].max(axis=1, skipna=False)
    result["Cloud_Bottom"] = result[["Senkou_A", "Senkou_B"]].min(axis=1, skipna=False)
    result["Price_Position"] = np.select(
        [result["Close"].gt(result["Cloud_Top"]), result["Close"].lt(result["Cloud_Bottom"])],
        ["above", "below"],
        default="inside",
    )
    no_cloud = result[["Cloud_Top", "Cloud_Bottom"]].isna().any(axis=1)
    result.loc[no_cloud, "Price_Position"] = "unavailable"

    # These are the values calculated now and drawn displacement periods later.
    # They are not values read from future rows.
    result["Forward_Senkou_A"] = (result["Tenkan"] + result["Kijun"]) / 2.0
    result["Forward_Senkou_B"] = (
        result["High"].rolling(config.span_b_period).max() + result["Low"].rolling(config.span_b_period).min()
    ) / 2.0
    result["Chikou_Above_Close"] = result["Close"].gt(result["Close"].shift(config.displacement))
    result["Chikou_Above_High"] = result["Close"].gt(result["High"].shift(config.displacement))
    result["Extension_From_Kijun_Pct"] = (result["Close"] / result["Kijun"] - 1.0) * 100.0

    ready = result[["Cloud_Top", "Cloud_Bottom", "Tenkan", "Kijun"]].notna().all(axis=1)
    if config.require_forward_bullish:
        ready &= result[["Forward_Senkou_A", "Forward_Senkou_B"]].notna().all(axis=1)
    if config.chikou_mode != CHIKOU_NONE:
        reference = "High" if config.chikou_mode == CHIKOU_HIGH else "Close"
        ready &= result[reference].shift(config.displacement).notna()
    if config.kijun_direction != KIJUN_NONE:
        ready &= result["Kijun"].shift(config.kijun_lookback).notna()
    result["Strategy_Ready"] = ready

    qualifiers = _entry_qualifiers(result, config) & ready
    result["Entry_Qualifiers"] = qualifiers
    tk_event = _cross_allowed(result, config)
    breakout_event = result["Price_Position"].eq("above") & ~result["Price_Position"].shift(1).eq("above")
    structure_event = qualifiers & ~qualifiers.shift(1, fill_value=False)
    if config.entry_event == ENTRY_TK_CROSS:
        event = tk_event
    elif config.entry_event == ENTRY_KUMO_BREAKOUT:
        event = breakout_event
    else:
        event = structure_event
    result["Entry_Event"] = event & qualifiers & ready

    held = False
    entry_armed = False
    entry_count = 0
    exit_count = 0
    pending_entry_execution = False
    entry_price: float | None = None
    highest_close: float | None = None
    held_periods = 0
    signals: list[str] = []
    reasons: list[str] = []
    positions: list[bool] = []

    for _, row in result.iterrows():
        if pending_entry_execution:
            entry_price = float(row["Open"])
            highest_close = float(row["Close"])
            held_periods = 1
            pending_entry_execution = False
        elif held and entry_price is not None:
            held_periods += 1
            highest_close = max(float(highest_close or row["Close"]), float(row["Close"]))

        signal = ""
        reason = ""
        if held:
            technical, technical_reason = _technical_exit(row, config)
            if config.exit_event == EXIT_TK_CROSS and exit_count > 0:
                technical = bool(
                    row["TK_Cross"] == "bearish"
                    or (pd.notna(row["Tenkan"]) and pd.notna(row["Kijun"]) and row["Tenkan"] < row["Kijun"])
                )
            exit_count = exit_count + 1 if technical else 0
            safety_reason = ""
            if entry_price is not None and config.fixed_stop_loss_pct is not None:
                if float(row["Close"]) <= entry_price * (1.0 - config.fixed_stop_loss_pct / 100.0):
                    safety_reason = f"Fixed stop-loss ({config.fixed_stop_loss_pct:g}%)"
            if not safety_reason and highest_close is not None and config.trailing_stop_pct is not None:
                if float(row["Close"]) <= highest_close * (1.0 - config.trailing_stop_pct / 100.0):
                    safety_reason = f"Trailing stop ({config.trailing_stop_pct:g}%)"
            if not safety_reason and config.maximum_holding_periods is not None:
                if held_periods >= config.maximum_holding_periods:
                    safety_reason = f"Maximum holding period ({config.maximum_holding_periods})"
            if safety_reason or exit_count >= config.exit_confirmation:
                signal = "EXIT"
                reason = safety_reason or technical_reason
                held = False
                exit_count = 0
                entry_price = None
                highest_close = None
                held_periods = 0
        else:
            if bool(row["Entry_Event"]):
                entry_armed = True
                entry_count = 1
            elif entry_armed and bool(row["Entry_Qualifiers"]):
                entry_count += 1
            elif entry_armed:
                entry_armed = False
                entry_count = 0
            if entry_armed and entry_count >= config.entry_confirmation:
                signal = "BUY"
                reason = config.entry_event
                held = True
                pending_entry_execution = True
                entry_armed = False
                entry_count = 0

        signals.append(signal)
        reasons.append(reason)
        positions.append(held)

    result["Signal"] = signals
    result["Signal_Reason"] = reasons
    result["Position"] = positions
    return result


def describe_strategy(config: IchimokuStrategyConfig) -> str:
    """Return a concise, deterministic explanation of the resolved rules."""

    entry_parts: list[str] = []
    if config.entry_event == ENTRY_TK_CROSS:
        entry_parts.append(f"a {config.cross_strength.lower()} bullish Tenkan–Kijun cross")
        entry_parts.append(f"while price is {config.price_location.lower()}")
    elif config.entry_event == ENTRY_KUMO_BREAKOUT:
        entry_parts.append("a close breaks from inside/below to above the Kumo")
    else:
        entry_parts.append("the complete selected bullish structure first becomes valid")
        entry_parts.append(f"with price {config.price_location.lower()}")
    if config.require_tk_alignment and config.entry_event != ENTRY_TK_CROSS:
        entry_parts.append("Tenkan above Kijun")
    if config.require_forward_bullish:
        entry_parts.append("a bullish forward Kumo")
    if config.chikou_mode == CHIKOU_CLOSE:
        entry_parts.append(f"Close above the Close {config.displacement} periods earlier")
    elif config.chikou_mode == CHIKOU_HIGH:
        entry_parts.append(f"Close above the High {config.displacement} periods earlier")
    if config.kijun_direction != KIJUN_NONE:
        entry_parts.append(f"Kijun {config.kijun_direction.lower()}")
    if config.maximum_extension_pct is not None:
        entry_parts.append(f"price no more than {config.maximum_extension_pct:g}% above Kijun")
    confirmation = "the qualifying close"
    if config.entry_confirmation > 1:
        confirmation = f"{config.entry_confirmation} consecutive qualifying closes"
    exit_text = config.exit_event.lower()
    if config.exit_confirmation > 1:
        exit_text += f" for {config.exit_confirmation} consecutive periods"
    safety: list[str] = []
    if config.fixed_stop_loss_pct is not None:
        safety.append(f"{config.fixed_stop_loss_pct:g}% fixed stop")
    if config.trailing_stop_pct is not None:
        safety.append(f"{config.trailing_stop_pct:g}% trailing stop")
    if config.maximum_holding_periods is not None:
        safety.append(f"{config.maximum_holding_periods}-period maximum hold")
    safety_text = f" Safety overrides: {', '.join(safety)}." if safety else ""
    return (
        f"Enter at the next Open after {confirmation} confirms "
        f"{'; '.join(entry_parts)}. Exit at the next Open after {exit_text}.{safety_text}"
    )


def strategy_metadata(config: IchimokuStrategyConfig) -> dict[str, Any]:
    values = asdict(replace(config, name=resolved_strategy_name(config)))
    values["fingerprint"] = strategy_fingerprint(config)
    return values


def build_strategy_comparison(
    data: pd.DataFrame,
    custom_config: IchimokuStrategyConfig,
    replay_config: ReplayConfig,
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]], pd.Timestamp | None]:
    """Replay all presets plus Custom on one common cash-start calendar."""

    configs = {name: strategy_preset(name) for name in PRESET_NAMES}
    configs["Custom"] = replace(custom_config, name="Custom")
    signals = {name: compute_ichimoku_signals(data, config) for name, config in configs.items()}
    if not signals or any(frame.empty for frame in signals.values()):
        return pd.DataFrame(), {}, None

    common_index = next(iter(signals.values())).index
    common_ready = pd.Series(True, index=common_index)
    for frame in signals.values():
        common_ready &= frame["Strategy_Ready"].reindex(common_index, fill_value=False)
    ready_dates = common_ready[common_ready].index
    if ready_dates.empty:
        return pd.DataFrame(), {}, None
    common_start = pd.Timestamp(ready_dates[0])
    comparison_index = common_index[common_index >= common_start]
    comparison = pd.DataFrame(index=comparison_index)
    replays: dict[str, dict[str, Any]] = {}
    for name, frame in signals.items():
        replay = replay_single_stock(
            frame.loc[common_start:],
            replay_config,
            ExecutionPolicy(execution_date_column="Execution_Date"),
        )
        replays[name] = {"signals": frame, "replay": replay, "config": configs[name]}
        values = pd.Series(float(replay_config.initial_capital), index=comparison_index)
        equity = replay.get("equity", pd.DataFrame())
        if not equity.empty:
            overlap = values.index.intersection(equity.index)
            values.loc[overlap] = equity.loc[overlap, "Pre_Tax_Value"].astype(float)
        comparison[name] = values.ffill()
    return comparison, replays, common_start
