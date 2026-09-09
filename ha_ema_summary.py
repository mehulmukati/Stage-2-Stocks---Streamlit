"""Deterministic descriptions for the latest HA + EMA trend state."""

from __future__ import annotations

from typing import Any, Mapping

HA_EMA_SUMMARY_VERSION = 2


def build_ha_ema_summary(state: Mapping[str, Any]) -> str:
    ticker = str(state.get("ticker", "The stock"))
    if not state.get("sufficient_data"):
        return f"{ticker} does not yet have the 52-week history required for the one-year return gate."

    status = str(state.get("status", "Waiting"))
    conditions = dict(state.get("conditions", {}))
    failed = [name for name, passed in conditions.items() if not passed]
    if status == "BUY planned":
        lead = (
            f"{ticker} has produced a weekend long-entry decision; execution is due at the following week's "
            "first available Open."
        )
    elif status == "EXIT planned":
        lead = (
            f"{ticker} has produced a weekend exit decision from a Heikin-Ashi candle with no upper wick; "
            "execution is due at the following week's first available Open."
        )
    elif status == "Long / Hold":
        lead = f"{ticker} remains in the strategy's long phase pending the next completed week-end review."
    else:
        lead = f"{ticker} is currently waiting in cash."

    if status in {"BUY planned", "EXIT planned", "Long / Hold"} or not failed:
        detail = "The entry filters are shown independently and do not alter the no-upper-wick exit rule."
    else:
        detail = "The latest daily setup fails: " + ", ".join(failed) + "."
    return f"{lead} {detail}"
