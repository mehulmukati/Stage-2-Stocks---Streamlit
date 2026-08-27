"""Deterministic, rule-based English descriptions of Ichimoku state."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

ICHIMOKU_SUMMARY_VERSION = 2


def _serialisable_state(state: Mapping[str, Any]) -> str:
    def default(value: Any) -> str:
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    return json.dumps(dict(state), default=default, sort_keys=True, separators=(",", ":"))


def _choose(options: Sequence[str], state: Mapping[str, Any], category: str) -> str:
    key = f"{state.get('ticker', '')}|{category}|{_serialisable_state(state)}"
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return options[int.from_bytes(digest[:8], "big") % len(options)]


def _periods(age: int, state: Mapping[str, Any]) -> str:
    unit = "week" if str(state.get("timeframe", "Daily")).lower() == "weekly" else "session"
    return f"1 {unit}" if age == 1 else f"{age} {unit}s"


def _price_sentence(state: Mapping[str, Any]) -> str:
    ticker = str(state.get("ticker", "The stock"))
    position = state.get("price_position")
    regime = state.get("displayed_cloud")
    if position == "unavailable" or regime == "unavailable":
        return _choose(
            [
                f"{ticker} does not yet have enough history for a cloud at the latest price date.",
                f"The displayed cloud for {ticker} is not yet available from the loaded history.",
            ],
            state,
            "price_unavailable",
        )

    regime_word = "balanced" if regime == "flat" else str(regime)
    if position == "inside":
        return _choose(
            [
                f"{ticker} is trading inside a {regime_word} cloud, indicating a mixed configuration.",
                f"Price for {ticker} sits within the {regime_word} cloud, where trend direction is less distinct.",
            ],
            state,
            "price_inside",
        )

    distance = float(state.get("distance_pct") or 0.0)
    return _choose(
        [
            f"{ticker} is trading {distance:.1f}% {position} a {regime_word} cloud.",
            f"Price for {ticker} sits {distance:.1f}% {position} the {regime_word} cloud.",
            f"{ticker} remains {position} its {regime_word} cloud by {distance:.1f}%.",
        ],
        state,
        f"price_{position}",
    )


def _tk_sentence(state: Mapping[str, Any]) -> str:
    relation = state.get("tk_relation")
    relation_text = {
        "above": "Tenkan-sen is above Kijun-sen",
        "below": "Tenkan-sen is below Kijun-sen",
        "equal": "Tenkan-sen and Kijun-sen are level",
    }.get(relation)
    if relation_text is None:
        return "There is not yet enough history to compare Tenkan-sen with Kijun-sen."

    cross = state.get("last_cross")
    if not cross:
        return _choose(
            [
                f"{relation_text}, with no valid crossover available in the loaded history.",
                f"{relation_text}; the loaded period contains no confirmed Tenkan–Kijun crossover.",
            ],
            state,
            "tk_no_cross",
        )

    direction = str(cross["direction"])
    strength = str(cross["strength"])
    strength_text = "an unclassified" if strength == "unclassified" else f"a {strength}"
    event_phrase = "An unclassified" if strength == "unclassified" else f"A {strength}"
    current_alignment = {
        "above": "Tenkan-sen is now above Kijun-sen",
        "below": "Tenkan-sen is now below Kijun-sen",
        "equal": "the two lines are now level",
    }[str(relation)]
    elapsed = _periods(int(cross["age_sessions"]), state)
    return _choose(
        [
            f"{relation_text}; the latest event was {strength_text} crossover {elapsed} ago.",
            f"The latest Tenkan–Kijun event was {strength_text} crossover {elapsed} ago, and {current_alignment}.",
            f"{event_phrase} Tenkan–Kijun crossover occurred {elapsed} ago; {current_alignment}.",
        ],
        state,
        f"tk_{direction}_{strength}",
    )


def _projection_sentence(state: Mapping[str, Any]) -> str:
    regime = state.get("projected_cloud")
    if regime == "unavailable":
        return "The forward cloud is not yet available from the loaded history."
    if regime == "flat":
        return "The projected cloud is currently balanced, with both boundaries at approximately the same level."
    return _choose(
        [
            f"The projected cloud remains {regime}.",
            f"The forward cloud is also {regime}.",
            f"The 26-period cloud projection is {regime}.",
        ],
        state,
        f"projection_{regime}",
    )


def build_ichimoku_summary(state: Mapping[str, Any]) -> str:
    """Return a concise description whose facts are fully determined by ``state``."""
    return " ".join((_price_sentence(state), _tk_sentence(state), _projection_sentence(state)))
