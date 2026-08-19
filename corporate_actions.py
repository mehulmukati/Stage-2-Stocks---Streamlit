"""Corporate-action registry loading and validation.

The registry is deliberately small and reviewable.  It complements historical
index compositions: compositions control eligibility, while this module tells
the portfolio engine how an existing holding changes identity.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache

import pandas as pd

REGISTRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "corporate_actions.json")
SUPPORTED_EVENT_TYPES = {"merger", "symbol_change"}


@lru_cache(maxsize=1)
def load_corporate_actions() -> list[dict]:
    """Load and validate the effective-dated corporate-action registry."""
    if not os.path.exists(REGISTRY_PATH):
        return []

    with open(REGISTRY_PATH, encoding="utf-8") as fh:
        raw = json.load(fh)

    actions = raw.get("actions", raw) if isinstance(raw, dict) else raw
    if not isinstance(actions, list):
        raise ValueError("corporate_actions.json must contain an 'actions' list")

    validated: list[dict] = []
    for position, action in enumerate(actions, start=1):
        if not isinstance(action, dict):
            raise ValueError(f"Corporate action #{position} must be an object")
        event_type = str(action.get("event_type", "")).strip().lower()
        old_symbol = str(action.get("old_symbol", "")).strip().upper()
        successor_symbol = str(action.get("successor_symbol", "")).strip().upper()
        if event_type not in SUPPORTED_EVENT_TYPES:
            raise ValueError(f"Unsupported corporate action type: {event_type!r}")
        if not old_symbol or not successor_symbol or old_symbol == successor_symbol:
            raise ValueError(f"Invalid corporate-action symbols in record #{position}")
        effective_date = pd.Timestamp(action.get("effective_date")).normalize()
        last_trading_date = pd.Timestamp(action.get("last_trading_date")).normalize()
        if last_trading_date >= effective_date:
            raise ValueError(f"last_trading_date must precede effective_date for {old_symbol}")
        share_ratio = float(action.get("share_ratio", 1.0))
        if share_ratio <= 0:
            raise ValueError(f"share_ratio must be positive for {old_symbol}")

        validated.append(
            {
                **action,
                "event_type": event_type,
                "old_symbol": old_symbol,
                "successor_symbol": successor_symbol,
                "effective_date": effective_date,
                "last_trading_date": last_trading_date,
                "share_ratio": share_ratio,
            }
        )

    return sorted(validated, key=lambda item: item["effective_date"])


@lru_cache(maxsize=1)
def load_index_replacements() -> list[dict]:
    """Load effective-dated index replacements used by composition rebuilds."""
    if not os.path.exists(REGISTRY_PATH):
        return []
    with open(REGISTRY_PATH, encoding="utf-8") as fh:
        raw = json.load(fh)
    replacements = raw.get("index_replacements", []) if isinstance(raw, dict) else []
    validated: list[dict] = []
    for position, replacement in enumerate(replacements, start=1):
        index_name = str(replacement.get("index_name", "")).strip()
        removed_symbol = str(replacement.get("removed_symbol", "")).strip().upper()
        added_symbol = str(replacement.get("added_symbol", "")).strip().upper()
        if not index_name or not removed_symbol or not added_symbol or removed_symbol == added_symbol:
            raise ValueError(f"Invalid index replacement #{position}")
        validated.append(
            {
                **replacement,
                "index_name": index_name,
                "removed_symbol": removed_symbol,
                "added_symbol": added_symbol,
                "effective_date": pd.Timestamp(replacement.get("effective_date")).normalize(),
            }
        )
    return sorted(validated, key=lambda item: (item["effective_date"], item["index_name"]))
