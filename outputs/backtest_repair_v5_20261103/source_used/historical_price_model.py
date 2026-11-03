"""Offline, explicit cash-dividend/split research-price normalization.

This is not nominal-share tax accounting. Unsupported entitlements stay named
blockers; neither price gaps nor corporate-action values are inferred from the
same return being validated. No split quantities or distributions are credited
again to a portfolio already consuming these adjusted research units.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

MODEL_VERSION = "nse-cash-split-research-v1"


def select_primary_equity_actions(frame):
    """Keep dated equity series; a restated API ISIN can also label debt rows.

    Return excluded rows for inspection instead of attributing bond interest
    or bond demerger records to the ordinary equity holder.
    """
    if "series" not in frame:
        raise ValueError("Primary action series is required")
    valid = frame.series.astype(str).str.strip().isin({"EQ", "BE", "BZ"})
    excluded = frame.loc[~valid].copy()
    excluded["exclusion_reason"] = "non-equity series; ISIN alone does not authorize an equity entitlement"
    return frame.loc[valid].copy(), excluded


def read_primary_action_bulletin(path):
    """Read original BC rows, retaining duplicates and exact archive references."""
    path = Path(path)
    with zipfile.ZipFile(path) as archive:
        members = [m for m in archive.namelist() if re.fullmatch(r"bc\d{6,8}\.csv", Path(m).name, re.I)]
        if len(members) != 1:
            raise ValueError("Expected one primary corporate-action bulletin")
        member = members[0]
        frame = pd.read_csv(io.BytesIO(archive.read(member)), dtype=str)
    required = {"SYMBOL", "SERIES", "EX_DT", "PURPOSE"}
    if required - set(frame):
        raise ValueError("Incomplete primary corporate-action bulletin")
    for column in frame:
        frame[column] = frame[column].str.strip()
    frame["date"] = pd.to_datetime(frame.EX_DT, format="%d/%m/%Y", errors="coerce")
    frame["source_row_id"] = [f"{path.name}:{member}:{i + 2}" for i in range(len(frame))]
    return frame


def corroborate_action_terms(proposed, bulletin_rows):
    """Resolve a restated identifier with dated-symbol and matching cash terms.

    Neither issuer prefixes nor a resemblance between modern ticker names can
    authorize this join. Conflicting amounts remain unresolved.
    """
    if proposed.get("kind") not in {"cash_dividend", "split", "bonus"}:
        return []
    matched = []
    for row in bulletin_rows:
        if row["SYMBOL"] != proposed["historical_symbol"] or pd.Timestamp(row["date"]) != pd.Timestamp(
            proposed["date"]
        ):
            continue
        decision = parse_primary_action(row["PURPOSE"])
        if (
            decision.get("supported")
            and decision["kind"] == proposed["kind"]
            and abs(decision.get("cash", 0) - proposed.get("cash", 0)) < 1e-10
            and abs(decision.get("share_ratio", 1) - proposed.get("share_ratio", 1)) < 1e-10
        ):
            matched.append(row["source_row_id"])
    return matched


def parse_primary_action(subject):
    text = " ".join(str(subject).strip().split())
    low = text.lower()
    low = re.sub(r"\bfv splt (?:frm|from)\b", "face value split from", low)
    low = low.replace("bonus issue", "bonus")
    # Explicit source spelling variants, not identity/name fuzzy matching.
    for original in ["divided", "divdend", "ividend"]:
        low = re.sub(r"\b" + original + r"\b", "dividend", low)
    complex_words = [
        "rights",
        "scheme",
        "sheme",
        "demerger",
        "de-merger",
        "merger",
        "reduction",
        "consolidation",
        "debenture",
        "preference",
        "reconstruction",
        "interest",
        "distribution",
        "ccds",
    ]
    if any(word in low for word in complex_words):
        return dict(kind="unsupported_entitlement", supported=False, reason=text)
    split = "split" in low or "sub-division" in low or "sub division" in low
    bonus = "bonus" in low
    cash = "dividend" in low or re.search(r"\bdiv\b", low)
    if split and bonus and not cash:
        face = re.search(r"(?:from\s+)?(?:rs\.?|re\.?)\s*([\d.]+).*?to\s+(?:rs\.?|re\.?)\s*([\d.]+)", low)
        entitlement = re.search(r"bonus\s*[-:]?\s*(\d+)\s*[:/]\s*(\d+)", low)
        if face and entitlement and min(float(face[1]), float(face[2]), int(entitlement[2])) > 0:
            return dict(
                kind="split_and_bonus",
                supported=True,
                share_ratio=float(face[1]) / float(face[2]) * (1 + int(entitlement[1]) / int(entitlement[2])),
                cash=0.0,
            )
    if sum(bool(x) for x in [split, bonus, cash]) > 1:
        return dict(kind="combined_action_requires_review", supported=False, reason=text)
    if split:
        match = re.search(r"(?:from\s+)?(?:rs\.?|re\.?)\s*([\d.]+).*?to\s+(?:rs\.?|re\.?)\s*([\d.]+)", low)
        if not match or min(float(match[1]), float(match[2])) <= 0:
            return dict(kind="split_terms_unresolved", supported=False, reason=text)
        return dict(kind="split", supported=True, share_ratio=float(match[1]) / float(match[2]), cash=0.0)
    if bonus:
        match = re.search(r"bonus\s*[-:]?\s*(\d+)\s*[:/]\s*(\d+)", low)
        if not match or int(match[2]) == 0:
            return dict(kind="bonus_terms_unresolved", supported=False, reason=text)
        return dict(kind="bonus", supported=True, share_ratio=1 + int(match[1]) / int(match[2]), cash=0.0)
    if cash:
        amounts = re.findall(r"\b(?:rs\.?|re\.?|inr)\s*[-:]*\s*(\d+(?:\.\d+)?)", low)
        if not amounts:
            amounts = re.findall(r"\bdiv(?:idend)?\s*[-:]*\s*(\d+(?:\.\d+)?)\s*(?:/-)?\s*(?:per\s+share|/share)", low)
        amount_values = [float(a[0] if isinstance(a, tuple) else a) for a in amounts]
        distinct_components = len(amount_values) == 1 or (
            low.count("dividend") >= len(amount_values)
            and not any(w in low for w in ["includ", "compris", "aggregate", "total"])
        )
        if not amount_values or not distinct_components:
            return dict(kind="cash_terms_unresolved", supported=False, reason=text)
        return dict(kind="cash_dividend", supported=True, share_ratio=1.0, cash=sum(amount_values))
    if any(
        word in low
        for word in [
            "general meeting",
            "general meetin",
            "agm",
            "egm",
            "book closure",
            "buyback",
            "buy back",
            "buy-back",
            "annual book",
            "e-voting",
        ]
    ):
        return dict(kind="no_mandatory_holder_distribution", supported=True, share_ratio=1.0, cash=0.0)
    return dict(kind="unclassified_primary_action", supported=False, reason=text)


def primary_event_factor(previous_raw_close, action):
    """Back-adjust pre-ex-date prices; never infer an unknown entitlement.

    Dividend convention matches standard backward dividend normalization:
    (previous close - cash dividend) / previous close. This is a stated research
    convention, not a claim of executable reinvestment or nominal statutory tax.
    """
    if not action.get("supported"):
        raise ValueError("Unsupported entitlement")
    previous = float(previous_raw_close)
    ratio, cash = float(action.get("share_ratio", 1)), float(action.get("cash", 0))
    if not np.isfinite([previous, ratio, cash]).all() or previous <= 0 or ratio <= 0 or cash < 0 or cash >= previous:
        raise ValueError("Invalid independently evidenced action terms")
    if action.get("kind") == "reviewed_fully_paid_rights":
        if not action.get("reviewed_source"):
            raise ValueError("Rights adjustment requires reviewed offer terms")
        entitlement = float(action["rights_ratio"])
        subscription = float(action["subscription_price"])
        if entitlement <= 0 or subscription <= 0 or not np.isfinite([entitlement, subscription]).all():
            raise ValueError("Invalid rights offer terms")
        return (previous + entitlement * subscription) / (previous * (1 + entitlement))
    return (previous - cash) / (previous * ratio)


def back_adjust_primary_quotes(frame, actions):
    """Return a separate research series plus explicit unresolved event rows.

    `actions` must already be attributed to one reviewed security lineage.
    Missing source dates are not inserted or made tradable. Unsupported actions
    are not normalized; the candidate remains unaccepted across those seams.
    """
    result = frame.sort_index().copy()
    if result.index.has_duplicates:
        raise ValueError("Ambiguous daily security quotes")
    factors = pd.Series(1.0, index=result.index)
    decisions = []
    seen = set()
    day_terms = {}
    for action in actions:
        if action.get("supported") and (
            action.get("share_ratio", 1) != 1
            or action.get("cash", 0) != 0
            or action.get("kind") == "reviewed_fully_paid_rights"
        ):
            day_terms.setdefault(pd.Timestamp(action["date"]), set()).add(
                (
                    action.get("kind"),
                    action.get("share_ratio"),
                    action.get("cash"),
                    action.get("rights_ratio"),
                    action.get("subscription_price"),
                )
            )
    for action in sorted(actions, key=lambda a: a["date"]):
        day = pd.Timestamp(action["date"])
        fingerprint = (
            day,
            action.get("kind"),
            action.get("share_ratio"),
            action.get("cash"),
            action.get("rights_ratio"),
            action.get("subscription_price"),
        )
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        before = result.index[result.index < day]
        if len(day_terms.get(day, set())) > 1:
            decisions.append(
                {**action, "applied": False, "reason": "multiple financial terms on one date require review"}
            )
            continue
        if not action.get("supported"):
            decisions.append({**action, "applied": False, "reason": action.get("reason", "unsupported entitlement")})
            continue
        if (
            action.get("share_ratio", 1) == 1
            and action.get("cash", 0) == 0
            and action.get("kind") != "reviewed_fully_paid_rights"
        ):
            decisions.append({**action, "applied": False, "reason": "no mandatory holder adjustment"})
            continue
        if not len(before):
            decisions.append({**action, "applied": False, "reason": "missing independent pre-ex-date quote"})
            continue
        previous = result.at[before[-1], "CLOSE"]
        try:
            factor = primary_event_factor(previous, action)
        except ValueError as exc:
            decisions.append({**action, "applied": False, "reason": str(exc)})
            continue
        factors.loc[factors.index < day] *= factor
        decisions.append(
            {
                **action,
                "applied": True,
                "factor": factor,
                "previous_raw_close": float(previous),
                "previous_quote_date": str(before[-1].date()),
                "ex_quote_available": day in result.index,
            }
        )
    for source, target in [("OPEN", "Open"), ("HIGH", "High"), ("LOW", "Low"), ("CLOSE", "Close")]:
        result[target] = result[source] * factors
    # Preserve actual reported nominal turnover capacity; do not silently adopt
    # a vendor's retrospectively split-scaled volume convention.
    result["Volume"] = result["TOTTRDQTY"]
    result["adjustment_factor"] = factors
    result["price_model"] = MODEL_VERSION
    result["accepted_for_backtest"] = False
    return result, decisions


def close_factor_seam_audit(raw, independent_adjusted, expected_factor, tolerance=0.001):
    """Compare factor CHANGES to independent action terms, not OHLC shape.

    A constant vendor normalization for later actions cancels. Nonconsecutive
    overlapping dates are explicitly skipped rather than hiding a missing date.
    """
    aligned = pd.concat(
        [raw.rename("raw"), independent_adjusted.rename("vendor"), expected_factor.rename("expected")], axis=1
    ).sort_index()
    if aligned.index.has_duplicates:
        raise ValueError("Ambiguous daily factor observations")
    valid = aligned.notna().all(axis=1) & aligned.gt(0).all(axis=1)
    factor = (aligned.vendor / aligned.raw).where(valid)
    residual = (factor / factor.shift()) / (aligned.expected / aligned.expected.shift()) - 1
    return pd.DataFrame(
        {
            "factor_change_residual": residual,
            "checked": valid & valid.shift(fill_value=False),
            "mismatch": residual.abs().gt(tolerance) & valid & valid.shift(fill_value=False),
        }
    )
