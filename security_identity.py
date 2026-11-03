"""Evidence-only dated aliases. Fuzzy proposals never establish identity.

An ISIN identifies a security, not an issuer across capital reorganizations.
Separate issuer_id and lineage are retained; vendor aliases require their own
dated evidence. Unknown periods stay unknown, including pre-listing periods.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

IDENTITY_VERSION = "dated-evidence-only-v1"
REGISTRY = Path(__file__).parent / "data" / "security_aliases.json"


def name_key(value):
    # Conservative punctuation/space normalization, no stripping INDIA, etc.
    return " ".join(str(value).casefold().replace(".", "").split())


def load_aliases(path=REGISTRY):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    aliases = raw["aliases"]
    required = {
        "security_id",
        "issuer_id",
        "name",
        "exchange_symbol",
        "valid_from",
        "valid_to",
        "source",
        "observed_at",
        "verified_at",
        "confidence",
        "decision",
    }
    for a in aliases:
        if required - set(a) or a["decision"] != "reviewed" or a["confidence"] != "verified":
            raise ValueError("Alias requires reviewed dated primary evidence")
        if not a["security_id"] or not a["source"].startswith("https://"):
            raise ValueError("Missing stable identity/source")
        if pd.Timestamp(a["valid_from"]) > pd.Timestamp(a["valid_to"]):
            raise ValueError("Reversed alias validity")
        for date in a.get("verified_effective_dates", []):
            if not pd.Timestamp(a["valid_from"]) <= pd.Timestamp(date) <= pd.Timestamp(a["valid_to"]):
                raise ValueError("Verified effective date falls outside alias span")
    for i, a in enumerate(aliases):
        for b in aliases[i + 1 :]:
            if name_key(a["name"]) == name_key(b["name"]) and max(a["valid_from"], b["valid_from"]) <= min(
                a["valid_to"], b["valid_to"]
            ):
                points_a = a.get("verified_effective_dates")
                points_b = b.get("verified_effective_dates")
                if points_a is not None or points_b is not None:
                    overlap = (
                        any(
                            b["valid_from"] <= d <= b["valid_to"] and (points_b is None or d in points_b)
                            for d in points_a
                        )
                        if points_a is not None
                        else any(a["valid_from"] <= d <= a["valid_to"] for d in points_b)
                    )
                    if not overlap:
                        continue
                if (a["security_id"], a["exchange_symbol"]) != (b["security_id"], b["exchange_symbol"]):
                    raise ValueError("Conflicting overlapping alias decisions")
    return aliases


def resolve_name(name, effective_date, aliases):
    day = pd.Timestamp(effective_date)
    matches = [
        a
        for a in aliases
        if name_key(a["name"]) == name_key(name)
        and pd.Timestamp(a["valid_from"]) <= day <= pd.Timestamp(a["valid_to"])
        and ("verified_effective_dates" not in a or str(day.date()) in a["verified_effective_dates"])
    ]
    return matches[0].copy() if matches else None


def resolve_source_events(events, proposals=None, aliases=None, legacy_inception_rule=False):
    aliases = load_aliases() if aliases is None else aliases
    result = events.copy()
    if "SOURCE_ROW_ID" not in result:
        result["SOURCE_ROW_ID"] = [f"input-row:{i}" for i in range(len(result))]
    result["EFFECTIVE_DATE"] = pd.to_datetime(result.EVENT_DATE, errors="raise").dt.strftime("%Y-%m-%d")
    if not result.EVENT_TYPE.isin(["INCL", "EXCL"]).all():
        raise ValueError("Unknown event direction")
    records = []
    for r in result.to_dict("records"):
        a = resolve_name(r["SCRIP_NAME"], r["EVENT_DATE"], aliases)
        records.append(
            {
                **r,
                "SYMBOL": a["exchange_symbol"] if a else None,
                "SECURITY_ID": a["security_id"] if a else None,
                "IDENTITY_STATUS": "verified" if a else "unresolved",
                "CONFIDENCE": "verified" if a else "none",
                "IDENTITY_EVIDENCE": json.dumps(a, sort_keys=True) if a else "",
                "OBSERVED_AT": a["observed_at"] if a else None,
                "VERIFIED_AT": a["verified_at"] if a else None,
                "RETRIEVED_AT": None,
                "SOURCE": "F2",
                "MATCH_SCORE": 100.0 if a else 0.0,
            }
        )
    result = pd.DataFrame(records)
    if proposals is not None and not proposals.empty:
        suggestions = proposals.set_index("SCRIP_NAME")["SYMBOL"].to_dict()
        result["PROPOSED_SYMBOL"] = result.SCRIP_NAME.map(suggestions)
    # Preserve the inherited user rule only as an explicit legacy decision,
    # never as proof that the earliest logged event was an index inception.
    first = result.groupby("INDEX_NAME").EVENT_DATE.transform("min")
    earliest_excl = result.EVENT_TYPE.eq("EXCL") & result.EVENT_DATE.eq(first)
    result["INCEPTION_REVIEW_REQUIRED"] = earliest_excl
    result["LEGACY_EXCLUDED"] = earliest_excl & legacy_inception_rule
    result["RECONSTRUCTION_ALLOWED"] = result.IDENTITY_STATUS.eq("verified") & ~result.LEGACY_EXCLUDED
    return result


def unresolved_id(row):
    return "UNRESOLVED:" + hashlib.sha256(name_key(row["SCRIP_NAME"]).encode()).hexdigest()[:20]
