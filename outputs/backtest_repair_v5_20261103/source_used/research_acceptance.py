"""Field/profile-scoped research acceptance over the existing shared revisions.

This is a consumption boundary, not a source of prices or approval. A certificate
must bind independently reviewed economics, full membership and exact consumed
fields. Legacy sources without such metadata remain explicitly uncertified.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json

import pandas as pd


def consumed_fields(config):
    """Dependencies of the present close-execution momentum backtest path.

    Circuit proxy, Sharpe, moving averages, positive days and rolling Stage 2
    use Close; volume eligibility and execution require Volume. Only the active
    distance-to-52-week-high filter consumes the source High in this path.
    """
    fields = {"Close", "Volume"}
    if config.pct_from_52w_high < 100:
        fields.add("High")
    return fields


def profile_digest(config):
    settings = {
        f.name: getattr(config, f.name)
        for f in dataclasses.fields(config)
        if f.name not in {"compositions_df", "market_calendar"}
    }
    return hashlib.sha256(json.dumps(settings, sort_keys=True, default=str).encode()).hexdigest()


def field_digest(frame, fields, start, end):
    selected = frame.loc[pd.Timestamp(start) : pd.Timestamp(end), sorted(fields)]
    digest = hashlib.sha256(str(list(selected.columns)).encode())
    digest.update(pd.util.hash_pandas_object(selected, index=True).values.tobytes())
    if "is_tradable" in frame:
        flags = frame.loc[selected.index, "is_tradable"]
        digest.update(pd.util.hash_pandas_object(flags, index=True).values.tobytes())
    return digest.hexdigest()


def certificate_digest(certificate):
    return hashlib.sha256(json.dumps(certificate, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def apply_field_scopes(frame, symbol, scopes):
    """Keep uncertified candle fields missing in every shared OHLCV reader."""
    result = frame.copy()
    applicable = []
    for scope in scopes:
        if symbol not in scope.get("price_field_hashes", {}):
            continue
        mask = result.index.to_series().between(scope["history_start"], scope["end"])
        if not mask.any():
            continue
        for column in {"Open", "High", "Low"} - set(scope["fields"]):
            if column in result:
                result.loc[mask, column] = float("nan")
        applicable.append(scope)
    if applicable:
        result.attrs["research_field_scopes"] = applicable
    return result


def validate_consumption(config, frames, membership_hash, calendar=None):
    """Fail before ranking/fills when scoped data cannot support this profile."""
    certificates = {}
    for symbol, frame in frames.items():
        for scope in frame.attrs.get("research_scope_catalogue", []):
            index_keys = {"".join(c for c in str(i).upper() if c.isalnum()) for i in config.index_names or []}
            if index_keys == set(scope["index_keys"]) and not (
                pd.Timestamp(config.end_date) < pd.Timestamp(scope["history_start"])
                or pd.Timestamp(config.start_date) > pd.Timestamp(scope["end"])
            ):
                certificates[certificate_digest(scope)] = scope
        for scope in frame.attrs.get("research_field_scopes", []):
            if pd.Timestamp(config.end_date) < pd.Timestamp(scope["history_start"]) or pd.Timestamp(
                config.start_date
            ) > pd.Timestamp(scope["end"]):
                continue
            key = certificate_digest(scope)
            certificates[key] = scope
            if (
                scope.get("accepted") is not True
                or not scope.get("reviewer")
                or scope.get("economics_validated") is not True
                or scope.get("complete_membership") is not True
                or scope.get("unresolved_consumed_events") != []
                or scope.get("unresolved_consumed_seams") != []
            ):
                raise ValueError("Research scope has unresolved acceptance conditions")
            if tuple(scope["source_revisions"]) != tuple(frame.attrs.get("source_revisions", ())):
                raise ValueError("Research certificate belongs to a different shared revision")
            if not (
                pd.Timestamp(scope["start"])
                <= pd.Timestamp(config.start_date)
                <= pd.Timestamp(config.end_date)
                <= pd.Timestamp(scope["end"])
            ):
                raise ValueError("Run exceeds the accepted research interval")
            index_keys = {"".join(c for c in str(i).upper() if c.isalnum()) for i in config.index_names or []}
            if index_keys != set(scope["index_keys"]):
                raise ValueError("Parent-index acceptance does not certify component universes")
            if not consumed_fields(config).issubset(scope["fields"]):
                raise ValueError("Profile requires uncertified price fields")
            if profile_digest(config) not in scope["profile_sha256"]:
                raise ValueError("Strategy profile is outside the reviewed research certificate")
            if membership_hash != scope["membership_sha256"]:
                raise ValueError("Run membership differs from the complete reviewed universe")
            if calendar is None:
                raise ValueError("Reviewed research requires the corrected session calendar")
            sessions = pd.DatetimeIndex(calendar)
            sessions = sessions[(sessions >= scope["history_start"]) & (sessions <= scope["end"])]
            if hashlib.sha256(sessions.asi8.tobytes()).hexdigest() != scope["calendar_sha256"]:
                raise ValueError("Consumed session calendar differs from the reviewed calendar")
            if (frame.index < pd.Timestamp(scope["history_start"])).any():
                raise ValueError("Unreviewed earlier observations affect cumulative eligibility")
            digest = field_digest(frame, scope["fields"], scope["history_start"], scope["end"])
            if digest != scope["price_field_hashes"][symbol]:
                raise ValueError("Consumed research observations changed since acceptance")
    # Every reviewed security remains required, including short IPO histories.
    for key, scope in certificates.items():
        if not set(scope["price_field_hashes"]).issubset(frames):
            raise ValueError("Research universe is incomplete; no price-available survivor subset")
        for symbol in scope["price_field_hashes"]:
            if key not in {certificate_digest(s) for s in frames[symbol].attrs.get("research_field_scopes", [])}:
                raise ValueError("Reviewed member frame lost its shared scope binding")
    return sorted(certificates)
