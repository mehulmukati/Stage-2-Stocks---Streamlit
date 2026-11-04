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
    for column in ("is_tradable", "price_status"):
        if column in frame:
            digest.update(column.encode())
            digest.update(pd.util.hash_pandas_object(frame.loc[selected.index, column], index=True).values.tobytes())
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


def _index_key(value):
    return "".join(c for c in str(value).upper() if c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")


def _required_symbols(config, index_keys, start, end):
    membership = config.compositions_df
    if membership is None or not index_keys:
        raise ValueError("Reviewed research requires complete dated membership")
    required = set()
    keys = membership["INDEX_NAME"].map(_index_key)
    for index in index_keys:
        rows = membership.loc[keys.eq(index)]
        dates = pd.to_datetime(rows["TIME_STAMP"], errors="raise")
        initial = dates[dates <= start].max()
        if pd.isna(initial):
            raise ValueError("Reviewed membership lacks an initial snapshot")
        members = rows.loc[dates.between(initial, end), "SYMBOL"]
        if members.empty or members.isna().any() or members.eq("").any():
            raise ValueError("Reviewed membership contains missing security identities")
        required.update(members)

    members = required.copy()
    # A former member may still be held when a later transfer takes effect.
    # Close transitively over the configured ownership graph, including locked
    # claims that are absent from every membership snapshot.
    actions = [action for action in config.corporate_actions if pd.Timestamp(action["effective_date"]) <= end]
    while True:
        successors = {a["successor_symbol"] for a in actions if a["old_symbol"] in required}
        if successors <= required:
            return members, required, actions
        required.update(successors)


def validate_consumption(config, frames, membership_hash, calendar=None):
    """Validate the catalogue even if no security opted into field masking."""
    certificates = {}
    index_keys = {_index_key(i) for i in config.index_names or []}
    start, end = pd.Timestamp(config.start_date), pd.Timestamp(config.end_date)
    for frame in frames.values():
        for scope in frame.attrs.get("research_scope_catalogue", []):
            if index_keys == set(scope["index_keys"]) and end >= pd.Timestamp(scope["history_start"]):
                certificates[certificate_digest(scope)] = scope
        for scope in frame.attrs.get("research_field_scopes", []):
            if end >= pd.Timestamp(scope["history_start"]):
                certificates[certificate_digest(scope)] = scope

    for key, scope in certificates.items():
        if (
            scope.get("accepted") is not True
            or not scope.get("reviewer")
            or scope.get("economics_validated") is not True
            or scope.get("complete_membership") is not True
            or scope.get("unresolved_consumed_events") != []
            or scope.get("unresolved_consumed_seams") != []
        ):
            raise ValueError("Research scope has unresolved acceptance conditions")
        history_start, scope_start, scope_end = (
            pd.Timestamp(scope[field]) for field in ("history_start", "start", "end")
        )
        if not history_start <= scope_start <= start <= end <= scope_end:
            raise ValueError("Run exceeds the accepted research interval")
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
        sessions = sessions[(sessions >= history_start) & (sessions <= scope_end)]
        if hashlib.sha256(sessions.asi8.tobytes()).hexdigest() != scope["calendar_sha256"]:
            raise ValueError("Consumed session calendar differs from the reviewed calendar")

        members, required, actions = _required_symbols(config, index_keys, start, end)
        hashes = scope.get("price_field_hashes") or {}
        carry_claims = scope.get("unpriced_locked_claims", {})
        for symbol, evidence in carry_claims.items():
            arrivals = [pd.Timestamp(a["effective_date"]) for a in actions if a["successor_symbol"] == symbol]
            departures = [pd.Timestamp(a["effective_date"]) for a in actions if a["old_symbol"] == symbol]
            if (
                symbol not in required - members
                or not symbol.startswith("CLAIM:")
                or symbol in frames
                or symbol in hashes
                or not isinstance(evidence, str)
                or len(evidence) != 64
                or any(c not in "0123456789abcdef" for c in evidence)
                or len(arrivals) != 1
                or len(departures) != 1
                or arrivals[0] >= departures[0]
            ):
                raise ValueError(f"Invalid no-observation ownership proof for {symbol}")
        missing = required - hashes.keys() - carry_claims.keys()
        if missing:
            raise ValueError(f"Research certificate lacks required security coverage: {', '.join(sorted(missing))}")
        if not hashes.keys() <= frames.keys():
            raise ValueError("Research universe is incomplete; no price-available survivor subset")
        for symbol, expected in hashes.items():
            frame = frames[symbol]
            if tuple(scope["source_revisions"]) != tuple(frame.attrs.get("source_revisions", ())):
                raise ValueError("Research certificate belongs to a different shared revision")
            if key not in {certificate_digest(s) for s in frame.attrs.get("research_field_scopes", [])}:
                raise ValueError("Reviewed member frame lost its shared scope binding")
            if (frame.index < history_start).any():
                raise ValueError("Unreviewed earlier observations affect cumulative eligibility")
            covered = frame.loc[history_start:scope_end]
            if covered.empty or not set(scope["fields"]).issubset(covered.columns):
                raise ValueError(f"Required fields or observations missing for {symbol}")
            if field_digest(frame, scope["fields"], history_start, scope_end) != expected:
                raise ValueError("Consumed research observations changed since acceptance")
    return sorted(certificates)
