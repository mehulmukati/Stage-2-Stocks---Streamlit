"""Offline security-ID membership reconciliation; no portfolio adjustments.

Corporate-action price ex-dates and exchange identifier observation dates are
distinct. This walk uses the latter to compare unchanged raw monthly ISIN rows.
It never multiplies adjusted research quantities or credits entitlements.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pandas as pd


def parse_review_transition_section(text):
    """Read numbered symbol rows from one already isolated Nifty 500 section.

    Ordinal continuity detects truncated tables. Announcements and amendments
    remain separate inputs; callers must explicitly mark superseded rows.
    """
    direction = None
    rows = []
    expected = {}
    for line_number, line in enumerate(text.splitlines(), 1):
        for word, kind in [("exclud", "EXCL"), ("includ", "INCL")]:
            if re.search(r"following compan(?:y|ies).*" + word, line, re.I):
                direction = kind
                expected[kind] = 1
        match = re.match(r"^\s*(\d+)\s+(.+?)\s{2,}([A-Z0-9&_.-]+)\s*$", line)
        if match and direction:
            ordinal = int(match[1])
            if ordinal != expected[direction]:
                raise ValueError("Incomplete/duplicated primary review table")
            expected[direction] += 1
            rows.append(dict(direction=direction, symbol=match[3], company=match[2], line=line_number, ordinal=ordinal))
    if not rows:
        raise ValueError("No primary review transitions")
    if len({(r["direction"], r["symbol"]) for r in rows}) != len(rows):
        raise ValueError("Duplicate primary review transition")
    return rows


def verify_evidence(root, records):
    for record in records:
        path = Path(root) / record["path"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != record["sha256"]:
            raise ValueError("Evidence digest mismatch: " + str(path))


def apply_security_conversion(state, action, reverse=False):
    """Preserve set membership; never treat a nonmember split as index entry."""
    if not action.get("membership_conversion_supported"):
        raise ValueError("Unsupported security conversion")
    old, new = action["old_security_id"], action["new_security_id"]
    if old == new or not all(re.fullmatch(r"ISIN:IN[A-Z0-9]{10}", v) for v in [old, new]):
        raise ValueError("Invalid distinct security conversion IDs")
    state = set(state)
    if old in state and new in state:
        raise ValueError("Distinct old/new securities coexist; cannot collapse share classes")
    before, after = (new, old) if reverse else (old, new)
    if before in state:
        state.remove(before)
        state.add(after)
    return state


def project_raw_identifier_observation(ids, day, actions):
    """Project an unchanged raw identifier observation across a supported split.

    A bhavcopy may still report the old ISIN on/after the ex-price date. Keep
    that source row intact; this explicit projection is for comparing economic
    membership only and never adjusts portfolio quantities or cash.
    """
    result = set(ids)
    day = str(pd.Timestamp(day).date())
    for action in actions:
        if action["ex_date"] <= day < action["identifier_effective_date"]:
            result = apply_security_conversion(result, action)
    return result


def walk_membership(
    initial,
    anchor,
    events,
    actions,
    start="2017-07-01",
    end="2020-09-01",
    action_date_field="identifier_effective_date",
):
    """Check both independent observed endpoints, with strict transition checks."""
    events = events.drop_duplicates(["EVENT_DATE", "EVENT_TYPE", "SECURITY_ID"])
    if action_date_field not in {"identifier_effective_date", "ex_date"}:
        raise ValueError("Unknown action-date policy")
    actions = [a for a in actions if start <= a[action_date_field] <= end]
    schedule = sorted(set(events.EVENT_DATE) | {a[action_date_field] for a in actions})
    schedule = [d for d in schedule if start <= d <= end]
    states = {pd.Timestamp(start): set(initial)}
    conflicts = []
    state = set(initial)
    for day in schedule:
        for action in actions:
            if action[action_date_field] == day:
                state = apply_security_conversion(state, action)
        group = events[events.EVENT_DATE.eq(day)]
        inc = set(group.loc[group.EVENT_TYPE.eq("INCL"), "SECURITY_ID"])
        exc = set(group.loc[group.EVENT_TYPE.eq("EXCL"), "SECURITY_ID"])
        if inc & state or exc - state or inc & exc:
            conflicts.append(
                dict(
                    date=day,
                    already_included=sorted(inc & state),
                    missing_exclusions=sorted(exc - state),
                    collision=sorted(inc & exc),
                )
            )
        state = (state - exc) | inc
        states[pd.Timestamp(day)] = set(state)
    final = set(state)
    state = set(anchor)  # independently observed anchor, not the forward result
    reverse_conflicts = []
    for day in reversed(schedule):
        group = events[events.EVENT_DATE.eq(day)]
        inc = set(group.loc[group.EVENT_TYPE.eq("INCL"), "SECURITY_ID"])
        exc = set(group.loc[group.EVENT_TYPE.eq("EXCL"), "SECURITY_ID"])
        if inc - state or exc & state or inc & exc:
            reverse_conflicts.append(
                dict(
                    date=day,
                    missing_inclusions=sorted(inc - state),
                    excluded_still_present=sorted(exc & state),
                    collision=sorted(inc & exc),
                )
            )
        state = (state - inc) | exc
        for action in reversed(actions):
            if action[action_date_field] == day:
                state = apply_security_conversion(state, action, reverse=True)
    return states, dict(
        forward_conflicts=conflicts,
        reverse_conflicts=reverse_conflicts,
        forward_anchor_matches=final == set(anchor),
        reverse_initial_matches=state == set(initial),
        forward_anchor_missing=sorted(set(anchor) - final),
        forward_anchor_extra=sorted(final - set(anchor)),
        reverse_initial_missing=sorted(set(initial) - state),
        reverse_initial_extra=sorted(state - set(initial)),
    )
