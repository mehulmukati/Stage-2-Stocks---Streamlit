"""Explicit offline promotion of reviewed, interval-scoped membership repairs.

Not called by startup/readers. A review certificate binds source revisions,
candidate bytes, identity registry and observed snapshots. Current candidates
fail these gates; constructing a certificate requires a completed review.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

import market_data as md
from backtest_provenance import sha256
from security_identity import REGISTRY


def publish(candidate_path, certificate_path, backup_dir):
    candidate_path, certificate_path, backup_dir = map(Path, [candidate_path, certificate_path, backup_dir])
    certificate = json.loads(certificate_path.read_text(encoding="utf-8"))
    required = {
        "accepted",
        "reviewer",
        "candidate_sha256",
        "identity_registry_sha256",
        "price_revision",
        "membership_revision",
        "validated_intervals",
        "observed_snapshots_path",
        "observed_snapshots_sha256",
    }
    if required - set(certificate) or certificate["accepted"] is not True or not certificate["reviewer"]:
        raise ValueError("Complete reviewed identity/coverage certificate required")
    if (
        sha256(candidate_path) != certificate["candidate_sha256"]
        or sha256(REGISTRY) != certificate["identity_registry_sha256"]
    ):
        raise ValueError("Candidate/identity registry changed since review")
    observed_path = Path(certificate["observed_snapshots_path"])
    if sha256(observed_path) != certificate["observed_snapshots_sha256"]:
        raise ValueError("Observed snapshot evidence changed since review")
    candidate = md.normalize_membership(pd.read_parquet(candidate_path))
    observed = md.normalize_membership(pd.read_parquet(observed_path))
    if (
        "CONFIDENCE" not in candidate
        or not candidate.CONFIDENCE.isin(["ground_truth", "reviewed_chain", "verified"]).all()
    ):
        raise ValueError("Unverified identity/event chains prevent publication")
    if candidate.SYMBOL.str.startswith("UNRESOLVED:").any():
        raise ValueError("Unresolved identities prevent publication")
    if "SECURITY_ID" not in candidate or candidate.SECURITY_ID.isna().any() or candidate.SECURITY_ID.eq("").any():
        raise ValueError("Stable verified security identifiers required for every candidate member")
    intervals = certificate["validated_intervals"]
    if not intervals:
        raise ValueError("Validated coverage intervals required")
    scopes = []
    covered = pd.Series(False, index=candidate.index)
    for interval in intervals:
        idx = md.index_key(interval["index"])
        start, end = pd.Timestamp(interval["start"]), pd.Timestamp(interval["end"])
        if start != start.normalize() or end != end.normalize() or start.tzinfo or end.tzinfo:
            raise ValueError("Repair scopes must be timezone-free inclusive calendar dates")
        if start > end:
            raise ValueError("Reversed coverage interval")
        for prior_idx, lo, hi in scopes:
            if idx == prior_idx and max(lo, start) <= min(hi, end):
                raise ValueError("Overlapping repair scopes")
        mask = candidate.INDEX_NAME.eq(idx) & candidate.TIME_STAMP.between(start, end)
        rows = candidate.loc[mask]
        if rows.empty or start not in set(rows.TIME_STAMP) or end not in set(rows.TIME_STAMP):
            raise ValueError("Repair interval must contain explicit boundary snapshots")
        covered |= mask
        scopes.append((idx, start, end))
    if not covered.all():
        raise ValueError("Candidate contains rows outside reviewed coverage")
    # Every supplied observed state within a repair interval is authoritative.
    for idx, start, end in scopes:
        truth = observed[observed.INDEX_NAME.eq(idx) & observed.TIME_STAMP.between(start, end)]
        for day, rows in truth.groupby("TIME_STAMP"):
            actual = candidate[candidate.INDEX_NAME.eq(idx) & candidate.TIME_STAMP.eq(day)]
            if set(actual.SYMBOL) != set(rows.SYMBOL):
                raise ValueError("Candidate changes an authoritative observed snapshot")
    with md.publisher_lock():
        price_rev, membership_rev = md.source_revisions()
        if (price_rev, membership_rev) != (certificate["price_revision"], certificate["membership_revision"]):
            raise RuntimeError("Accepted sources changed since review; rebuild and revalidate")
        if (
            sha256(candidate_path) != certificate["candidate_sha256"]
            or sha256(observed_path) != certificate["observed_snapshots_sha256"]
            or sha256(REGISTRY) != certificate["identity_registry_sha256"]
        ):
            raise RuntimeError("Review evidence changed before publication")
        history, metadata = md._read(md.MEMBERSHIP_PATH)
        keep = pd.Series(True, index=history.index)
        for idx, start, end in scopes:
            keep &= ~(history.INDEX_NAME.eq(idx) & history.TIME_STAMP.between(start, end))
        # Scopes are inclusive calendar days. Restore the ORIGINAL effective
        # state on end+1 calendar day (including weekends), not the next market
        # session. Another scope starting there takes precedence. Existing
        # snapshots at that boundary already supply the authoritative state.
        restorations = []
        for idx, start, end in scopes:
            boundary = end + pd.Timedelta(days=1)
            if any(i == idx and lo <= boundary <= hi for i, lo, hi in scopes):
                continue
            original = history[history.INDEX_NAME.eq(idx) & history.TIME_STAMP.le(boundary)]
            if original.empty:
                raise ValueError("Cannot restore unknown outside-scope state; include a prior anchor")
            state = original[original.TIME_STAMP.eq(original.TIME_STAMP.max())].copy()
            if (history.INDEX_NAME.eq(idx) & history.TIME_STAMP.eq(boundary)).any():
                continue
            state["TIME_STAMP"] = boundary
            restorations.append(state)
        combined = md.normalize_membership(pd.concat([history.loc[keep], candidate, *restorations], ignore_index=True))
        backup_dir.mkdir(parents=True, exist_ok=False)
        shutil.copy2(md.MEMBERSHIP_PATH, backup_dir / md.MEMBERSHIP_PATH.name)
        shutil.copy2(certificate_path, backup_dir / "certificate.json")
        shutil.copy2(candidate_path, backup_dir / "candidate.parquet")
        shutil.copy2(observed_path, backup_dir / "observed_snapshots.parquet")
        shutil.copy2(REGISTRY, backup_dir / "identity_registry.json")
        (backup_dir / "recovery.json").write_text(
            json.dumps(
                dict(
                    prices=price_rev,
                    membership=membership_rev,
                    previous_sha256=sha256(md.MEMBERSHIP_PATH),
                    candidate_sha256=sha256(candidate_path),
                    scope_semantics="inclusive timezone-free calendar dates; original state restored on end+1 day",
                ),
                indent=2,
            )
        )
        return md.write_source(
            combined,
            md.MEMBERSHIP_PATH,
            {
                **metadata,
                "identity_policy": "dated-evidence-only-v1",
                "repair_certificate_sha256": sha256(certificate_path),
            },
            expected_revision=membership_rev,
        )


def restore_backup(backup_dir, *, expected_current_revision):
    """Restore a checked backup as a new revision; never silently rewind sources."""
    backup_dir = Path(backup_dir)
    recovery = json.loads((backup_dir / "recovery.json").read_text(encoding="utf-8"))
    path = backup_dir / md.MEMBERSHIP_PATH.name
    if sha256(path) != recovery["previous_sha256"]:
        raise ValueError("Backup bytes differ from recovery manifest")
    with md.publisher_lock():
        price_rev, current_rev = md.source_revisions()
        if price_rev != recovery["prices"] or current_rev != expected_current_revision:
            raise RuntimeError("Accepted sources changed; review compatibility before recovery")
        frame, metadata = md._read(path)
        return md.write_source(
            frame,
            md.MEMBERSHIP_PATH,
            {**metadata, "restored_from_revision": recovery["membership"]},
            expected_revision=current_rev,
        )
