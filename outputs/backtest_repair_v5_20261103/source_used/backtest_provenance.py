"""Read-only, reproducible input/policy manifests and inspectable coverage."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path

import pandas as pd

from backtest_accounting import POLICIES
from equity_data import calendar_provenance

ROOT = Path(__file__).parent


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def frame_hash(frame):
    digest = hashlib.sha256()
    digest.update(str(list(frame.columns)).encode())
    digest.update(pd.util.hash_pandas_object(frame, index=True).values.tobytes())
    return digest.hexdigest()


def result_calculations_current(result):
    """Reject a saved result when calculation code changes with identical data.

    Support both direct engine results and the app's paired band-rule result.
    This does not certify the market data or any investment convention.
    """
    manifests = result.get("run_manifests")
    if manifests is None:
        manifests = {"single": result.get("run_manifest")}
    if not manifests:
        return False
    required = ("backtest_engine.py", "backtest_accounting.py", "momentum_engine.py", "research_acceptance.py")
    expected = {name: sha256(ROOT / name) for name in required}
    return all(
        isinstance(manifest, dict)
        and all(manifest.get("code_and_registry_sha256", {}).get(name) == digest for name, digest in expected.items())
        for manifest in manifests.values()
    )


def run_manifest(config, frames, benchmarks, calendar):
    from research_acceptance import validate_consumption

    membership_digest = frame_hash(config.compositions_df) if config.compositions_df is not None else None
    accepted_scopes = validate_consumption(config, frames, membership_digest, calendar)
    settings = {
        f.name: getattr(config, f.name)
        for f in dataclasses.fields(config)
        if f.name not in {"compositions_df", "market_calendar"}
    }
    source_files = [
        "backtest_engine.py",
        "backtest_accounting.py",
        "backtest_provenance.py",
        "research_acceptance.py",
        "security_identity.py",
        "equity_data.py",
        "momentum_engine.py",
        "momentum_ranking.py",
        "stage2_engine.py",
        "config.py",
        "data/security_aliases.json",
        "data/corporate_actions.json",
        "data/nse_session_calendar.json",
    ]
    manifest = dict(
        schema=1,
        config=settings,
        policies=dict(POLICIES),
        calendar=calendar_provenance(),
        code_and_registry_sha256={p: sha256(ROOT / p) for p in source_files},
        session_grid_sha256=hashlib.sha256(calendar.asi8.tobytes()).hexdigest(),
        membership_sha256=membership_digest,
        reviewed_field_scope_sha256=accepted_scopes,
        price_sha256={s: frame_hash(f) for s, f in sorted(frames.items())},
        benchmarks_sha256={s: frame_hash(f.to_frame()) for s, f in sorted(benchmarks.items())},
        source_revisions=sorted({str(f.attrs.get("source_revisions")) for f in frames.values()}),
        certification=(
            "reviewed field/profile-scoped adjusted research; nominal execution/tax not certified"
            if accepted_scopes
            else "uncertified research; identity/coverage and nominal tax basis unresolved"
        ),
        terminal_policy="no liquidation; realized tax accrued; scheduled events only",
        benchmark_basis="vendor-adjusted equity research versus supplied benchmark basis; no alpha claim",
    )
    payload = json.dumps(manifest, sort_keys=True, default=str, separators=(",", ":"))
    manifest["run_id"] = hashlib.sha256(payload.encode()).hexdigest()
    return manifest


def coverage_at(compositions, indices, day, frames):
    if compositions is None or not indices:
        return []

    def key(s):
        return "".join(c for c in str(s).upper() if c.isalnum())

    eligible = compositions[pd.to_datetime(compositions.TIME_STAMP) <= day]
    reports = []
    for index in indices:
        keys = eligible["_INDEX_KEY"] if "_INDEX_KEY" in eligible else eligible.INDEX_NAME.map(key)
        rows = eligible[keys.eq(key(index))]
        latest = pd.to_datetime(rows.TIME_STAMP).max() if len(rows) else pd.NaT
        snapshot = rows[pd.to_datetime(rows.TIME_STAMP).eq(latest)] if len(rows) else rows
        symbols = set(snapshot.SYMBOL)
        missing = []
        no_quote = []
        for s in sorted(symbols):
            f = frames.get(s)
            if f is None or f.loc[:day, "Close"].dropna().empty:
                missing.append(s)
            if f is None or day not in f.index or pd.isna(f.at[day, "Close"]):
                no_quote.append(s)
        reports.append(
            dict(
                date=day,
                index=index,
                snapshot_date=None if pd.isna(latest) else latest,
                recorded_members=len(symbols),
                priced_members=len(symbols) - len(missing),
                missing_price_identities=missing,
                missing_current_marks=no_quote,
                source_confidence=(
                    sorted(set(snapshot.CONFIDENCE.astype(str))) if "CONFIDENCE" in snapshot else ["unspecified"]
                ),
                coverage="missing component history" if not len(rows) else "uncertified historical coverage",
            )
        )
    return reports


def write_manifest_exclusive(path, manifest):
    # Never restate a historical run. Callers choose a new version/directory.
    with Path(path).open("x", encoding="utf-8") as out:
        json.dump(manifest, out, sort_keys=True, indent=2, default=str)
