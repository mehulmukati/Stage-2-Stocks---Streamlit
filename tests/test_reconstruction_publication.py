import json

import pandas as pd
import pytest

import market_data as md
from backtest_provenance import sha256
from scripts.publish_reconstructed_membership import publish, restore_backup
from security_identity import REGISTRY


def staged(market, tmp_path):
    _, _, days = market
    history = pd.DataFrame(
        dict(
            INDEX_NAME=["X", "X", "X", "Y"],
            TIME_STAMP=[days[0], days[1], days[3], days[1]],
            SYMBOL=["A", "WRONG", "B", "OTHER"],
        )
    )
    md.write_source(history, md.MEMBERSHIP_PATH)
    candidate = pd.DataFrame(
        dict(
            INDEX_NAME=["X", "X"],
            TIME_STAMP=[days[1], days[2]],
            SYMBOL=["A", "B"],
            SECURITY_ID=["TEST:A", "TEST:B"],
            CONFIDENCE=["verified"] * 2,
        )
    )
    observed = candidate.copy()
    path = tmp_path / "candidate.parquet"
    truth = tmp_path / "observed.parquet"
    certificate = tmp_path / "certificate.json"
    candidate.to_parquet(path, index=False)
    observed.to_parquet(truth, index=False)
    prices, membership = md.source_revisions()
    cert = dict(
        accepted=True,
        reviewer="fixture review",
        candidate_sha256=sha256(path),
        identity_registry_sha256=sha256(REGISTRY),
        price_revision=prices,
        membership_revision=membership,
        validated_intervals=[dict(index="X", start=str(days[1].date()), end=str(days[2].date()))],
        observed_snapshots_path=str(truth),
        observed_snapshots_sha256=sha256(truth),
    )
    certificate.write_text(json.dumps(cert))
    return path, certificate, cert, candidate


def test_failed_repair_preserves_accepted_bytes(market, tmp_path):
    path, certificate, cert, candidate = staged(market, tmp_path)
    original = sha256(md.MEMBERSHIP_PATH)
    candidate.loc[0, "SYMBOL"] = "UNRESOLVED:old-name"
    candidate.to_parquet(path, index=False)
    cert["candidate_sha256"] = sha256(path)
    certificate.write_text(json.dumps(cert))
    with pytest.raises(ValueError, match="Unresolved"):
        publish(path, certificate, tmp_path / "backup")
    assert sha256(md.MEMBERSHIP_PATH) == original
    assert not (tmp_path / "backup").exists()


def test_repair_conflict_check_and_interval_replacement_with_backup(market, tmp_path):
    path, certificate, cert, candidate = staged(market, tmp_path)
    original = sha256(md.MEMBERSHIP_PATH)
    cert["price_revision"] = "stale"
    certificate.write_text(json.dumps(cert))
    with pytest.raises(RuntimeError, match="changed since review"):
        publish(path, certificate, tmp_path / "failed_backup")
    assert sha256(md.MEMBERSHIP_PATH) == original
    cert["price_revision"] = md.source_revisions()[0]
    certificate.write_text(json.dumps(cert))
    publish(path, certificate, tmp_path / "backup")
    published = md.load_snapshot().membership
    # WRONG was repaired only within the approved dates; the old effective
    # state must resume immediately afterwards until the next old snapshot.
    assert md.constituents_at(published, "2025-01-02", ["X"]) == {"X": ["A"]}
    assert md.constituents_at(published, "2025-01-03", ["X"]) == {"X": ["B"]}
    assert md.constituents_at(published, "2025-01-04", ["X"]) == {"X": ["WRONG"]}
    assert {"A", "B", "OTHER", "WRONG"} == set(published.SYMBOL)
    assert sha256(tmp_path / "backup" / md.MEMBERSHIP_PATH.name) == original
    current = md.source_revisions()[1]
    with pytest.raises(RuntimeError, match="changed"):
        restore_backup(tmp_path / "backup", expected_current_revision="wrong")
    restore_backup(tmp_path / "backup", expected_current_revision=current)
    assert "WRONG" in set(md.load_snapshot().membership.SYMBOL)


def test_observed_snapshot_cannot_be_changed(market, tmp_path):
    path, certificate, cert, candidate = staged(market, tmp_path)
    original = sha256(md.MEMBERSHIP_PATH)
    candidate.loc[0, "SYMBOL"] = "FAKE"
    candidate.to_parquet(path, index=False)
    cert["candidate_sha256"] = sha256(path)
    certificate.write_text(json.dumps(cert))
    with pytest.raises(ValueError, match="authoritative"):
        publish(path, certificate, tmp_path / "backup")
    assert sha256(md.MEMBERSHIP_PATH) == original
