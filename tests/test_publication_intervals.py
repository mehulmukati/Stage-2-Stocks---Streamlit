import json

import pandas as pd
import pytest

import market_data as md
from backtest_provenance import sha256
from scripts.publish_reconstructed_membership import publish
from security_identity import REGISTRY


@pytest.mark.parametrize("following", ["2025-01-11", "2025-01-20", None])
@pytest.mark.parametrize("multiple", ["adjacent", "spaced", False])
def test_actual_publisher_preserves_every_outside_calendar_date(market, tmp_path, following, multiple):
    rows = [("2025-01-01", "OLD")] + ([(following, "AFTER")] if following else [])
    history = pd.DataFrame([dict(INDEX_NAME="X", TIME_STAMP=pd.Timestamp(d), SYMBOL=s) for d, s in rows])
    md.write_source(history, md.MEMBERSHIP_PATH)
    scopes = [dict(index="X", start="2025-01-05", end="2025-01-10")]
    if multiple:
        scopes.append(dict(index="X", start="2025-01-11" if multiple == "adjacent" else "2025-01-12", end="2025-01-15"))
    candidate = pd.DataFrame(
        [
            dict(
                INDEX_NAME="X",
                TIME_STAMP=pd.Timestamp(scope[key]),
                SYMBOL="NEW",
                SECURITY_ID="TEST:NEW",
                CONFIDENCE="verified",
            )
            for scope in scopes
            for key in ["start", "end"]
        ]
    )
    cp = tmp_path / "candidate.parquet"
    op = tmp_path / "observed.parquet"
    cert = tmp_path / "certificate.json"
    candidate.to_parquet(cp, index=False)
    candidate.to_parquet(op, index=False)
    prices, members = md.source_revisions()
    cert.write_text(
        json.dumps(
            dict(
                accepted=True,
                reviewer="fixture",
                candidate_sha256=sha256(cp),
                identity_registry_sha256=sha256(REGISTRY),
                price_revision=prices,
                membership_revision=members,
                validated_intervals=scopes,
                observed_snapshots_path=str(op),
                observed_snapshots_sha256=sha256(op),
            )
        )
    )
    publish(cp, cert, tmp_path / "backup")
    actual = md.load_snapshot().membership
    for day in pd.date_range("2025-01-01", "2025-02-01"):
        inside = any(pd.Timestamp(s["start"]) <= day <= pd.Timestamp(s["end"]) for s in scopes)
        expected = {"X": ["NEW"]} if inside else md.constituents_at(history, day, ["X"])
        assert md.constituents_at(actual, day, ["X"]) == expected, day
