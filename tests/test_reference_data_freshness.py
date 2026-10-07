"""Membership freshness follows verification time, not effective date or mtime."""

from datetime import datetime, timezone

import pandas as pd
import pytest

import data
import market_data as md


def test_old_membership_effective_date_with_recent_verification_is_valid(market):
    assert not any("Constituents" in message for _, message in data.check_data_freshness())


def test_new_file_mtime_does_not_hide_stale_verification(market):
    _, history, _ = market
    md.write_source(history, md.MEMBERSHIP_PATH, {"verified_at": "2020-01-01T00:00:00+00:00"})
    assert any("last verified" in message for _, message in data.check_data_freshness())


def test_missing_shared_membership_blocks_even_with_legacy_files(market):
    md.MEMBERSHIP_PATH.unlink()
    assert any(
        level == "error" and "Shared market sources unavailable" in message
        for level, message in data.check_data_freshness()
    )


def test_invalid_historical_dates_are_rejected_without_replacing_revision(market):
    _, history, _ = market
    revision = md.source_revisions()
    history.loc[0, "TIME_STAMP"] = pd.NaT
    with pytest.raises(ValueError, match="Missing membership"):
        md.write_source(history, md.MEMBERSHIP_PATH)
    assert md.source_revisions() == revision


def test_unverified_current_observation_is_never_backdated(market):
    _, history, days = market
    revised = md.append_membership_snapshot(
        history,
        {"Nifty 50": ["B"]},
        days[-1],
        datetime.now(timezone.utc).isoformat(),
        "NSE CSV",
        "observed_not_effective",
    )
    md.write_source(revised, md.MEMBERSHIP_PATH)
    snapshot = md.load_snapshot(days[-11])
    assert snapshot.symbols() == {"A", "LOWVOL"}
    assert "observed_not_effective" in revised.DATE_BASIS.dropna().unique()


def test_tradable_membership_ignores_dummy_securities(market):
    _, history, days = market
    revised = pd.concat(
        [history, pd.DataFrame(dict(INDEX_NAME=["Nifty 50"], TIME_STAMP=[days[-10]], SYMBOL=["DUMMYHEG"]))],
        ignore_index=True,
    )
    md.write_source(revised, md.MEMBERSHIP_PATH)
    assert "DUMMYHEG" in md.load_snapshot().membership.SYMBOL.values
    assert "DUMMYHEG" not in md.load_snapshot().symbols()
