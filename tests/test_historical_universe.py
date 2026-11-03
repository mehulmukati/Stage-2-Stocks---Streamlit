import hashlib
import json
import zipfile
from pathlib import Path

import pandas as pd

from historical_universe import match_event_identity, read_nifty500_archive
from security_identity import load_aliases, resolve_name


def test_pinned_primary_snapshot_preserves_names_share_classes_and_date(tmp_path):
    fixture = Path(__file__).parent / "fixtures/nifty500_aug2020.pdf"
    metadata = json.loads(fixture.with_suffix(".source.json").read_text())
    assert hashlib.sha256(fixture.read_bytes()).hexdigest() == metadata["sha256"]
    archive = tmp_path / "indices_dataAug2020.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(metadata["member"], fixture.read_bytes())
    rows = read_nifty500_archive(archive)
    assert len(rows) == 501  # verified security rows in this specific source, not a general size rule
    assert rows.date.eq(pd.Timestamp("2020-08-31")).all()
    names = rows.set_index("symbol").name
    assert names["BALKRISIND"] == "Balkrishna Industries Ltd."
    assert names["ABBOTINDIA"] == "Abbott India Ltd."
    assert "TATAMTRDVR" in names.index and "TATAMOTORS" in names.index
    assert names["ADANIPORTS"] == "Adani Ports and Special Economic Zone Ltd."


def test_historical_join_is_exact_dated_and_security_specific():
    obs = pd.DataFrame(
        dict(
            name=["Abbott India Ltd.", "ABB India Ltd."],
            symbol=["ABBOTINDIA", "ABB"],
            date=pd.to_datetime(["2020-08-31"] * 2),
            archive=["primary.zip"] * 2,
            source_row_id=["pdf:1", "pdf:2"],
        )
    )
    bhav = pd.DataFrame(
        dict(
            SYMBOL=["ABBOTINDIA", "ABB"],
            ISIN=["INE358A01014", "INE117A01022"],
            SERIES=["EQ"] * 2,
            date=pd.to_datetime(["2020-08-20"] * 2),
            source_row_id=["exchange:1", "exchange:2"],
            archive=["exchange.zip"] * 2,
        )
    )
    alias, reason = match_event_identity(dict(SCRIP_NAME="Abbott India Limited", EVENT_DATE="2020-08-20"), obs, bhav)
    assert reason is None
    assert alias["exchange_symbol"] == "ABBOTINDIA" and alias["security_id"] == "ISIN:INE358A01014"
    assert alias["vendor_symbol"] is None
    assert match_event_identity(dict(SCRIP_NAME="Abbott India Limited", EVENT_DATE="2017-09-29"), obs, bhav)[0] is None
    assert (
        match_event_identity(dict(SCRIP_NAME="Abbott Different Company Ltd.", EVENT_DATE="2020-08-20"), obs, bhav)[0]
        is None
    )


def test_missing_execution_does_not_erase_identity_but_multiple_isins_are_unresolved():
    obs = pd.DataFrame(
        dict(
            name=["Old Company Ltd."],
            symbol=["OLD"],
            date=pd.to_datetime(["2020-08-31"]),
            archive=["primary.zip"],
            source_row_id=["pdf:1"],
        )
    )
    prior = pd.DataFrame(
        dict(
            SYMBOL=["OLD"],
            ISIN=["INE358A01014"],
            SERIES=["EQ"],
            date=pd.to_datetime(["2020-08-20"]),
            source_row_id=["exchange:1"],
            archive=["exchange.zip"],
        )
    )
    event = dict(SCRIP_NAME="Old Company Ltd.", EVENT_DATE="2020-08-24")
    alias, _ = match_event_identity(event, obs, prior)
    assert alias is not None and "no event-day execution inferred" in alias["validity_basis"]
    other = prior.copy()
    other["ISIN"] = "INE117A01022"
    assert match_event_identity(event, obs, pd.concat([prior, other]))[0] is None


def test_reusable_alias_resolves_multiple_proven_events_without_backfill(tmp_path):
    alias = load_aliases()[0]
    alias.update(valid_from="2017-09-29", valid_to="2020-06-26", verified_effective_dates=["2017-09-29", "2020-06-26"])
    path = tmp_path / "aliases.json"
    path.write_text(json.dumps(dict(aliases=[alias])))
    decisions = load_aliases(path)
    assert resolve_name(alias["name"], "2017-09-29", decisions)
    assert resolve_name(alias["name"], "2020-06-26", decisions)
    assert resolve_name(alias["name"], "2018-01-01", decisions) is None
