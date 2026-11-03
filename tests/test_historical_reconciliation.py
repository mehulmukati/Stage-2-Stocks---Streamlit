import hashlib
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from historical_price_evidence import parse_vendor_chart
from historical_reconciliation import (
    apply_security_conversion,
    parse_review_transition_section,
    project_raw_identifier_observation,
    walk_membership,
)
from historical_universe import parse_primary_holidays, read_bhavcopy, read_nifty500_archive, validate_constituent_cache


def test_cache_is_bound_to_raw_pdf_and_rejects_replaced_payload(tmp_path):
    pdf = Path(__file__).parent / "fixtures/nifty500_aug2020.pdf"
    metadata = json.loads(pdf.with_suffix(".source.json").read_text())
    archive = tmp_path / "indices.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(metadata["member"], pdf.read_bytes())
    rows = read_nifty500_archive(archive)
    validate_constituent_cache(rows, archive)
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(metadata["member"], pdf.read_bytes() + b"\nchanged raw artifact")
    with pytest.raises(ValueError, match="digest mismatch"):
        validate_constituent_cache(rows, archive)
    rows["pdf_sha256"] = hashlib.sha256(pdf.read_bytes() + b"\nchanged raw artifact").hexdigest()
    rows["archive"] = "different.zip"
    with pytest.raises(ValueError, match="member mismatch"):
        validate_constituent_cache(rows, archive)


def action(old="ISIN:INE962Y01013", new="ISIN:INE962Y01021"):
    return dict(
        old_security_id=old,
        new_security_id=new,
        ex_date="2020-04-03",
        identifier_effective_date="2020-04-08",
        membership_conversion_supported=True,
    )


def test_ex_price_date_and_identifier_date_are_distinct_with_independent_endpoints():
    a = action()
    old, new = a["old_security_id"], a["new_security_id"]
    events = pd.DataFrame([dict(EVENT_DATE="2020-04-09", EVENT_TYPE="EXCL", SECURITY_ID=new)])
    states, result = walk_membership({old}, set(), events, [a], "2020-04-01", "2020-04-10")
    assert states[pd.Timestamp("2020-04-01")] == {old}
    assert states[pd.Timestamp("2020-04-08")] == {new}
    assert states[pd.Timestamp("2020-04-09")] == set()
    assert result["forward_anchor_matches"] and result["reverse_initial_matches"]
    assert not result["forward_conflicts"] and not result["reverse_conflicts"]
    _, bad = walk_membership({old}, {new}, events, [a], "2020-04-01", "2020-04-10")
    assert not bad["forward_anchor_matches"] and bad["reverse_conflicts"]


def test_dated_primary_two_digit_year_is_explicit_and_archive_mismatch_fails(tmp_path):
    archive = tmp_path / "cm13JUL2020bhav.csv.zip"
    csv = "SYMBOL,ISIN,SERIES,TIMESTAMP,CLOSE\nOLD,INE962Y01013,EQ,13-Jul-20,100\n"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("primary.csv", csv)
    assert read_bhavcopy(archive).date.iloc[0] == pd.Timestamp("2020-07-13")
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("primary.csv", csv.replace("13-Jul-20", "12-Jul-20"))
    with pytest.raises(ValueError, match="archive date"):
        read_bhavcopy(archive)


def test_primary_calendar_no_space_after_comma_and_muhurat_override():
    text = """9 September 13,2018 Thursday Ganesh Chaturthi
10 September 20,2018 Thursday Moharram
13 November 07, 2018 Wednesday Diwali-Laxmi Pujan
*Muhurat Trading will be conducted on Wednesday, November 07, 2018.
"""
    closed, special = parse_primary_holidays(text, 2018)
    assert pd.Timestamp("2018-09-13") in closed
    assert pd.Timestamp("2018-09-20") in closed
    assert special == {pd.Timestamp("2018-11-07")}
    assert len(closed - special) == 2
    with pytest.raises(ValueError, match="Mixed"):
        parse_primary_holidays(text, 2019)


def test_nonmember_conversion_never_adds_name_and_share_classes_cannot_collapse():
    a = action()
    assert apply_security_conversion(set(), a) == set()
    with pytest.raises(ValueError, match="coexist"):
        apply_security_conversion({a["old_security_id"], a["new_security_id"]}, a)
    with pytest.raises(ValueError, match="Unsupported"):
        apply_security_conversion(set(), {**a, "membership_conversion_supported": False})


def test_economic_ex_date_projection_preserves_raw_identifier_evidence():
    a = action()
    raw = {a["old_security_id"]}
    assert project_raw_identifier_observation(raw, "2020-04-02", [a]) == raw
    assert project_raw_identifier_observation(raw, "2020-04-03", [a]) == {a["new_security_id"]}
    assert project_raw_identifier_observation(raw, "2020-04-07", [a]) == {a["new_security_id"]}
    assert raw == {a["old_security_id"]}
    events = pd.DataFrame([dict(EVENT_DATE="2020-04-09", EVENT_TYPE="EXCL", SECURITY_ID=a["new_security_id"])])
    states, checks = walk_membership(raw, set(), events, [a], "2020-04-01", "2020-04-10", action_date_field="ex_date")
    assert states[pd.Timestamp("2020-04-03")] == {a["new_security_id"]}
    assert checks["forward_anchor_matches"] and checks["reverse_initial_matches"]


def test_complete_review_and_amendment_preserve_superseded_transition_provenance():
    fixture = Path(__file__).parent / "fixtures/nifty500_review_amendment_2017.json"
    main, amendment = json.loads(fixture.read_text())
    main_rows = parse_review_transition_section(main["section_text"])
    amendment_rows = parse_review_transition_section(amendment["section_text"])
    assert len(main_rows) == 50 and len(amendment_rows) == 2
    amended = {(r["direction"], r["symbol"]) for r in amendment_rows}
    retained = [r for r in main_rows if (r["direction"], r["symbol"]) not in amended]
    assert len(retained) == 48
    assert amended == {("EXCL", "RELCAPITAL"), ("INCL", "MFSL")}
    assert len(main_rows) == 50  # original notice retained, not silently rewritten
    truncated = main["section_text"].replace("     2      Anant Raj Ltd.", "     7      Anant Raj Ltd.")
    assert truncated != main["section_text"]
    with pytest.raises(ValueError, match="Incomplete"):
        parse_review_transition_section(truncated)


def chart():
    return {
        "chart": {
            "error": None,
            "result": [
                {
                    "meta": {"symbol": "OLD.NS", "currency": "INR", "exchangeTimezoneName": "Asia/Kolkata"},
                    "timestamp": [1585872000, 1586217600],
                    "indicators": {
                        "quote": [
                            {
                                "open": [100, None],
                                "high": [110, None],
                                "low": [90, None],
                                "close": [105, None],
                                "volume": [1000, None],
                            }
                        ],
                        "adjclose": [{"adjclose": [52.5, None]}],
                    },
                    "events": {"splits": {"1": {"splitRatio": "5:1"}}},
                }
            ],
        }
    }


def test_adjusted_vendor_units_do_not_receive_second_split_or_fabricated_gap():
    rows, events = parse_vendor_chart(chart(), "OLD.NS")
    assert rows.iloc[0].adjusted_close == 52.5  # factor .5 only; not a further x5 action
    assert rows.iloc[0].adjusted_open == 50
    assert rows.iloc[0].reported_volume == 1000  # retain vendor convention, no conversion
    assert rows.iloc[1].status == "unsupported_vendor_bar"
    assert pd.isna(rows.iloc[1].adjusted_close)
    assert events["splits"]  # entitlements retained as evidence, never credited


@pytest.mark.parametrize(
    "mutation,match",
    [("symbol", "symbol mismatch"), ("currency", "currency/calendar"), ("array", "Truncated"), ("date", "Duplicate")],
)
def test_vendor_mismatch_and_malformed_sources_fail_closed(mutation, match):
    source = chart()
    r = source["chart"]["result"][0]
    if mutation == "symbol":
        r["meta"]["symbol"] = "SUCCESSOR.NS"
    elif mutation == "currency":
        r["meta"]["currency"] = "USD"
    elif mutation == "array":
        r["indicators"]["adjclose"][0]["adjclose"].pop()
    else:
        r["timestamp"][1] = r["timestamp"][0]
    with pytest.raises(ValueError, match=match):
        parse_vendor_chart(source, "OLD.NS")
