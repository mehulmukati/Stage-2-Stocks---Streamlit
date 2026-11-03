import zipfile

import pandas as pd
import pytest

from historical_price_model import (
    back_adjust_primary_quotes,
    close_factor_seam_audit,
    corroborate_action_terms,
    parse_primary_action,
    primary_event_factor,
    read_primary_action_bulletin,
)


def raw(values, dates=None):
    return pd.DataFrame(
        {name: values for name in ["OPEN", "HIGH", "LOW", "CLOSE"]} | {"TOTTRDQTY": [1000] * len(values)},
        index=pd.to_datetime(dates or pd.date_range("2017-01-02", periods=len(values))),
    )


def test_independent_cash_and_split_terms_remove_only_their_mechanical_effect():
    source = raw([100, 50, 49])
    actions = [
        dict(date="2017-01-03", **parse_primary_action("Face Value Split From Rs 10 To Rs 5")),
        dict(date="2017-01-04", **parse_primary_action("Dividend Rs 1 Per Share")),
    ]
    prices, decisions = back_adjust_primary_quotes(source, actions)
    assert list(prices.Close) == [49, 49, 49]
    assert list(prices.Volume) == [1000, 1000, 1000]
    assert len(prices) == 3 and not prices.accepted_for_backtest.any()
    assert decisions[0]["factor"] == 0.5 and decisions[1]["factor"] == 0.98
    assert list(source.CLOSE) == [100, 50, 49]


def test_duplicate_series_action_does_not_deduct_dividend_twice():
    action = dict(date="2017-01-03", **parse_primary_action("Dividend Rs 1 Per Share"))
    prices, decisions = back_adjust_primary_quotes(raw([100, 99]), [action, dict(action)])
    assert list(prices.Close) == [99, 99]
    assert len(decisions) == 1


def test_explicit_special_cash_components_and_percentage_annotation():
    assert parse_primary_action("Final Dividend Rs 6 Plus Special Dividend Rs 4 Per Share")["cash"] == 10
    assert parse_primary_action("Dividend Rs 15 Per Share (150%)")["cash"] == 15
    assert not parse_primary_action("Dividend 150%")["supported"]
    assert not parse_primary_action("Dividend Rs 10 including Special Dividend Rs 4")["supported"]


def test_combined_split_and_ordinary_bonus_without_second_quantity_credit():
    action = parse_primary_action("Bonus 1:2/Sub-Division From Rs 2 To Re 1 Per Share")
    assert action["share_ratio"] == 3
    prices, _ = back_adjust_primary_quotes(raw([300, 100]), [{**action, "date": "2017-01-03"}])
    assert list(prices.Close) == [100, 100]
    assert not parse_primary_action("Bonus Preference Shares 21:1")["supported"]
    assert not parse_primary_action("Bonus - 1 Debenture For 1 Equity Share Held")["supported"]


@pytest.mark.parametrize(
    "text",
    [
        "Rights 9:77 Partly Paid @ Premium Rs 100",
        "Scheme Of Arrangement",
        "Composite Scheme Of Demerger",
        "Bonus 1:1 / Dividend Rs 5 Per Share",
    ],
)
def test_unknown_entitlements_are_not_inferred_from_the_price_drop(text):
    action = dict(date="2017-01-03", **parse_primary_action(text))
    prices, decisions = back_adjust_primary_quotes(raw([100, 30]), [action])
    assert not decisions[0]["applied"] and list(prices.Close) == [100, 30]


def test_distinct_same_day_cash_terms_require_review_instead_of_compounding():
    actions = [dict(date="2017-01-03", **parse_primary_action(f"Dividend Rs {cash} Per Share")) for cash in [1, 2]]
    prices, decisions = back_adjust_primary_quotes(raw([100, 97]), actions)
    assert not any(a["applied"] for a in decisions)
    assert list(prices.Close) == [100, 97]


def test_missing_ex_date_quote_is_not_fabricated_by_a_known_adjustment():
    source = raw([100, 99], ["2017-01-02", "2017-01-04"])
    action = dict(date="2017-01-03", **parse_primary_action("Dividend Rs 1 Per Share"))
    prices, decisions = back_adjust_primary_quotes(source, [action])
    assert len(prices) == 2 and pd.Timestamp("2017-01-03") not in prices.index
    assert list(prices.Close) == [99, 99]
    assert decisions[0]["ex_quote_available"] is False


def test_return_factor_audit_catches_error_invisible_to_ohlc_shape():
    index = pd.to_datetime(["2017-01-02", "2017-01-03"])
    source = pd.Series([100, 50], index=index)
    expected = pd.Series([0.5, 1], index=index)
    good = close_factor_seam_audit(source, pd.Series([100, 100], index=index), expected)
    bad = close_factor_seam_audit(source, pd.Series([100, 50], index=index), expected)
    assert not good.mismatch.any() and bad.mismatch.iloc[-1]


def test_reviewed_rights_uses_issue_price_including_premium_and_no_cash_credit():
    action = dict(
        kind="reviewed_fully_paid_rights",
        supported=True,
        rights_ratio=87 / 38,
        subscription_price=12.5,
        reviewed_source="pinned issuer and NSE circular",
        share_ratio=1,
        cash=0,
    )
    previous = 30
    oracle = (38 * previous + 87 * 12.5) / (125 * previous)
    assert primary_event_factor(previous, action) == pytest.approx(oracle)
    with pytest.raises(ValueError, match="reviewed"):
        primary_event_factor(previous, {**action, "reviewed_source": None})


def test_original_bulletin_rows_corroborate_dated_symbol_and_terms(tmp_path):
    path = tmp_path / "PR010117.zip"
    text = (
        "SERIES,SYMBOL,EX_DT,PURPOSE\n"
        "EQ,OLD,03/01/2017,AGM/DIV-RS 1 PER SHARE\n"
        "BE,OLD,03/01/2017,AGM/DIV-RS 1 PER SHARE\n"
    )
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("Bc010117.csv", text)
    rows = read_primary_action_bulletin(path).to_dict("records")
    proposed = dict(date="2017-01-03", historical_symbol="OLD", **parse_primary_action("Dividend Rs 1 Per Share"))
    assert len(corroborate_action_terms(proposed, rows)) == 2  # provenance, not two distributions
    assert not corroborate_action_terms({**proposed, "cash": 2}, rows)
    assert not corroborate_action_terms({**proposed, "historical_symbol": "SUCCESSOR"}, rows)


def test_debt_action_cannot_join_equity_by_restated_isin():
    from historical_price_model import select_primary_equity_actions

    source = pd.DataFrame(
        [
            {"series": "EQ", "isin": "same", "subject": "Dividend Rs 2"},
            {"series": "H5", "isin": "same", "subject": "Interest Payment"},
        ]
    )
    equity, excluded = select_primary_equity_actions(source)
    assert list(equity.series) == ["EQ"]
    assert list(excluded.series) == ["H5"]
    assert len(source) == 2


def test_factor_audit_orders_outer_calendar_and_rejects_duplicates():
    days = pd.to_datetime(["2017-01-02", "2017-01-03", "2017-01-04"])
    raw_close = pd.Series([100, 100], index=days[[0, 2]])
    vendor = pd.Series([100, 100, 100], index=days)
    expected = pd.Series(1.0, index=days)
    result = close_factor_seam_audit(raw_close, vendor, expected)
    assert list(result.index) == list(days)
    assert not result.checked.any()  # Missing intervening raw quote stays visible.
    duplicate = pd.Series([100, 100], index=[days[0], days[0]])
    with pytest.raises(ValueError):
        close_factor_seam_audit(duplicate, duplicate, duplicate)


def test_explicit_primary_bulletin_abbreviations():
    assert parse_primary_action("Fv Splt Frm Rs 10 To Re 1")["share_ratio"] == 10
    assert parse_primary_action("FV SPLT FROM RS 2 TO RE 1")["share_ratio"] == 2
    assert parse_primary_action("Bonus Issue 1 : 1")["share_ratio"] == 2
