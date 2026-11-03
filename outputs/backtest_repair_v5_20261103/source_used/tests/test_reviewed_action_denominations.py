"""Primary terms and share denominations, independent of an observed price drop."""

import pytest

from historical_price_model import parse_primary_action, primary_event_factor


def test_typo_after_share_is_not_a_second_rupee_amount():
    action = parse_primary_action("Interim Dividend Rs 6/- Per Share 9purpose Revised)")
    assert action["supported"] and action["cash"] == 6


@pytest.mark.parametrize("denomination,cash_before", [("pre_action_share", 29), ("post_action_share", 58)])
def test_combined_cash_share_terms_require_an_explicit_denominator(denomination, cash_before):
    from research_entitlements import reviewed_cash_share_action

    action = reviewed_cash_share_action(2, 29, denomination)
    assert action["cash"] == cash_before
    assert primary_event_factor(100, action) == (100 - cash_before) / 200
    with pytest.raises(ValueError):
        reviewed_cash_share_action(2, 29, "unspecified")
