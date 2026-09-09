from ha_ema_summary import build_ha_ema_summary


def test_summary_names_failed_entry_gates_while_waiting():
    state = {
        "ticker": "TEST",
        "sufficient_data": True,
        "status": "Waiting",
        "conditions": {"Bullish HA": True, "Volume gate": False, "1-year return gate": False},
    }
    summary = build_ha_ema_summary(state)
    assert "Volume gate" in summary
    assert "1-year return gate" in summary


def test_summary_explains_insufficient_history():
    summary = build_ha_ema_summary({"ticker": "TEST", "sufficient_data": False})
    assert "52-week" in summary


def test_summary_explains_weekend_decision_and_following_week_execution():
    state = {
        "ticker": "TEST",
        "sufficient_data": True,
        "status": "BUY planned",
        "conditions": {"Bullish HA": True},
    }

    summary = build_ha_ema_summary(state)

    assert "weekend long-entry decision" in summary
    assert "following week's first available Open" in summary
