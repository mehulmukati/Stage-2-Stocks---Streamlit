import re

from ichimoku_summary import build_ichimoku_summary


def _state(**overrides):
    state = {
        "ticker": "RELIANCE",
        "sufficient_data": True,
        "price_position": "above",
        "displayed_cloud": "bullish",
        "projected_cloud": "bullish",
        "tk_relation": "above",
        "distance_pct": 4.24,
        "latest_date": "2026-08-25",
        "last_cross": {
            "direction": "bullish",
            "strength": "strong bullish",
            "age_sessions": 12,
        },
    }
    state.update(overrides)
    return state


def test_summary_is_deterministic_for_same_state():
    state = _state()
    assert build_ichimoku_summary(state) == build_ichimoku_summary(state)


def test_summary_contains_only_matching_bullish_facts():
    summary = build_ichimoku_summary(_state())
    assert "4.2%" in summary
    assert "strong bullish" in summary
    assert "12 sessions" in summary
    assert "bearish" not in summary


def test_inside_cloud_omits_distance():
    summary = build_ichimoku_summary(_state(price_position="inside", distance_pct=None))
    assert "4.2%" not in summary
    assert "inside" in summary or "within" in summary


def test_singular_session_is_grammatical():
    state = _state()
    state["last_cross"] = {"direction": "bearish", "strength": "neutral bearish", "age_sessions": 1}
    summary = build_ichimoku_summary(state)
    assert "1 session ago" in summary
    assert "1 sessions" not in summary


def test_weekly_summary_uses_weeks_for_cross_age():
    state = _state(timeframe="Weekly")
    state["last_cross"] = {"direction": "bullish", "strength": "strong bullish", "age_sessions": 3}
    summary = build_ichimoku_summary(state)
    assert "3 weeks ago" in summary
    assert "3 sessions" not in summary


def test_unavailable_state_has_clear_fallback():
    summary = build_ichimoku_summary(
        _state(
            sufficient_data=False,
            price_position="unavailable",
            displayed_cloud="unavailable",
            projected_cloud="unavailable",
            tk_relation="unavailable",
            last_cross=None,
        )
    )
    assert "not yet" in summary.lower()


def test_summary_has_no_placeholders_or_recommendation_language():
    summary = build_ichimoku_summary(_state())
    assert not re.search(r"[{}]", summary)
    assert not re.search(r"\b(buy|sell|target|guaranteed)\b", summary, flags=re.IGNORECASE)
