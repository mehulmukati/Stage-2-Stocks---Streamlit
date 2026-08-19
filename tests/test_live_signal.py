import pandas as pd

from app_live_signal import _classify_weight_changes, _symbols_needed_for_replay


def _by_ticker(changes: list[dict]) -> dict[str, dict]:
    return {row["Ticker"]: row for row in changes}


def test_classify_weight_changes_covers_all_four_trade_types():
    previous = {"ADD": 30.0, "TRIM": 20.0, "KEEP": 10.0, "EXIT": 40.0}
    new = {"ADD": 35.0, "TRIM": 15.0, "KEEP": 10.0005, "NEW": 39.9995}

    changes = _by_ticker(_classify_weight_changes(previous, new))

    assert set(changes) == {"ADD", "TRIM", "KEEP", "EXIT", "NEW"}
    assert changes["ADD"]["Action"] == "BUY"
    assert changes["NEW"]["Action"] == "BUY"
    assert changes["TRIM"]["Action"] == "SELL"
    assert changes["EXIT"]["Action"] == "SELL"
    assert changes["KEEP"]["Action"] == "HOLD"
    assert changes["NEW"]["Previous weight (%)"] == 0.0
    assert changes["EXIT"]["New weight (%)"] == 0.0


def test_classify_weight_changes_uses_tolerance_for_non_actionable_drift():
    changes = _by_ticker(
        _classify_weight_changes(
            {"UP": 10.0, "DOWN": 20.0},
            {"UP": 10.0009, "DOWN": 19.9991},
        )
    )

    assert changes["UP"]["Action"] == "HOLD"
    assert changes["DOWN"]["Action"] == "HOLD"


def test_classify_weight_changes_treats_fresh_portfolio_as_buys():
    changes = _classify_weight_changes({}, {"A": 60.0, "B": 40.0})

    assert [row["Ticker"] for row in changes] == ["A", "B"]
    assert all(row["Action"] == "BUY" for row in changes)


def test_classify_weight_changes_handles_empty_portfolios():
    assert _classify_weight_changes({}, {}) == []


def test_replay_symbols_retain_historical_member_and_merger_successor():
    compositions = pd.DataFrame(
        {
            "INDEX_NAME": ["NIFTY SMALLCAP 250", "NIFTY SMALLCAP 250"],
            "SYMBOL": ["JBCHEPHARM", "PFOCUS"],
        }
    )
    actions = [{"old_symbol": "JBCHEPHARM", "successor_symbol": "TORNTPHARM"}]

    symbols = _symbols_needed_for_replay(
        ["Nifty Smallcap 250"],
        {"Nifty Smallcap 250": ["PFOCUS"]},
        compositions,
        actions,
    )

    assert symbols == {"JBCHEPHARM", "PFOCUS", "TORNTPHARM"}
