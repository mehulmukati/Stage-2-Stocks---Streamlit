from datetime import date

import pandas as pd

from app_live_signal import (
    _classify_weight_changes,
    _comparison_weights_for_live_event,
    _next_business_day,
    _normalise_broker_snapshot,
    _portfolio_from_replay,
    _read_broker_snapshot,
    _reconcile_actual_portfolio,
    _simulation_start_date,
    _strategy_fingerprint,
    _symbols_needed_for_replay,
)


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


def test_next_business_day_skips_nse_holiday():
    assert _next_business_day(date(2025, 8, 26), {"2025-08-27"}) == date(2025, 8, 28)


def test_live_comparison_prefers_current_drifted_weights():
    current = {
        "marg_weights": {"A": 55.0, "B": 45.0},
        "pre_rebalance_marg_weights": {"A": 55.0, "B": 45.0},
    }
    previous = {"marg_weights": {"A": 50.0, "B": 50.0}}

    weights = _comparison_weights_for_live_event(current, previous, "marg_weights", "pre_rebalance_marg_weights", False)
    changes = _classify_weight_changes(weights, current["marg_weights"])

    assert all(row["Action"] == "HOLD" for row in changes)


def test_live_comparison_fallback_maps_corporate_action():
    current = {
        "corporate_actions": [{"old_symbol": "OLD", "successor_symbol": "NEW"}],
    }
    previous = {"marg_weights": {"OLD": 40.0, "KEEP": 60.0}}

    weights = _comparison_weights_for_live_event(current, previous, "marg_weights", "pre_rebalance_marg_weights", False)

    assert weights == {"NEW": 40.0, "KEEP": 60.0}


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


def test_broker_snapshot_accepts_common_aliases_and_nse_suffix():
    frame, errors = _normalise_broker_snapshot(
        pd.DataFrame({"Trading Symbol": ["akums.ns", "CUPID"], "Net Qty": [10, 20]})
    )

    assert errors == []
    assert frame.to_dict("records") == [
        {"Ticker": "AKUMS", "Quantity": 10},
        {"Ticker": "CUPID", "Quantity": 20},
    ]


def test_broker_snapshot_blocks_duplicates_and_fractional_quantities():
    _, errors = _normalise_broker_snapshot(
        pd.DataFrame({"Ticker": ["AKUMS", "AKUMS", "CUPID"], "Quantity": [10, 5, 1.5]})
    )

    assert any("Duplicate" in error for error in errors)
    assert any("whole shares" in error for error in errors)


def test_broker_csv_reader_uses_required_columns():
    frame, errors = _read_broker_snapshot("positions.csv", b"Symbol,Qty\nAKUMS,12\n")

    assert errors == []
    assert frame.to_dict("records") == [{"Ticker": "AKUMS", "Quantity": 12}]


def test_actual_portfolio_reconciliation_sells_off_model_and_preserves_reserve():
    snapshot = pd.DataFrame({"Ticker": ["A", "X"], "Quantity": [5, 2]})
    result = _reconcile_actual_portfolio(
        snapshot,
        cash=400,
        reserve_cash=100,
        target_weights={"A": 50.0003, "B": 50.0003},
        prices={"A": 100.0, "B": 200.0, "X": 50.0},
    )
    rows = _by_ticker(result["rows"])

    assert result["errors"] == []
    assert result["gross_value"] == 1_000
    assert rows["A"]["Action"] == "SELL"
    assert rows["A"]["Order quantity"] == -1
    assert rows["B"]["Action"] == "BUY"
    assert rows["B"]["Order quantity"] == 2
    assert rows["X"]["Action"] == "SELL"
    assert rows["X"]["Target quantity"] == 0
    assert round(rows["A"]["Strategy target (%)"] + rows["B"]["Strategy target (%)"], 8) == 100
    assert result["projected_cash"] >= 100


def test_actual_portfolio_reconciliation_blocks_missing_prices():
    result = _reconcile_actual_portfolio(
        pd.DataFrame({"Ticker": ["UNKNOWN"], "Quantity": [1]}),
        cash=0,
        reserve_cash=0,
        target_weights={},
        prices={},
    )

    assert result["rows"] == []
    assert "No signal-date price for: UNKNOWN" in result["errors"]


def test_replayed_portfolio_uses_whole_shares_and_preserves_residual_cash():
    positions, cash, errors = _portfolio_from_replay(
        {"A": 60.0, "B": 40.0},
        portfolio_value=1_000.0,
        prices={"A": 110.0, "B": 90.0},
    )

    assert errors == []
    assert positions.to_dict("records") == [
        {"Ticker": "A", "Quantity": 5},
        {"Ticker": "B", "Quantity": 4},
    ]
    assert cash == 90.0


def test_replayed_fresh_portfolio_starts_entirely_in_cash():
    positions, cash, errors = _portfolio_from_replay({}, 1_000.0, {})

    assert errors == []
    assert positions.empty
    assert cash == 1_000.0


def test_replay_start_uses_fixed_internal_buffer_and_ignores_legacy_input():
    base = {"portfolio_start": date(2026, 1, 5), "signal_date": date(2026, 9, 3)}

    assert _simulation_start_date({**base, "warmup": 26}) == date(2025, 1, 3)
    assert _simulation_start_date({**base, "warmup": 156}) == date(2025, 1, 3)


def test_legacy_warmup_does_not_change_replay_identity():
    base = {
        "portfolio_start": date(2026, 1, 5),
        "signal_date": date(2026, 9, 3),
        "freq": "weekly",
        "indices": ["Nifty 50"],
    }

    short = _strategy_fingerprint({**base, "warmup": 26}, "2026-09-03", "test")
    long = _strategy_fingerprint({**base, "warmup": 156}, "2026-09-03", "test")

    assert short == long
