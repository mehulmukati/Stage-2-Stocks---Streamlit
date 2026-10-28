import copy
import json
from datetime import date

import pandas as pd
import pytest

import backtest_engine as engine
import live_signal_state as state
from app_live_signal import _rebalance_anchor, _reconcile_actual_portfolio, _request_identity, _strategy_fingerprint


@pytest.fixture
def live_market(monkeypatch):
    calendar = pd.bdate_range("2026-09-01", "2026-10-30").difference(pd.DatetimeIndex(["2026-10-02", "2026-10-20"]))
    symbols = {"A", "BUFFER", "EXIT", "NEW", "UNHELD_BUFFER"}
    frames = {
        s: pd.DataFrame({"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0, "Volume": 200000.0}, index=calendar)
        for s in symbols
    }

    class Snapshot:
        revisions = ("prices-v1", "membership-v1")

        def trading_calendar(self):
            return calendar

        def symbols(self, indices=None, as_of=None):
            return symbols

        def ohlcv(self, symbols=None, as_of=None):
            return {s: f.loc[:as_of] for s, f in frames.items() if s in symbols}

    scores = {s: 5.0 for s in symbols}
    monkeypatch.setattr(state, "_stage2_scores", lambda *args: dict(scores))
    monkeypatch.setattr(
        engine,
        "rank_universe_at_date",
        lambda *args, **kwargs: (["NEW", "A", "UNHELD_BUFFER", "BUFFER"], {"EXIT": "quality_filter"}),
    )
    monkeypatch.setattr(engine, "load_corporate_actions", lambda: [])
    params = dict(
        state.STRATEGY_DEFAULTS,
        **dict(
            signal_date=date(2026, 10, 13),
            rebalance_anchor=date(2026, 10, 6),
            portfolio_source="continuation",
            continuation_action="initialize",
            broker_snapshot=pd.DataFrame({"Ticker": ["A", "BUFFER", "EXIT"], "Quantity": [10, 20, 30]}),
            m=2,
            n=4,
            indices=["Nifty 50"],
            max_pos=0,
            cash_balance=0,
            reserve_cash=0,
        )
    )
    return Snapshot(), params, scores, frames


def _next(snapshot, params):
    initial = state.continue_portfolio(params, snapshot)
    assert initial["continuation_status"] == "initialized", initial
    return dict(
        params,
        signal_date=date(2026, 10, 20),
        continuation_action="continue",
        continuation_checkpoint=initial["checkpoint"],
    )


def test_actual_buffer_holdings_retained_and_absent_buffer_names_not_rebought(live_market):
    snapshot, params, _, _ = live_market
    result = state.continue_portfolio(_next(snapshot, params), snapshot)
    event = result["holdings_log"][-1]
    assert event["holdings"] == ["A", "BUFFER", "NEW"]
    assert event["entries"] == ["NEW (rank #1)"]
    assert event["exits"] == ["EXIT (excluded: quality_filter)"]
    assert event["date"] == pd.Timestamp("2026-10-19")
    assert event["marg_weights"]["NEW"] <= 100 / 3 + 1e-8


def test_proposals_are_repeatable_and_never_mutate_or_advance_confirmed_checkpoint(live_market):
    snapshot, params, _, _ = live_market
    params = _next(snapshot, params)
    before = copy.deepcopy(params["continuation_checkpoint"])
    a = state.continue_portfolio(params, snapshot)
    b = state.continue_portfolio(params, snapshot)
    assert a["proposal_checkpoint"] == b["proposal_checkpoint"]
    assert params["continuation_checkpoint"] == before
    with pytest.raises(ValueError, match="confirmed"):
        state.read_checkpoint(a["proposal_checkpoint"])
    confirmed = state.confirm_checkpoint(a["proposal_checkpoint"])
    assert state.read_checkpoint(json.dumps(confirmed))["parent_checkpoint_id"] == before["checkpoint_id"]
    params.update(continuation_checkpoint=confirmed, signal_date=date(2026, 10, 26))
    assert state.continue_portfolio(params, snapshot)["continuation_status"] == "not_due"


def test_revision_changes_do_not_recalculate_previous_stage2_scores(live_market):
    snapshot, params, scores, _ = live_market
    params = _next(snapshot, params)
    scores["BUFFER"] = 2.0
    snapshot.revisions = ("repaired-prices", "new-membership")
    result = state.continue_portfolio(params, snapshot)
    assert "BUFFER (S2 -3)" in result["holdings_log"][-1]["exits"]
    assert params["continuation_checkpoint"]["stage2_scores"]["BUFFER"] == 5


@pytest.mark.parametrize(
    "field,value", [("m", 3), ("rebalance_anchor", date(2026, 10, 5)), ("sort_method", "3 months")]
)
def test_changed_strategy_or_schedule_requires_explicit_rebaseline(live_market, field, value):
    snapshot, params, _, _ = live_market
    params = _next(snapshot, params)
    params[field] = value
    assert "differs from the checkpoint" in state.continue_portfolio(params, snapshot)["error"]


def test_policy_upgrade_requires_explicit_rebaseline(live_market):
    snapshot, params, _, _ = live_market
    params = _next(snapshot, params)
    params["continuation_checkpoint"] = state.seal_checkpoint(
        dict(params["continuation_checkpoint"], equity_policy_version=-1)
    )
    assert "Calculation rules changed" in state.continue_portfolio(params, snapshot)["error"]


def test_missing_checkpoint_or_score_never_guesses_a_historical_portfolio(live_market):
    snapshot, params, _, _ = live_market
    assert "Upload" in state.continue_portfolio(dict(params, continuation_action="continue"), snapshot)["error"]
    params = _next(snapshot, params)
    altered = copy.deepcopy(params["continuation_checkpoint"])
    del altered["stage2_scores"]["BUFFER"]
    params["continuation_checkpoint"] = state.seal_checkpoint(altered)
    assert "baseline for: BUFFER" in state.continue_portfolio(params, snapshot)["error"]


def test_missed_multiple_decisions_and_later_price_dates_are_blocked(live_market):
    snapshot, params, _, _ = live_market
    params = _next(snapshot, params)
    assert (
        "Multiple scheduled"
        in state.continue_portfolio(dict(params, signal_date=date(2026, 10, 27)), snapshot)["error"]
    )
    assert (
        "pending decision is dated 2026-10-19"
        in state.continue_portfolio(dict(params, signal_date=date(2026, 10, 26)), snapshot)["error"]
    )


def test_missing_whole_market_session_cannot_become_a_liquidation_signal(live_market):
    snapshot, params, _, frames = live_market
    params = _next(snapshot, params)
    for symbol, frame in frames.items():
        frames[symbol] = frame.loc[:"2026-10-16"]
    result = state.continue_portfolio(params, snapshot)
    assert "No market-wide tradable prices" in result["error"]
    assert "holdings_log" not in result


def test_no_change_marginal_decision_preserves_actual_whole_share_positions(live_market, monkeypatch):
    snapshot, params, _, frames = live_market
    params["broker_snapshot"] = pd.DataFrame({"Ticker": ["A", "BUFFER"], "Quantity": [7, 13]})
    frames["A"]["Close"] = 113.37
    frames["BUFFER"]["Close"] = 94.21
    monkeypatch.setattr(engine, "rank_universe_at_date", lambda *args, **kwargs: (["A", "BUFFER", "NEW"], {}))
    result = state.continue_portfolio(_next(snapshot, params), snapshot)
    assert all(r["Action"] == "HOLD" for r in result["reconciliation"]["rows"])


def test_displacement_respects_existing_buffer_and_free_slots(live_market):
    snapshot, params, _, _ = live_market
    params["band"] = "displacement"
    params["broker_snapshot"] = pd.DataFrame({"Ticker": ["A", "BUFFER"], "Quantity": [10, 20]})
    result = state.continue_portfolio(_next(snapshot, params), snapshot)
    assert result["holdings_log"][-1]["holdings"] == ["A", "BUFFER"]
    assert result["holdings_log"][-1]["entries"] == []


def test_checkpoint_corruption_and_malformed_inputs_raise_clear_errors(live_market):
    snapshot, params, _, _ = live_market
    confirmed = _next(snapshot, params)["continuation_checkpoint"]
    changed = dict(confirmed, decision_date="2026-10-20")
    with pytest.raises(ValueError, match="changed"):
        state.read_checkpoint(changed)
    for bad in [b"[]", b"null", b"garbage"]:
        with pytest.raises(ValueError):
            state.read_checkpoint(bad)


def test_fixed_anchor_identity_survives_signal_weekday_change():
    params = {"portfolio_start": date(2025, 7, 2), "freq": "biweekly", "indices": ["Nifty 50"]}
    tuesday = dict(params, signal_date=date(2026, 10, 20))
    monday = dict(params, signal_date=date(2026, 10, 26))
    assert _rebalance_anchor(tuesday) == _rebalance_anchor(monday) == date(2025, 7, 1)
    assert _strategy_fingerprint(tuesday, "same-data", "shared") == _strategy_fingerprint(monday, "same-data", "shared")
    assert _request_identity(tuesday) != _request_identity(monday)


def test_calendar_endpoint_fallback_is_stable_when_replay_is_extended():
    sessions = pd.bdate_range("2026-10-01", "2026-10-27").difference(pd.DatetimeIndex(["2026-10-20"]))
    early = engine.get_anchored_rebalance_dates(
        sessions[sessions <= "2026-10-19"], "weekly", pd.Timestamp("2026-10-06"), through=pd.Timestamp("2026-10-20")
    )
    later = engine.get_anchored_rebalance_dates(
        sessions, "weekly", pd.Timestamp("2026-10-06"), through=pd.Timestamp("2026-10-27")
    )
    assert early[-1] == pd.Timestamp("2026-10-19")
    assert later[:-1] == early


@pytest.mark.parametrize(
    "frequency,cutoff,expected",
    [
        ("monthly", "2026-10-15", "2026-09-30"),
        ("quarterly", "2026-10-15", "2026-09-30"),
        ("half-yearly", "2026-10-15", "2026-06-30"),
    ],
)
def test_partial_period_does_not_create_extra_live_rebalances(frequency, cutoff, expected):
    sessions = pd.bdate_range("2026-01-01", cutoff)
    events = engine.get_anchored_rebalance_dates(
        sessions, frequency, pd.Timestamp("2026-01-01"), through=pd.Timestamp(cutoff)
    )
    assert events[-1] == pd.Timestamp(expected)


def test_rounding_and_nonfinite_money_inputs():
    positions = pd.DataFrame({"Ticker": ["A", "B"], "Quantity": [7, 13]})
    prices = {"A": 113.37, "B": 94.21}
    total = sum(prices[s] * q for s, q in zip(positions.Ticker, positions.Quantity))
    weights = {s: prices[s] * q / total * 100 for s, q in zip(positions.Ticker, positions.Quantity)}
    result = _reconcile_actual_portfolio(positions, 0, 0, weights, prices)
    assert all(r["Action"] == "HOLD" for r in result["rows"])
    assert _reconcile_actual_portfolio(positions, float("nan"), 0, weights, prices)["errors"]


def test_month_end_on_weekend_uses_completed_friday_close():
    sessions = pd.bdate_range("2026-09-01", "2026-10-30")
    events = engine.get_anchored_rebalance_dates(
        sessions, "monthly", pd.Timestamp("2026-01-01"), through=pd.Timestamp("2026-10-30")
    )
    assert events[-1] == pd.Timestamp("2026-10-30")


def test_order_price_uses_reported_tradable_bar_not_a_later_unverified_mark(live_market):
    snapshot, params, _, frames = live_market
    params = _next(snapshot, params)
    frames["A"].loc[pd.Timestamp("2026-10-19"), ["Close", "Volume"]] = [999, 0]
    result = state.continue_portfolio(params, snapshot)
    assert result["close_prices"]["A"] == 100
    assert result["latest_price_dates"]["A"] == "2026-10-16"


def test_missing_current_price_is_not_a_zero_stage2_score():
    calendar = pd.bdate_range("2026-10-12", "2026-10-19")
    frame = pd.DataFrame(
        {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0, "Volume": 200000.0}, index=calendar
    )
    frame.loc[pd.Timestamp("2026-10-19"), "Volume"] = 0
    assert "A" not in state._stage2_scores({"A": frame}, calendar, pd.Timestamp("2026-10-19"))


def test_holiday_fallback_can_be_generated_at_previous_completed_close():
    sessions = pd.bdate_range("2026-10-01", "2026-10-19")
    events = engine.get_anchored_rebalance_dates(
        sessions, "weekly", pd.Timestamp("2026-10-06"), through=pd.Timestamp("2026-10-19")
    )
    assert events[-1] == pd.Timestamp("2026-10-19")


def test_live_results_require_separate_confirmation_and_reject_edited_inputs(live_market, monkeypatch):
    from streamlit.testing.v1 import AppTest

    import app_live_signal as live

    snapshot, params, _, _ = live_market
    params = _next(snapshot, params)
    proposal = state.continue_portfolio(params, snapshot)
    proposal.update(
        run_params=params,
        strategy_fingerprint="test",
        live_signal_version=live._LIVE_SIGNAL_VERSION,
        data_freshness={"source_revisions": snapshot.revisions, "equity_policy_version": state.EQUITY_POLICY_VERSION},
    )
    calls = []

    def run_signal(_params):
        calls.append(1)
        return copy.deepcopy(proposal)

    monkeypatch.setattr(live, "_run_signal", run_signal)
    monkeypatch.setattr(live.shared_market, "source_revisions", lambda: snapshot.revisions)
    app = AppTest.from_string(
        "import streamlit as st\nfrom app_live_signal import live_signal_results\n"
        "live_signal_results(st.session_state['test_params'])"
    )
    app.session_state["ls_run_triggered"] = True
    app.session_state["test_params"] = params
    app.run(timeout=20)
    assert not app.exception
    assert not any("Save confirmed checkpoint" in b.label for b in app.get("download_button"))
    app.checkbox[0].check().run()
    assert not app.exception
    assert any("Save confirmed checkpoint" in b.label for b in app.get("download_button"))
    assert len(calls) == 1
    app.session_state["test_params"] = dict(params, cash_balance=100)
    app.run()
    assert not app.exception
    assert any("Inputs changed" in item.value for item in app.info)
    assert not app.get("download_button")


def test_continuation_is_default_and_missing_checkpoint_disables_generation(monkeypatch):
    from streamlit.testing.v1 import AppTest

    import app_live_signal as live

    monkeypatch.setattr(live, "check_data_freshness", lambda: [])
    monkeypatch.setattr(live.shared_market, "source_revisions", lambda: ("test", "test"))
    app = AppTest.from_string("from app_live_signal import _live_signal_inputs\n_live_signal_inputs(['Nifty 50'])")
    app.run(timeout=20)
    assert not app.exception
    assert app.radio[0].value == "Continue actual portfolio"
    assert next(b for b in app.button if "Generate Signal" in b.label).disabled
    app.selectbox(key="ls_continuation_action").set_value("initialize").run()
    assert not app.exception
    assert any(b.label == "Create starting checkpoint" for b in app.button)


def test_real_shared_snapshot_continues_without_replay_or_vendor_calls(market, monkeypatch):
    import app_live_signal as live

    _, _, days = market
    tuesdays = days[days.weekday == 1]
    initial_date, next_date = tuesdays[-2].date(), tuesdays[-1].date()

    def forbidden(*args, **kwargs):
        raise AssertionError("Actual continuation must not replay a model or fetch vendor data")

    monkeypatch.setattr(engine, "run_backtest", forbidden)
    monkeypatch.setattr("data_backtest.sync_benchmark_data", forbidden)
    params = dict(
        state.STRATEGY_DEFAULTS,
        portfolio_source="continuation",
        continuation_action="initialize",
        rebalance_anchor=initial_date,
        signal_date=initial_date,
        indices=["Nifty 50"],
        m=1,
        n=3,
        max_pos=0,
        broker_snapshot=pd.DataFrame({"Ticker": ["A", "B"], "Quantity": [7, 13]}),
        cash_balance=0,
        reserve_cash=0,
    )
    starting = live._run_signal(params)
    assert starting["continuation_status"] == "initialized", starting
    checkpoint = starting["checkpoint"]
    result = live._run_signal(
        dict(params, continuation_action="continue", signal_date=next_date, continuation_checkpoint=checkpoint)
    )
    assert "error" not in result, result
    assert result["holdings_log"][-1]["holdings"] == ["A", "B"]
    assert all(r["Action"] == "HOLD" for r in result["reconciliation"]["rows"])
    assert checkpoint["decision_date"] == str(initial_date)
