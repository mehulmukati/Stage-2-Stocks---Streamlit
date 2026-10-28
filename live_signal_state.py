"""One-step live decisions from actual positions and immutable decision checkpoints.

Checkpoint files belong to the user, not a shared server/session. Generating a
proposal never advances a checkpoint; only a separate execution confirmation does.
"""

from __future__ import annotations

import hashlib
import json
import math

import pandas as pd

import backtest_engine as engine
from equity_data import EQUITY_POLICY_VERSION, session_frame
from stage2_engine import compute_rolling_stage2

STATE_VERSION = 1
DECISION_POLICY_VERSION = 1
STRATEGY_DEFAULTS = {
    "band": "classic",
    "variant": "Marginal Rebalance",
    "m": 15,
    "n": 30,
    "sort_method": "Average of 3/6/9/12 months",
    "freq": "weekly",
    "s2_drop": True,
    "s2_threshold": 3,
    "s2_entry": False,
    "s2_entry_threshold": 2,
    "max_pos": 0,
    "min_history": 252,
    "minimum_median_volume": 100000,
    "max_stale_sessions": 3,
    "min_annual_return": 0.0,
    "pct_from_52w_high": 100.0,
    "max_circuits": 999,
    "close_above_100dma": False,
    "close_above_200dma": False,
    "pos_days_3m_min": 0.0,
    "pos_days_6m_min": 0.0,
    "pos_days_12m_min": 0.0,
}


def strategy_settings(params):
    settings = {k: params.get(k, default) for k, default in STRATEGY_DEFAULTS.items()}
    for key, default in STRATEGY_DEFAULTS.items():
        value = settings[key]
        if isinstance(default, bool):
            valid = isinstance(value, bool)
        elif isinstance(default, (int, float)):
            valid = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
            if isinstance(default, int):
                valid = valid and int(value) == value
        else:
            valid = isinstance(value, str)
        if not valid:
            raise ValueError(f"Invalid strategy setting: {key}.")
    if settings["freq"] not in {"weekly", "biweekly", "monthly", "quarterly", "half-yearly"}:
        raise ValueError("Unsupported live frequency.")
    if not 0 <= settings["max_pos"] <= 100 or settings["max_stale_sessions"] < 0:
        raise ValueError("Invalid position cap or stale-session allowance.")
    if not isinstance(params.get("indices", []), list) or any(
        not isinstance(s, str) for s in params.get("indices", [])
    ):
        raise ValueError("Invalid index universe.")
    settings["indices"] = sorted(params.get("indices", []))
    settings["rebalance_anchor"] = str(pd.Timestamp(params["rebalance_anchor"]).date())
    return settings


def _digest(payload):
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def seal_checkpoint(payload):
    state = {k: v for k, v in payload.items() if k != "checkpoint_id"}
    return dict(state, checkpoint_id=_digest(state))


def read_checkpoint(payload):
    try:
        state = json.loads(payload) if isinstance(payload, (str, bytes)) else dict(payload)
        if not isinstance(state, dict):
            raise ValueError("Checkpoint must be a JSON object.")
        if state.get("checkpoint_id") != _digest({k: v for k, v in state.items() if k != "checkpoint_id"}):
            raise ValueError("Checkpoint contents changed or are incomplete.")
        if state.get("version") != STATE_VERSION or state.get("status") != "confirmed":
            raise ValueError("Upload a confirmed checkpoint, not an unexecuted proposal.")
        if not isinstance(state["decision_date"], str) or pd.isna(pd.Timestamp(state["decision_date"])):
            raise ValueError("Invalid checkpoint decision date.")
        if not isinstance(state["strategy"], dict) or not isinstance(state["stage2_scores"], dict):
            raise ValueError("Invalid checkpoint strategy or Stage 2 baseline.")
        if strategy_settings(state["strategy"]) != state["strategy"]:
            raise ValueError("Incomplete checkpoint strategy settings.")
        for score in state["stage2_scores"].values():
            if (
                isinstance(score, bool)
                or not isinstance(score, (int, float))
                or not math.isfinite(score)
                or not 0 <= score <= 8
            ):
                raise ValueError("Invalid Stage 2 baseline score.")
        return state
    except (KeyError, TypeError, json.JSONDecodeError, OverflowError) as exc:
        raise ValueError("Invalid continuation checkpoint file.") from exc


def confirm_checkpoint(proposal):
    if proposal.get("status") != "proposed":
        raise ValueError("Only a proposed decision can be confirmed after execution.")
    if proposal.get("checkpoint_id") != _digest({k: v for k, v in proposal.items() if k != "checkpoint_id"}):
        raise ValueError("Proposal contents changed. Regenerate the decision before confirming.")
    return seal_checkpoint(dict(proposal, status="confirmed"))


def _stage2_scores(data, calendar, as_of):
    scores = {}
    for symbol, frame in data.items():
        aligned = session_frame(frame, calendar, as_of)
        if aligned.empty or as_of not in aligned.index or pd.isna(aligned.at[as_of, "Close"]):
            continue
        score = compute_rolling_stage2(aligned).at[as_of, "Score"]
        if pd.notna(score):
            scores[symbol] = float(score)
    return scores


def _checkpoint(params, decision_date, positions, scores, revisions, *, status, parent=None):
    return seal_checkpoint(
        {
            "version": STATE_VERSION,
            "decision_policy_version": DECISION_POLICY_VERSION,
            "equity_policy_version": EQUITY_POLICY_VERSION,
            "status": status,
            "decision_date": str(decision_date.date()),
            "strategy": strategy_settings(params),
            "stage2_scores": scores,
            "positions": {str(s): int(q) for s, q in positions.items()},
            "source_revisions": list(revisions),
            "parent_checkpoint_id": parent,
        }
    )


def continue_portfolio(params, snapshot):
    """Return an initialization checkpoint or at most one pending decision.

    No warm-up portfolio is replayed. A checkpoint is only a decision/score
    baseline; quantities always come from the newly supplied broker positions.
    """
    try:
        return _continue_portfolio(params, snapshot)
    except (ValueError, KeyError, TypeError) as exc:
        return {"error": str(exc)}


def _continue_portfolio(params, snapshot):
    # Keep parsing and price validation shared with the app's executable orders.
    from app_live_signal import _normalise_broker_snapshot, _reconcile_actual_portfolio

    settings = strategy_settings(params)
    if not 1 <= settings["m"] < settings["n"]:
        raise ValueError("Entry M must be positive and less than exit N.")
    if settings["band"] not in {"classic", "displacement"}:
        raise ValueError("Unsupported band rule.")
    if settings["variant"] not in {"Full Rebalance", "Marginal Rebalance", "Prop Rebalance"}:
        raise ValueError("Unsupported rebalance variant.")
    positions, errors = _normalise_broker_snapshot(params.get("broker_snapshot", pd.DataFrame()))
    if errors:
        raise ValueError(" ".join(errors))
    quantities = dict(zip(positions.Ticker, positions.Quantity))
    as_of = pd.Timestamp(params["signal_date"]).normalize()
    calendar = snapshot.trading_calendar()
    sessions = calendar[calendar <= as_of]
    if sessions.empty:
        raise ValueError("No exchange session is available at this date.")
    valuation_date = sessions[-1]
    initializing = params.get("continuation_action") == "initialize"
    state = None
    if not initializing:
        if not params.get("continuation_checkpoint"):
            raise ValueError("Upload your last confirmed checkpoint, or initialize a starting checkpoint first.")
        state = read_checkpoint(params["continuation_checkpoint"])
        if (
            state.get("decision_policy_version") != DECISION_POLICY_VERSION
            or state.get("equity_policy_version") != EQUITY_POLICY_VERSION
        ):
            raise ValueError(
                "Calculation rules changed. Explicitly initialize a new checkpoint from actual holdings; "
                "past decisions will not be replayed."
            )
        if state["strategy"] != settings:
            raise ValueError(
                "Strategy or schedule differs from the checkpoint. "
                "Restore its settings or explicitly initialize a new checkpoint."
            )
        previous = pd.Timestamp(state["decision_date"])
        if as_of < previous:
            raise ValueError("Signal date precedes the confirmed checkpoint.")
        due = engine.get_anchored_rebalance_dates(
            calendar, settings["freq"], pd.Timestamp(settings["rebalance_anchor"]), through=as_of
        )
        pending = [d for d in due if d > previous]
        if not pending:
            return {
                "continuation_status": "not_due",
                "message": "No new scheduled decision is due after this checkpoint. No orders generated.",
            }
        if len(pending) > 1:
            raise ValueError(
                "Multiple scheduled decisions were missed. Do not replay hypothetical trades against current holdings. "
                "Explicitly initialize a new checkpoint from the portfolio you own."
            )
        decision_date = pending[0]
        if decision_date != valuation_date:
            raise ValueError(
                f"The pending decision is dated {decision_date.date()}. "
                "Select that signal date, or explicitly initialize a new checkpoint. "
                "Later prices cannot silently move the schedule."
            )
    else:
        decision_date = valuation_date

    # Filter price loading to this universe plus actual holdings. Initialization
    # records scores for all candidates, so subsequent external purchases can
    # use a genuine saved baseline when that security was already observable.
    valid = snapshot.symbols(settings["indices"] or None, as_of=decision_date)
    data = snapshot.ohlcv(symbols=valid | set(quantities), as_of=decision_date)
    calendar = calendar[calendar <= decision_date]
    latest_dates, prices, tradability = {}, {}, {}
    observed = []
    for symbol, frame in data.items():
        last = engine.latest_tradable_date(frame, decision_date)
        if last is None:
            continue
        observed.append(last)
        # Match the order's price to the same positive-volume bar whose
        # tradability/date we report. A later unverified mark is not executable.
        prices[symbol] = float(frame.at[last, "Close"])
        latest_dates[symbol] = str(last.date())
        age = engine.trading_session_age(last, decision_date, calendar)
        tradability[symbol] = "TRADABLE" if age <= settings["max_stale_sessions"] else "STALE"
    if not observed or max(observed) < decision_date:
        raise ValueError(f"No market-wide tradable prices for {decision_date.date()}. Refresh shared data first.")
    blocking = [s for s in quantities if tradability.get(s) != "TRADABLE"]
    if blocking:
        raise ValueError("Actual holdings require current tradable prices: " + ", ".join(sorted(blocking)))
    if state:
        for action in engine.load_corporate_actions():
            if previous < action["effective_date"] <= decision_date and action["old_symbol"] in quantities:
                raise ValueError(
                    f"Update the broker snapshot for corporate action "
                    f"{action['old_symbol']} → {action['successor_symbol']}; "
                    "continuation uses actual successor quantities."
                )

    # Validate cash and valuation before producing or advancing any checkpoint.
    valuation = _reconcile_actual_portfolio(
        positions, params.get("cash_balance", 0), params.get("reserve_cash", 0), {}, prices
    )
    if valuation["errors"]:
        raise ValueError(" ".join(valuation["errors"]))
    scores = _stage2_scores(data, calendar, decision_date)
    if initializing:
        checkpoint = _checkpoint(params, decision_date, quantities, scores, snapshot.revisions, status="confirmed")
        return {
            "continuation_status": "initialized",
            "checkpoint": checkpoint,
            "message": f"Starting checkpoint created at {decision_date.date()}. "
            "Existing holdings are preserved; no orders generated.",
        }

    ranked, excluded = engine.rank_universe_at_date(
        data,
        decision_date,
        settings["sort_method"],
        valid_symbols=valid,
        min_history_days=settings["min_history"],
        minimum_median_volume=settings["minimum_median_volume"],
        max_stale_sessions=settings["max_stale_sessions"],
        trading_calendar=calendar,
        return_excluded_reasons=True,
        **{
            k: settings[k]
            for k in (
                "min_annual_return",
                "pct_from_52w_high",
                "max_circuits",
                "close_above_100dma",
                "close_above_200dma",
                "pos_days_3m_min",
                "pos_days_6m_min",
                "pos_days_12m_min",
            )
        },
    )
    held = set(quantities)
    top_n, top_m = set(ranked[: settings["n"]]), set(ranked[: settings["m"]])
    if settings["s2_drop"] and settings["freq"] == "weekly":
        missing = (held & top_n) - set(state["stage2_scores"])
        missing |= (held & top_n) - set(scores)
        if missing:
            raise ValueError(
                "Missing saved/current Stage 2 baseline for: "
                + ", ".join(sorted(missing))
                + ". Initialize a new checkpoint explicitly."
            )
    select = (
        engine._compute_holdings_displacement
        if settings["band"] == "displacement"
        else engine._compute_holdings_classic
    )
    kwargs = dict(
        current_holdings=held,
        top_m=top_m,
        top_n=top_n,
        ranked=ranked,
        stage2_precomputed={s: pd.Series([score], index=[decision_date]) for s, score in scores.items()},
        rank_as_of=decision_date,
        prev_stage2_scores=state["stage2_scores"],
        stage2_entry_filter=settings["s2_entry"],
        stage2_drop_exit=settings["s2_drop"],
        stage2_entry_threshold=settings["s2_entry_threshold"],
        stage2_drop_threshold=settings["s2_threshold"],
        rebalance_freq=settings["freq"],
    )
    if settings["band"] == "displacement":
        kwargs["m"] = settings["m"]
    entries, exits, entry_reasons, exit_reasons = select(**kwargs)
    for symbol in exits:
        if symbol in valid and symbol not in ranked:
            exit_reasons[symbol] = "excluded: " + excluded.get(symbol, "unrankable")
    new_holdings = (held - exits) | entries
    securities = valuation["securities_value"]
    actual_weights = {s: quantities[s] * prices[s] / securities for s in held} if securities else {}
    full, marginal, prop = engine._compute_weight_variants(
        new_holdings, entries, exits, actual_weights, actual_weights, len(new_holdings)
    )
    variants = {"full_weights": full, "marg_weights": marginal, "prop_weights": prop}
    event = {
        "date": decision_date,
        "rank_as_of": decision_date,
        "holdings": sorted(new_holdings),
        "entries": [f"{s} ({entry_reasons[s]})" for s in sorted(entries)],
        "exits": [f"{s} ({exit_reasons[s]})" for s in sorted(exits)],
        "full_ranking": ranked,
        "index_universe": sorted(valid),
        "valid_universe_size": len(valid),
        "excluded_reasons": excluded,
        "corporate_actions": [],
    }
    for key, weights in variants.items():
        event[f"pre_rebalance_{key}"] = {s: w * 100 for s, w in actual_weights.items()}
        event[f"pre_cap_{key}"] = {s: w * 100 for s, w in weights.items()}
        cap = settings["max_pos"] / 100
        if cap and weights and cap * len(weights) < 1 - 1e-9:
            raise ValueError(
                "Position cap cannot be met with the selected holdings. "
                "Adjust the strategy explicitly; no orders generated."
            )
        capped = engine._apply_weight_cap(weights, cap)
        event[key] = {s: w * 100 for s, w in capped.items()}
        event[key.replace("weights", "turnover_pct")] = (
            sum(abs(capped.get(s, 0) - actual_weights.get(s, 0)) for s in held | new_holdings) * 100
        )
    key = (
        "prop_weights"
        if "Prop" in settings["variant"]
        else "marg_weights" if "Marginal" in settings["variant"] else "full_weights"
    )
    reconciliation = _reconcile_actual_portfolio(
        positions, params.get("cash_balance", 0), params.get("reserve_cash", 0), event[key], prices
    )
    if reconciliation["errors"]:
        raise ValueError(" ".join(reconciliation["errors"]))
    traded = [r["Ticker"] for r in reconciliation["rows"] if r["Action"] != "HOLD"]
    if any(tradability.get(s) != "TRADABLE" for s in traded):
        raise ValueError("A proposed trade lacks a current tradable price. No orders generated.")
    next_quantities = {r["Ticker"]: r["Target quantity"] for r in reconciliation["rows"] if r["Target quantity"] > 0}
    proposal = _checkpoint(
        params,
        decision_date,
        next_quantities,
        scores,
        snapshot.revisions,
        status="proposed",
        parent=state["checkpoint_id"],
    )
    return {
        "continuation_status": "proposed",
        "holdings_log": [event],
        "proposal_checkpoint": proposal,
        "parent_checkpoint": state["checkpoint_id"],
        "portfolio_reset_date": None,
        "close_prices": prices,
        "latest_price_dates": latest_dates,
        "tradability_status": tradability,
        "blocking_stale_incumbents": [],
        "ohlcv_date": str(decision_date.date()),
        "ohlcv_source": "shared market snapshot",
        "decision_date": str(decision_date.date()),
        "reconciliation": reconciliation,
    }
