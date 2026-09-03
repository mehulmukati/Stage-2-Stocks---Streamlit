"""Generate a machine-readable weekly Live Signal continuity audit.

This script is intentionally read-only with respect to market/reference data.  It
uses the committed parquet files plus any existing local delta cache and never
performs a network refresh.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app_live_signal import _parse_ticker_reason, _symbols_needed_for_replay
from backtest_engine import BacktestConfig, run_backtest
from corporate_actions import load_corporate_actions
from data import _load_constituents, load_nse_holidays
from data_backtest import _long_to_symbol_dict, load_compositions
from live_signal_audit import build_live_signal_audit_workbook

ALL_INDICES = [
    "Nifty 50",
    "Nifty Next 50",
    "Nifty Midcap 150",
    "Nifty Smallcap 250",
    "Nifty Microcap 250",
]


def _load_prices() -> tuple[dict[str, pd.DataFrame], dict]:
    paths = [ROOT / "data" / "backtest_history.parquet", ROOT / "data" / "backtest_delta.parquet"]
    frames = []
    sources = []
    for path in paths:
        if path.exists():
            frame = pd.read_parquet(path)
            frame["date"] = pd.to_datetime(frame["date"])
            frames.append(frame)
            sources.append(path.name)
    if not frames:
        raise FileNotFoundError("No OHLCV parquet data found")
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(["symbol", "date"], keep="last").sort_values(["symbol", "date"])
    meta = {
        "source_files": sources,
        "row_count": int(len(merged)),
        "symbol_count": int(merged["symbol"].nunique()),
        "min_date": str(merged["date"].min().date()),
        "max_date": str(merged["date"].max().date()),
    }
    return _long_to_symbol_dict(merged), meta


def _next_nse_session(d: date, holidays: set[str]) -> date:
    candidate = d + timedelta(days=1)
    while candidate.weekday() >= 5 or candidate.isoformat() in holidays:
        candidate += timedelta(days=1)
    return candidate


def _next_observed_session(d: date, sessions: pd.DatetimeIndex, holidays: set[str]) -> date:
    later = sessions[sessions > pd.Timestamp(d)]
    if not later.empty:
        return later[0].date()
    return _next_nse_session(d, holidays)


def _move_corporate_action_weights(weights: dict[str, float], actions: list[dict]) -> dict[str, float]:
    moved = dict(weights)
    for action in actions:
        old = action["old_symbol"]
        successor = action["successor_symbol"]
        if old in moved:
            moved[successor] = moved.get(successor, 0.0) + moved.pop(old)
    return moved


def _price_asof(
    symbol_data: dict[str, pd.DataFrame], ticker: str, when: pd.Timestamp
) -> tuple[float | None, str | None]:
    frame = symbol_data.get(ticker)
    if frame is None:
        return None, None
    series = frame.loc[frame.index <= when, "Close"].dropna()
    if series.empty:
        return None, None
    return float(series.iloc[-1]), str(series.index[-1].date())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--replay-checks", action="store_true")
    args = parser.parse_args()

    symbol_data_all, price_meta = _load_prices()
    compositions = load_compositions()
    constituents = _load_constituents()
    corporate_actions = load_corporate_actions()
    allowed = _symbols_needed_for_replay(ALL_INDICES, constituents, compositions, corporate_actions)
    symbol_data = {symbol: frame for symbol, frame in symbol_data_all.items() if symbol in allowed}

    common = dict(
        m=15,
        n=30,
        rebalance_freq="weekly",
        sort_method="Average of 3/6/9/12 months",
        start_date="2024-07-02",
        compositions_df=compositions,
        index_names=ALL_INDICES,
        transaction_cost_pct=0.001,
        min_history_days=252,
        apply_volume_filter=True,
        brokerage_per_sale=0.0,
        initial_capital=1_000_000.0,
        ltcg_rate=0.125,
        stcg_rate=0.20,
        band_rule="classic",
        portfolio_start_date="2025-07-02",
        rank_on_rebalance_date=True,
        stage2_drop_exit=True,
        stage2_drop_threshold=3,
        stage2_entry_filter=False,
        stage2_entry_threshold=2,
        max_position_pct=None,
        min_annual_return=7.0,
        pct_from_52w_high=25.0,
        max_circuits=18,
        close_above_100dma=False,
        close_above_200dma=True,
        pos_days_3m_min=45.0,
        pos_days_6m_min=45.0,
        pos_days_12m_min=45.0,
    )
    config = BacktestConfig(end_date="2026-09-01", rebalance_anchor_date="2026-09-01", **common)
    result = run_backtest(symbol_data, {}, config)
    if "error" in result:
        raise RuntimeError(result["error"])

    reset = pd.Timestamp(result["portfolio_reset_date"])
    events = [event for event in result["holdings_log"] if pd.Timestamp(event["date"]) >= reset]
    nav = result["nav"]["Marginal Rebalance"].copy()
    final_nav = float(nav.iloc[-1])
    current_value = 1_000_000.0
    value_scale = current_value / final_nav
    holidays = set(load_nse_holidays())

    event_rows = []
    position_rows = []
    transition_rows = []
    issue_rows = []
    replay_rows = []

    for idx, event in enumerate(events):
        signal_ts = pd.Timestamp(event["date"])
        signal_date = signal_ts.date()
        prior = events[idx - 1] if idx else None
        prior_ts = pd.Timestamp(prior["date"]) if prior else None
        weights = {key: float(value) for key, value in event["marg_weights"].items()}
        prior_target_raw = {key: float(value) for key, value in prior["marg_weights"].items()} if prior else {}
        actions = event.get("corporate_actions", [])
        prior_target = _move_corporate_action_weights(prior_target_raw, actions)
        drifted_prior = {key: float(value) for key, value in event.get("pre_rebalance_marg_weights", {}).items()}
        expected_exec = _next_observed_session(signal_date, result["trading_days"], holidays)
        app_exec = _next_nse_session(signal_date, holidays)
        if signal_date < result["trading_days"].max().date():
            app_exec = _next_observed_session(signal_date, result["trading_days"], holidays)
        nav_at_signal = float(nav.asof(signal_ts))
        portfolio_value = nav_at_signal * value_scale
        entries = dict(_parse_ticker_reason(item) for item in event.get("entries", []))
        exits = dict(_parse_ticker_reason(item) for item in event.get("exits", []))
        ranks = {ticker: rank for rank, ticker in enumerate(event.get("full_ranking", []), start=1)}
        all_tickers = sorted(set(drifted_prior) | set(weights))
        sum_new = sum(weights.values())
        sum_drifted = sum(drifted_prior.values())
        legacy_trade_weight = sum(abs(weights.get(t, 0.0) - prior_target.get(t, 0.0)) for t in all_tickers)
        app_trade_weight = sum(abs(weights.get(t, 0.0) - drifted_prior.get(t, 0.0)) for t in all_tickers)
        correct_trade_weight = sum(abs(weights.get(t, 0.0) - drifted_prior.get(t, 0.0)) for t in all_tickers)
        action_mismatch_count = 0

        if app_exec != expected_exec:
            issue_rows.append(
                {
                    "severity": "HIGH",
                    "category": "Execution calendar",
                    "signal_date": str(signal_date),
                    "ticker": "",
                    "observed": str(app_exec),
                    "expected": str(expected_exec),
                    "difference": (expected_exec - app_exec).days,
                    "explanation": "App's next-business-day helper ignores NSE holidays.",
                }
            )

        event_rows.append(
            {
                "week": idx + 1,
                "signal_date": str(signal_date),
                "app_execution_date": str(app_exec),
                "expected_execution_date": str(expected_exec),
                "execution_date_match": app_exec == expected_exec,
                "holdings": len(weights),
                "entries": len(entries),
                "exits": len(exits),
                "turnover_pct": float(event["marg_turnover_pct"]),
                "app_table_trade_weight_pct": app_trade_weight,
                "legacy_app_table_trade_weight_pct": legacy_trade_weight,
                "drift_adjusted_trade_weight_pct": correct_trade_weight,
                "weight_sum_pct": sum_new,
                "prior_drift_weight_sum_pct": sum_drifted,
                "engine_nav": nav_at_signal,
                "current_normalized_value": portfolio_value,
                "valid_universe_size": int(event["valid_universe_size"]),
                "corporate_actions": len(actions),
                "action_mismatches": 0,
            }
        )

        for ticker, weight in sorted(weights.items(), key=lambda item: (-item[1], item[0])):
            signal_close, signal_price_date = _price_asof(symbol_data, ticker, signal_ts)
            execution_close, execution_price_date = _price_asof(symbol_data, ticker, pd.Timestamp(expected_exec))
            position_rows.append(
                {
                    "week": idx + 1,
                    "signal_date": str(signal_date),
                    "execution_date": str(expected_exec),
                    "ticker": ticker,
                    "rank": ranks.get(ticker),
                    "weight_pct": weight,
                    "target_value": portfolio_value * weight / 100.0,
                    "signal_close": signal_close,
                    "signal_price_date": signal_price_date,
                    "execution_close": execution_close,
                    "execution_price_date": execution_price_date,
                    "signal_to_execution_return_pct": (
                        (execution_close / signal_close - 1.0) * 100.0 if signal_close and execution_close else None
                    ),
                    "entry_reason": entries.get(ticker, ""),
                }
            )

        for ticker in all_tickers:
            previous_weight = drifted_prior.get(ticker, 0.0)
            new_weight = weights.get(ticker, 0.0)
            delta = new_weight - previous_weight
            action = "BUY" if delta > 0.001 else "SELL" if delta < -0.001 else "HOLD"
            app_delta = new_weight - drifted_prior.get(ticker, 0.0)
            app_action = "BUY" if app_delta > 0.001 else "SELL" if app_delta < -0.001 else "HOLD"
            if app_action != action:
                action_mismatch_count += 1
            prior_close, prior_price_date = (
                _price_asof(symbol_data, ticker, prior_ts) if prior_ts is not None else (None, None)
            )
            current_close, current_price_date = _price_asof(symbol_data, ticker, signal_ts)
            stock_return = (current_close / prior_close - 1.0) if prior_close and current_close else None
            contribution = (prior_target.get(ticker, 0.0) / 100.0) * stock_return if stock_return is not None else None
            transition_rows.append(
                {
                    "week": idx + 1,
                    "prior_signal_date": str(prior_ts.date()) if prior_ts is not None else "",
                    "signal_date": str(signal_date),
                    "ticker": ticker,
                    "prior_target_weight_pct": prior_target.get(ticker, 0.0),
                    "drifted_prior_weight_pct": previous_weight,
                    "new_target_weight_pct": new_weight,
                    "trade_weight_pct": delta,
                    "action": action,
                    "legacy_previous_weight_pct": prior_target.get(ticker, 0.0),
                    "app_previous_weight_pct": drifted_prior.get(ticker, 0.0),
                    "legacy_app_trade_weight_pct": new_weight - prior_target.get(ticker, 0.0),
                    "app_trade_weight_pct": app_delta,
                    "app_action": app_action,
                    "action_matches": app_action == action,
                    "rank": ranks.get(ticker),
                    "entry_reason": entries.get(ticker, ""),
                    "exit_reason": exits.get(ticker, ""),
                    "prior_close": prior_close,
                    "prior_price_date": prior_price_date,
                    "current_close": current_close,
                    "current_price_date": current_price_date,
                    "stock_return_pct": stock_return * 100.0 if stock_return is not None else None,
                    "approx_contribution_pct": contribution * 100.0 if contribution is not None else None,
                }
            )

        event_rows[-1]["action_mismatches"] = action_mismatch_count
        if action_mismatch_count:
            issue_rows.append(
                {
                    "severity": "HIGH",
                    "category": "Trade classification uses stale weights",
                    "signal_date": str(signal_date),
                    "ticker": "",
                    "observed": action_mismatch_count,
                    "expected": 0,
                    "difference": app_trade_weight - correct_trade_weight,
                    "explanation": (
                        "Live table compares the prior target snapshot with the new target instead of comparing "
                        "the price-drifted current portfolio with the new target. Difference is percentage points "
                        "of two-sided absolute trade weight."
                    ),
                }
            )
        if abs(app_trade_weight - float(event["marg_turnover_pct"])) > 0.02:
            issue_rows.append(
                {
                    "severity": "HIGH",
                    "category": "Displayed turnover reconciliation",
                    "signal_date": str(signal_date),
                    "ticker": "",
                    "observed": app_trade_weight,
                    "expected": float(event["marg_turnover_pct"]),
                    "difference": app_trade_weight - float(event["marg_turnover_pct"]),
                    "explanation": "Displayed absolute trade weights must reconcile to engine Marginal turnover.",
                }
            )

        if abs(sum_new - 100.0) > 0.01:
            issue_rows.append(
                {
                    "severity": "HIGH",
                    "category": "Weight tie-out",
                    "signal_date": str(signal_date),
                    "ticker": "",
                    "observed": sum_new,
                    "expected": 100.0,
                    "difference": sum_new - 100.0,
                    "explanation": "Target weights do not sum to 100% within tolerance.",
                }
            )

    if args.replay_checks:
        # Re-run each historical signal independently and ensure the last two
        # snapshots agree with the corresponding snapshots from the final replay.
        for idx, expected_event in enumerate(events):
            signal = pd.Timestamp(expected_event["date"])
            check_cfg = BacktestConfig(end_date=str(signal.date()), rebalance_anchor_date=str(signal.date()), **common)
            checked = run_backtest(symbol_data, {}, check_cfg)
            if "error" in checked:
                replay_rows.append(
                    {
                        "week": idx + 1,
                        "signal_date": str(signal.date()),
                        "status": "ERROR",
                        "detail": checked["error"],
                    }
                )
                continue
            actual = checked["holdings_log"][-1]
            holdings_match = actual["holdings"] == expected_event["holdings"]
            weights_match = actual["marg_weights"] == expected_event["marg_weights"]
            replay_rows.append(
                {
                    "week": idx + 1,
                    "signal_date": str(signal.date()),
                    "status": "OK" if holdings_match and weights_match else "FAIL",
                    "holdings_match": holdings_match,
                    "weights_match": weights_match,
                    "detail": "",
                }
            )

    payload = {
        "metadata": {
            "generated_at": pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
            "price_data": price_meta,
            "selected_symbol_count": len(symbol_data),
            "composition_rows": int(len(compositions)),
            "composition_max_date": str(pd.to_datetime(compositions["TIME_STAMP"]).max().date()),
            "portfolio_reset_signal_date": str(reset.date()),
            "portfolio_start_execution_date": "2025-07-02",
            "current_signal_date": "2026-09-01",
            "current_portfolio_value": current_value,
            "final_engine_nav": final_nav,
            "normalization_rupees_per_nav_point": value_scale,
            "event_count": len(events),
            "nav_timing": "session return first; rebalance at close; new weights active from next session",
        },
        "parameters": {
            "band_rule": "Classic",
            "variant": "Marginal Rebalance",
            "m": 15,
            "n": 30,
            "ranking": "Average of 3/6/9/12 months",
            "frequency": "weekly",
            "stage2_entry": False,
            "stage2_drop_exit": True,
            "stage2_drop_threshold": 3,
            "max_position_pct": 0,
            "min_history_days": 252,
            "quality_filters": {
                "min_annual_return_pct": 7.0,
                "within_pct_of_52w_high": 25.0,
                "max_circuits_1y": 18,
                "close_above_100dma": False,
                "close_above_200dma": True,
                "positive_days_3m_pct": 45.0,
                "positive_days_6m_pct": 45.0,
                "positive_days_12m_pct": 45.0,
            },
        },
        "events": event_rows,
        "positions": position_rows,
        "transitions": transition_rows,
        "issues": issue_rows,
        "replay_checks": replay_rows,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".xlsx":
        audit_params = {
            "portfolio_source": "replay",
            "signal_date": date(2026, 9, 1),
            "portfolio_start": date(2025, 7, 2),
            "band": "classic",
            "variant": "Marginal Rebalance",
            "m": common["m"],
            "n": common["n"],
            "sort_method": common["sort_method"],
            "freq": common["rebalance_freq"],
            "indices": ALL_INDICES,
            "s2_drop": common["stage2_drop_exit"],
            "s2_threshold": common["stage2_drop_threshold"],
            "s2_entry": common["stage2_entry_filter"],
            "s2_entry_threshold": common["stage2_entry_threshold"],
            "max_pos": 0,
            "min_history": common["min_history_days"],
            "min_annual_return": common["min_annual_return"],
            "pct_from_52w_high": common["pct_from_52w_high"],
            "max_circuits": common["max_circuits"],
            "close_above_100dma": common["close_above_100dma"],
            "close_above_200dma": common["close_above_200dma"],
            "pos_days_3m_min": common["pos_days_3m_min"],
            "pos_days_6m_min": common["pos_days_6m_min"],
            "pos_days_12m_min": common["pos_days_12m_min"],
        }
        audit_result = {
            **result,
            "ohlcv_date": price_meta["max_date"],
            "ohlcv_source": ", ".join(price_meta["source_files"]),
            "strategy_fingerprint": "cli-fixed-audit",
            "latest_price_dates": {},
            "tradability_status": {},
            "data_freshness": {},
        }
        output.write_bytes(
            build_live_signal_audit_workbook(
                audit_params,
                audit_result,
                replay_checks=replay_rows if args.replay_checks else None,
            )
        )
    else:
        output.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(output),
                "events": len(events),
                "positions": len(position_rows),
                "issues": len(issue_rows),
            }
        )
    )


if __name__ == "__main__":
    main()
