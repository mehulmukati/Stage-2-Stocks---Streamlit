"""
Momentum portfolio backtester.

Entry rule : stock enters portfolio if it ranks in top-M
Exit rule  : stock leaves portfolio if it falls out of top-N  (N > M)
Rebalance  : weekly | biweekly | monthly | quarterly | half-yearly

Two portfolio variants are tracked simultaneously:
  - Full rebalance   : every rebalance date all holdings reset to equal weight (1/size)
  - Marginal rebalance: only in/out stocks are adjusted; incumbents keep price-drifted weights

Survivorship-bias mitigations applied:
  - Historical constituent filter via compositions.parquet (only stocks in-index at each date)
  - Minimum history required before a stock can be ranked (configurable; default 252 trading days)
  - Stocks with > 5% missing close prices excluded (suspended / bad data)
  - Volume filter: median volume must meet MIN_VOLUME threshold
  - Transaction costs deducted at each rebalance
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from config import MIN_VOLUME
from momentum_engine import _calculate_avg_sharpe, precompute_metrics, score_momentum
from stage2_engine import compute_rolling_stage2

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────


@dataclass
class BacktestConfig:
    """All strategy and cost parameters for run_backtest (excludes raw data inputs)."""

    # Required strategy params
    m: int
    n: int
    rebalance_freq: str
    sort_method: str
    start_date: str
    end_date: str
    # Optional data / filtering
    compositions_df: pd.DataFrame | None = field(default=None, compare=False, repr=False)
    index_names: list[str] | None = None
    # Universe filtering
    min_history_days: int = 750
    apply_volume_filter: bool = True
    max_position_pct: float | None = None
    # Portfolio mechanics
    band_rule: str = "classic"
    # Cost model
    transaction_cost_pct: float = 0.001
    brokerage_per_sale: float = 0.0
    initial_capital: float = 1_000_000.0
    # Tax
    ltcg_rate: float = 0.0
    stcg_rate: float = 0.0
    # Stage 2 drop/entry filters
    stage2_drop_exit: bool = False
    stage2_drop_threshold: int = 2
    stage2_entry_filter: bool = False
    stage2_entry_threshold: int = 2


# ──────────────────────────────────────────────────────────────
# UTILITY
# ──────────────────────────────────────────────────────────────


def _close_price(all_ohlcv: dict, sym: str, date: pd.Timestamp) -> float | None:
    try:
        c = all_ohlcv[sym]["Close"]
        return float(c.at[date]) if date in c.index else None
    except (KeyError, TypeError):
        return None


def _schema_error(symbol: str, missing: set[str]) -> str:
    return f"{symbol}: missing required OHLCV columns {sorted(missing)}"


def _validate_ohlcv_schema(all_ohlcv: dict[str, pd.DataFrame]) -> list[str]:
    """Return schema validation errors for backtest OHLCV input."""
    required = {"Close", "High", "Volume"}
    errors: list[str] = []
    for sym, df in all_ohlcv.items():
        missing = required - set(df.columns)
        if missing:
            errors.append(_schema_error(sym, missing))
        if not isinstance(df.index, pd.DatetimeIndex):
            errors.append(f"{sym}: index must be a DatetimeIndex")
    return errors


def _financial_year(date: pd.Timestamp) -> int:
    """India FY Apr–Mar. Returns start year: Apr 2021–Mar 2022 → 2021."""
    return date.year if date.month >= 4 else date.year - 1


def _canonical_index_name(name: str) -> str:
    """Normalise display/canonical NSE index labels for composition matching."""
    return re.sub(r"[^A-Z0-9]", "", str(name).upper())


def _compute_fy_tax(
    fy: int,
    st_gains: float,
    st_losses: float,
    lt_gains: float,
    lt_losses: float,
    cf_st: list,
    cf_lt: list,
    stcg_rate: float,
    ltcg_rate: float,
) -> tuple[float, list, list]:
    """
    Compute India CGT for one financial year with carry-forward loss offset.

    cf_st / cf_lt : list of (expiry_fy, amount) — ST / LT carry-forward loss buckets.
    Returns       : (tax_amount_in_nav_units, updated_cf_st, updated_cf_lt).

    Loss offset rules:
      - Current-year ST losses  → offset ST gains first, then LT gains.
      - Current-year LT losses  → offset LT gains only.
      - Carry-forward ST losses → oldest first, offset remaining ST then LT gains.
      - Carry-forward LT losses → oldest first, offset remaining LT gains only.
      - Unabsorbed losses carried forward for 8 years (usable up to fy+8 inclusive).
    """
    # Expire buckets whose usability window has passed (expiry_fy < fy means already past)
    cf_st = [(e, a) for e, a in cf_st if e >= fy]
    cf_lt = [(e, a) for e, a in cf_lt if e >= fy]

    # ── Step 1: net within current year ──
    net_st = st_gains - st_losses
    net_lt = lt_gains - lt_losses

    # ── Step 2: excess current-year ST loss → offset LT gains ──
    carry_st_new = 0.0
    if net_st < 0:
        st_excess = -net_st
        net_st = 0.0
        absorbed = min(st_excess, max(net_lt, 0.0))
        net_lt -= absorbed
        carry_st_new = st_excess - absorbed  # whatever couldn't be absorbed → CF

    carry_lt_new = 0.0
    if net_lt < 0:
        carry_lt_new = -net_lt
        net_lt = 0.0

    # ── Step 3: apply carry-forward ST losses (oldest first) ──
    new_cf_st = []
    for exp, amt in sorted(cf_st):
        if net_st > 0 and amt > 0:
            used = min(amt, net_st)
            net_st -= used
            amt -= used
        if net_lt > 0 and amt > 0:
            used = min(amt, net_lt)
            net_lt -= used
            amt -= used
        if amt > 0:
            new_cf_st.append((exp, amt))

    # ── Step 4: apply carry-forward LT losses (oldest first) ──
    new_cf_lt = []
    for exp, amt in sorted(cf_lt):
        if net_lt > 0 and amt > 0:
            used = min(amt, net_lt)
            net_lt -= used
            amt -= used
        if amt > 0:
            new_cf_lt.append((exp, amt))

    # ── Step 5: add current year's new carry-forward entries ──
    if carry_st_new > 0:
        new_cf_st.append((fy + 8, carry_st_new))
    if carry_lt_new > 0:
        new_cf_lt.append((fy + 8, carry_lt_new))

    tax = net_st * stcg_rate + net_lt * ltcg_rate
    return tax, new_cf_st, new_cf_lt


# ──────────────────────────────────────────────────────────────
# HISTORICAL CONSTITUENT LOOKUP
# ──────────────────────────────────────────────────────────────


def _prepare_compositions(
    comp_df: pd.DataFrame,
    index_names: list[str],
) -> pd.DataFrame | None:
    """
    Precompute the filtered/keyed compositions DataFrame for the requested indices.
    Returns None only when the universe filter should be fully disabled (no data or
    no index names). Returns a (possibly empty) DataFrame otherwise so the per-rebalance
    call can apply the filter and correctly return set() when no symbols match.
    Call once before the rebalance loop; pass the result to _valid_symbols_at_date.
    """
    if comp_df is None or comp_df.empty or not index_names:
        return None

    requested_keys = {_canonical_index_name(name) for name in index_names}
    comp = comp_df.copy()
    comp["_INDEX_KEY"] = comp["INDEX_NAME"].map(_canonical_index_name)
    filtered = comp[comp["_INDEX_KEY"].isin(requested_keys)]

    if filtered.empty:
        logging.warning("None of the requested indices %r were found in compositions_df", index_names)

    return filtered


def _valid_symbols_at_date(
    comp_prepared: pd.DataFrame | None,
    index_names: list[str],
    as_of: pd.Timestamp,
) -> set[str] | None:
    """
    Return the set of symbols that were members of the given indices on or
    before `as_of`, based on the most recent composition snapshot per index.
    Returns None when comp_prepared is None (filter disabled).
    comp_prepared must be the result of _prepare_compositions (precomputed once).
    """
    if comp_prepared is None:
        return None

    eligible = comp_prepared[comp_prepared["TIME_STAMP"] <= as_of]
    if eligible.empty:
        logging.warning(
            "No composition snapshot exists on or before %s for requested indices %r; using empty universe",
            as_of,
            index_names,
        )
        return set()

    valid: set[str] = set()
    for idx_name in index_names:
        idx_key = _canonical_index_name(idx_name)
        idx_rows = eligible[eligible["_INDEX_KEY"] == idx_key]
        if idx_rows.empty:
            logging.debug(
                "Index %r has no composition snapshot on or before %s — skipping for this rebalance", idx_name, as_of
            )
            continue
        latest_ts = idx_rows["TIME_STAMP"].max()
        valid.update(idx_rows.loc[idx_rows["TIME_STAMP"] == latest_ts, "SYMBOL"])

    return valid


# ──────────────────────────────────────────────────────────────
# RANKING
# ──────────────────────────────────────────────────────────────


def _precompute_all_metrics(all_ohlcv: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Pre-compute scoring metrics for every symbol once before the rebalance loop."""
    result: dict[str, pd.DataFrame] = {}
    for sym, df in all_ohlcv.items():
        try:
            result[sym] = precompute_metrics(df)
        except Exception as exc:
            logging.warning("precompute_metrics failed for %s: %s", sym, exc)
    return result


def _precompute_stage2_scores(all_ohlcv: dict[str, pd.DataFrame]) -> dict[str, pd.Series]:
    """Pre-compute Stage 2 score series per symbol for fast .asof() lookup."""
    result: dict[str, pd.Series] = {}
    for sym, df in all_ohlcv.items():
        try:
            result[sym] = compute_rolling_stage2(df)["Score"]
        except Exception as exc:
            logging.warning("compute_rolling_stage2 failed for %s: %s", sym, exc)
    return result


def rank_universe_at_date(
    all_ohlcv: dict[str, pd.DataFrame],
    as_of: pd.Timestamp,
    sort_method: str,
    valid_symbols: set[str] | None = None,
    min_history_days: int = 750,
    apply_volume_filter: bool = True,
    precomputed: dict[str, pd.DataFrame] | None = None,
    return_excluded_reasons: bool = False,
) -> "list[str] | tuple[list[str], dict[str, str]]":
    """
    Score every symbol using data up to `as_of` and return symbols ordered
    best→worst by the chosen sort_method.

    valid_symbols          : if provided, only these symbols are considered
                             (historical constituent filter — prevents survivorship bias)
    min_history_days       : minimum trading days of history required before as_of
                             (default 750 ≈ 3 years; prevents ranking on thin data)
    apply_volume_filter    : if True, exclude symbols whose median volume < MIN_VOLUME
    precomputed            : pre-computed metric DataFrames from _precompute_all_metrics;
                             when provided, uses O(log n) date lookup instead of slicing OHLCV
    return_excluded_reasons: if True, return (ranked_list, excluded_reasons_dict) where
                             excluded_reasons maps symbol → reason string for every symbol
                             in valid_symbols that failed a pre-ranking filter
    """
    ranked: list[tuple[str, float]] = []
    excluded_reasons: dict[str, str] = {} if return_excluded_reasons else None  # type: ignore[assignment]

    for sym, df in all_ohlcv.items():
        if valid_symbols is not None and sym not in valid_symbols:
            continue

        if precomputed is not None:
            # Fast path: O(log n) binary-search lookup in pre-computed DataFrame
            mdf = precomputed.get(sym)
            if mdf is None or mdf.empty:
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "no_data"
                continue
            idx = mdf.index.searchsorted(as_of, side="right") - 1
            if idx < 0 or idx >= len(mdf):
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "no_data"
                continue
            row = mdf.iloc[idx]
            if row["_count"] < min_history_days:
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "insufficient_history"
                continue
            if row["_missing_rate"] > 0.05:
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "missing_data"
                continue
            if apply_volume_filter:
                vol = row.get("Vol_Median")
                if pd.isna(vol) or vol < MIN_VOLUME:
                    if excluded_reasons is not None:
                        excluded_reasons[sym] = "low_volume"
                    continue
            score = _calculate_avg_sharpe(row, sort_method)
            if score is None or pd.isna(score):
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "no_valid_score"
                continue
        else:
            # Original path: slice OHLCV and score on demand
            sub = df[df.index <= as_of]
            if len(sub) < min_history_days:
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "insufficient_history"
                continue
            # Reject stocks with > 5% missing close prices (suspended / delisted mid-period)
            if sub["Close"].isna().mean() > 0.05:
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "missing_data"
                continue
            metrics = score_momentum(sub)
            if metrics is None:
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "no_valid_score"
                continue
            if apply_volume_filter:
                vol = metrics.get("Vol_Median")
                if vol is None or vol < MIN_VOLUME:
                    if excluded_reasons is not None:
                        excluded_reasons[sym] = "low_volume"
                    continue
            score = _calculate_avg_sharpe(metrics, sort_method)
            if score is None:
                if excluded_reasons is not None:
                    excluded_reasons[sym] = "no_valid_score"
                continue

        ranked.append((sym, score))

    ranked.sort(key=lambda x: x[1], reverse=True)
    ranked_list = [sym for sym, _ in ranked]

    if return_excluded_reasons:
        return ranked_list, excluded_reasons
    return ranked_list


# ──────────────────────────────────────────────────────────────
# REBALANCE DATE GENERATION
# ──────────────────────────────────────────────────────────────


def _trading_days(all_ohlcv: dict[str, pd.DataFrame], start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    """Union of all dates present in the OHLCV store within [start, end]."""
    if not all_ohlcv:
        return pd.DatetimeIndex([])
    combined = pd.concat([df["Close"].loc[start:end] for df in all_ohlcv.values()], axis=1)
    return combined.index


def get_rebalance_dates(
    trading_days: pd.DatetimeIndex,
    freq: str,
) -> list[pd.Timestamp]:
    """
    Return rebalance dates from trading_days based on freq:
      'weekly'     – last trading day of each calendar week
      'biweekly'   – last trading day of every other calendar week
      'monthly'    – last trading day of each calendar month
      'quarterly'  – last trading day of each calendar quarter
      'half-yearly'– last trading day of each half-year (Jan–Jun, Jul–Dec)
    """
    if trading_days.empty:
        return []

    series = pd.Series(trading_days, index=trading_days)

    if freq == "monthly":
        grouped = series.groupby([series.dt.year, series.dt.month])
        return [grp.iloc[-1] for _, grp in grouped]

    if freq == "quarterly":
        grouped = series.groupby([series.dt.year, series.dt.quarter])
        return [grp.iloc[-1] for _, grp in grouped]

    if freq == "half-yearly":
        half = (series.dt.month - 1) // 6
        grouped = series.groupby([series.dt.year, half])
        return [grp.iloc[-1] for _, grp in grouped]

    # week number per year
    week_key = trading_days.isocalendar().week.values
    year_key = trading_days.isocalendar().year.values

    dates_df = pd.DataFrame({"date": trading_days, "year": year_key, "week": week_key})
    last_per_week = dates_df.groupby(["year", "week"])["date"].last().reset_index()
    last_per_week = last_per_week.sort_values("date").reset_index(drop=True)

    if freq == "weekly":
        return last_per_week["date"].tolist()
    else:  # biweekly – every other week
        return last_per_week["date"].iloc[::2].tolist()


# ──────────────────────────────────────────────────────────────
# DAILY NAV HELPERS
# ──────────────────────────────────────────────────────────────


def _daily_returns(all_ohlcv: dict[str, pd.DataFrame], symbols: list[str], dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Return a DataFrame of daily close-to-close returns for given symbols over dates."""
    frames = {}
    for sym in symbols:
        if sym in all_ohlcv:
            s = all_ohlcv[sym]["Close"].reindex(dates).ffill(limit=5)
            frames[sym] = s.pct_change()
    if not frames:
        return pd.DataFrame(index=dates)
    return pd.DataFrame(frames, index=dates)


# ──────────────────────────────────────────────────────────────
# REBALANCE HELPERS
# ──────────────────────────────────────────────────────────────


def _drift_weights(
    weights: dict[str, float],
    all_ohlcv: dict[str, pd.DataFrame],
    prev_day: pd.Timestamp,
    curr_day: pd.Timestamp,
) -> dict[str, float]:
    """Adjust portfolio weights to reflect price drift between two rebalance dates."""
    if not weights:
        return weights
    drifted: dict[str, float] = {}
    for s, w in weights.items():
        try:
            closes = all_ohlcv[s]["Close"]
            p_prev = float(closes.at[prev_day]) if prev_day in closes.index else None
            p_now = float(closes.at[curr_day]) if curr_day in closes.index else None
            drifted[s] = w * (p_now / p_prev) if (p_prev and p_now and p_prev > 0) else w
        except (KeyError, TypeError, ZeroDivisionError):
            drifted[s] = w
    total_d = sum(drifted.values())
    return {s: w / total_d for s, w in drifted.items()} if total_d > 0 else drifted


def _realise_weight_reductions(
    lots: dict[str, list[dict]],
    old_weights: dict[str, float],
    new_weights: dict[str, float],
    all_ohlcv: dict[str, pd.DataFrame],
    day: pd.Timestamp,
    nav: float,
) -> tuple[float, float, float, float]:
    """FIFO-tax every weight reduction, including partial trims and full exits."""
    st_g = st_l = lt_g = lt_l = 0.0
    for sym in set(old_weights) | set(new_weights):
        sold_w = old_weights.get(sym, 0.0) - new_weights.get(sym, 0.0)
        if sold_w <= 1e-12:
            continue
        price = _close_price(all_ohlcv, sym, day)
        if price is None or price <= 0:
            continue
        shares_to_sell = (nav * sold_w) / price
        sym_lots = lots.get(sym, [])
        while shares_to_sell > 1e-12 and sym_lots:
            lot = sym_lots[0]
            lot_shares = float(lot["shares"])
            used = min(lot_shares, shares_to_sell)
            gain = (price - float(lot["price"])) * used
            is_long_term = day > lot["date"] + relativedelta(months=12)
            if is_long_term:
                if gain >= 0:
                    lt_g += gain
                else:
                    lt_l += abs(gain)
            else:
                if gain >= 0:
                    st_g += gain
                else:
                    st_l += abs(gain)
            lot["shares"] = lot_shares - used
            shares_to_sell -= used
            if lot["shares"] <= 1e-12:
                sym_lots.pop(0)
        if sym_lots:
            lots[sym] = sym_lots
        else:
            lots.pop(sym, None)
    return st_g, st_l, lt_g, lt_l


def _record_weight_increases(
    lots: dict[str, list[dict]],
    old_weights: dict[str, float],
    new_weights: dict[str, float],
    all_ohlcv: dict[str, pd.DataFrame],
    day: pd.Timestamp,
    nav: float,
) -> None:
    """Append FIFO lots for every weight increase at the rebalance close."""
    for sym, new_w in new_weights.items():
        bought_w = new_w - old_weights.get(sym, 0.0)
        if bought_w <= 1e-12:
            continue
        price = _close_price(all_ohlcv, sym, day)
        if price is None or price <= 0:
            continue
        lots.setdefault(sym, []).append({"date": day, "price": price, "shares": (nav * bought_w) / price})


def _compute_holdings_classic(
    current_holdings: set[str],
    top_m: set[str],
    top_n: set[str],
    ranked: list[str],
    stage2_precomputed: dict[str, pd.Series],
    rank_as_of: pd.Timestamp,
    prev_stage2_scores: dict[str, float],
    stage2_entry_filter: bool,
    stage2_drop_exit: bool,
    stage2_entry_threshold: int,
    stage2_drop_threshold: int,
    rebalance_freq: str,
) -> tuple[set[str], set[str], dict[str, str], dict[str, str]]:
    """
    Classic band rule: exit if rank > N, enter if rank ≤ M (may briefly exceed M).
    Optional Stage 2 filters add/remove holdings on top of the momentum band rule.
    Returns (entries, exits, entry_reasons, exit_reasons).
    """
    rank_pos = {s: rank_idx + 1 for rank_idx, s in enumerate(ranked)}

    exits: set[str] = current_holdings - top_n
    exit_reasons: dict[str, str] = {
        sym: (f"rank #{rank_pos[sym]}" if sym in rank_pos else "left universe") for sym in exits
    }

    entries: set[str] = top_m - current_holdings
    entry_reasons: dict[str, str] = {sym: f"rank #{rank_pos.get(sym, '?')}" for sym in entries}

    if stage2_entry_filter and stage2_precomputed and rebalance_freq == "weekly":
        for sym in ranked:
            if sym in current_holdings or sym in entries:
                continue
            s2 = stage2_precomputed.get(sym)
            if s2 is None:
                continue
            curr_s2 = s2.asof(rank_as_of)
            if pd.isna(curr_s2):
                continue
            prev_s2 = prev_stage2_scores.get(sym)
            if prev_s2 is None:
                continue
            if curr_s2 - prev_s2 >= stage2_entry_threshold:
                entries = entries | {sym}
                entry_reasons[sym] = f"S2 +{int(curr_s2 - prev_s2)}"

    if stage2_drop_exit and stage2_precomputed and rebalance_freq == "weekly":
        for sym in list(current_holdings - exits):
            s2_series = stage2_precomputed.get(sym)
            if s2_series is None:
                continue
            curr_score = s2_series.asof(rank_as_of)
            prev_score = prev_stage2_scores.get(sym)
            if (
                prev_score is not None
                and not pd.isna(curr_score)
                and (prev_score - curr_score) >= stage2_drop_threshold
            ):
                exits.add(sym)
                exit_reasons[sym] = f"S2 -{int(prev_score - curr_score)}"

    return entries, exits, entry_reasons, exit_reasons


def _compute_holdings_displacement(
    current_holdings: set[str],
    top_m: set[str],
    top_n: set[str],
    ranked: list[str],
    m: int,
    stage2_precomputed: dict[str, pd.Series],
    rank_as_of: pd.Timestamp,
    prev_stage2_scores: dict[str, float],
    stage2_entry_filter: bool,
    stage2_drop_exit: bool,
    stage2_entry_threshold: int,
    stage2_drop_threshold: int,
    rebalance_freq: str,
) -> tuple[set[str], set[str], dict[str, str], dict[str, str]]:
    """
    Displacement band rule: N is the Worst Rank Held (WRH).
    Stocks ranked > N exit unconditionally; stocks ranked M+1..N sit in a buffer zone
    and stay until their rank exceeds N. New entrants (top-M or S2 jumpers) fill freed
    slots only — they never displace buffer-zone incumbents. Hard cap stays at M.
    Returns (entries, exits, entry_reasons, exit_reasons).
    """
    # rank_of: O(1) lookup; stocks absent from `ranked` get rank = len(ranked) (worst).
    rank_of = {s: rank_idx for rank_idx, s in enumerate(ranked)}
    _worst = len(ranked)

    # Step 1 — WRH exits: rank > N must leave unconditionally
    wrh_exits: set[str] = current_holdings - top_n
    exit_reasons: dict[str, str] = {
        sym: (f"rank #{rank_of.get(sym, _worst) + 1}" if sym in rank_of else "left universe") for sym in wrh_exits
    }
    holdings_after_wrh = current_holdings - wrh_exits

    # Step 2 — Stage 2 drop exits: free additional slots
    s2_exits: set[str] = set()
    if stage2_drop_exit and stage2_precomputed and rebalance_freq == "weekly":
        for sym in list(holdings_after_wrh):
            s2_series = stage2_precomputed.get(sym)
            if s2_series is None:
                continue
            curr_score = s2_series.asof(rank_as_of)
            prev_score = prev_stage2_scores.get(sym)
            if (
                prev_score is not None
                and not pd.isna(curr_score)
                and (prev_score - curr_score) >= stage2_drop_threshold
            ):
                s2_exits.add(sym)
                exit_reasons[sym] = f"S2 -{int(prev_score - curr_score)}"

    exits = wrh_exits | s2_exits
    holdings_after_exits = current_holdings - exits

    # Step 3 — build candidate pool: top-M new entrants + Stage 2 score jumpers
    candidates: set[str] = top_m - holdings_after_exits
    s2_jump_deltas: dict[str, int] = {}
    if stage2_entry_filter and stage2_precomputed and rebalance_freq == "weekly":
        for sym in ranked:
            if sym in holdings_after_exits or sym in candidates:
                continue
            s2 = stage2_precomputed.get(sym)
            if s2 is None:
                continue
            curr_s2 = s2.asof(rank_as_of)
            if pd.isna(curr_s2):
                continue
            prev_s2 = prev_stage2_scores.get(sym)
            if prev_s2 is None:
                continue
            delta = curr_s2 - prev_s2
            if delta >= stage2_entry_threshold:
                candidates.add(sym)
                s2_jump_deltas[sym] = int(delta)

    # Step 4 — fill freed slots from combined candidates, best rank first
    entries_wanted = sorted(candidates, key=lambda s: rank_of.get(s, _worst))
    free_slots = max(0, m - len(holdings_after_exits))
    entries: set[str] = set(entries_wanted[:free_slots])
    entry_reasons: dict[str, str] = {
        sym: (f"S2 +{s2_jump_deltas[sym]}" if sym in s2_jump_deltas else f"rank #{rank_of.get(sym, _worst) + 1}")
        for sym in entries
    }

    return entries, exits, entry_reasons, exit_reasons


def _compute_weight_variants(
    new_holdings: set[str],
    entries: set[str],
    exits: set[str],
    marg_weights: dict[str, float],
    prop_weights: dict[str, float],
    size: int,
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """
    Compute the three weight vectors after a rebalance:
      Full rebalance    : equal weight 1/size for all holdings.
      Slot-fill marginal: freed capital split equally among new entrants only.
      Prop-fill marginal: entrants seeded at 1/size; normalization redistributes surplus.
    Returns (new_full, new_slot, new_prop).
    """
    if size <= 0 or not new_holdings:
        return {}, {}, {}

    new_full = {s: 1.0 / size for s in new_holdings}

    freed_slot = sum(marg_weights.get(s, 0.0) for s in exits)
    new_slot: dict[str, float] = {s: marg_weights[s] for s in new_holdings - entries if s in marg_weights}
    if entries:
        per_entry_slot = (freed_slot / len(entries)) if freed_slot > 0 else (1.0 / size)
        for s in entries:
            new_slot[s] = per_entry_slot
    if not new_slot:
        new_slot = {s: 1.0 / size for s in new_holdings}
    total_w = sum(new_slot.values())
    if total_w > 0:
        new_slot = {s: w / total_w for s, w in new_slot.items()}

    new_prop: dict[str, float] = {s: prop_weights[s] for s in new_holdings - entries if s in prop_weights}
    if entries:
        for s in entries:
            new_prop[s] = 1.0 / size
    if not new_prop:
        new_prop = {s: 1.0 / size for s in new_holdings}
    total_w = sum(new_prop.values())
    if total_w > 0:
        new_prop = {s: w / total_w for s, w in new_prop.items()}

    return new_full, new_slot, new_prop


def _compute_summary_stats(nav_df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-column CAGR / Sharpe / drawdown / Calmar / Sortino from a NAV DataFrame."""
    stats = {}
    for col in nav_df.columns:
        s = nav_df[col].dropna()
        if len(s) < 2:
            continue
        daily_ret = s.pct_change().dropna()
        n_days = len(s)
        cagr = (s.iloc[-1] / s.iloc[0]) ** (252 / (n_days - 1)) - 1
        sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() > 0 else np.nan
        rolling_max = s.cummax()
        drawdown = (s - rolling_max) / rolling_max
        max_dd = drawdown.min()
        calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan
        neg_ret = daily_ret[daily_ret < 0]
        sortino = (
            (daily_ret.mean() / neg_ret.std() * np.sqrt(252)) if len(neg_ret) > 1 and neg_ret.std() > 0 else np.nan
        )
        stats[col] = {
            "CAGR (%)": round(cagr * 100, 2),
            "Sharpe": round(float(sharpe), 3) if not np.isnan(sharpe) else np.nan,
            "Max Drawdown (%)": round(max_dd * 100, 2),
            "Calmar": round(float(calmar), 3) if not np.isnan(calmar) else np.nan,
            "Sortino": round(float(sortino), 3) if not np.isnan(sortino) else np.nan,
            "Final NAV": round(s.iloc[-1], 2),
        }
    return pd.DataFrame(stats).T


# ──────────────────────────────────────────────────────────────
# CORE BACKTEST
# ──────────────────────────────────────────────────────────────


def _apply_weight_cap(weights: dict[str, float], cap: float) -> dict[str, float]:
    """Trim positions exceeding cap using water-filling: cap top-k positions and scale the rest.

    If cap < 1/n (infeasible), falls back to equal weights.
    """
    if not weights or cap <= 0.0 or cap >= 1.0:
        return weights
    n = len(weights)
    total = sum(weights.values())
    if total <= 0:
        return weights
    # Sort descending by normalised weight
    ordered = sorted(((s, v / total) for s, v in weights.items()), key=lambda x: -x[1])
    for k in range(n + 1):
        if k == n or (1.0 - k * cap) <= 0:
            # Infeasible: cap too tight — return equal weights
            return {s: 1.0 / n for s, _ in ordered}
        remaining = 1.0 - k * cap
        free = ordered[k:]
        free_total = sum(v for _, v in free)
        if free_total <= 0:
            return {s: 1.0 / n for s, _ in ordered}
        scale = remaining / free_total
        if ordered[k][1] * scale <= cap + 1e-9:
            result = {s: cap for s, _ in ordered[:k]}
            for s, v in free:
                result[s] = v * scale
            return result
    return {s: 1.0 / n for s, _ in ordered}


def run_backtest(
    all_ohlcv: dict[str, pd.DataFrame],
    benchmarks: dict[str, pd.Series],
    config: BacktestConfig,
) -> dict:
    """Run both portfolio variants and return NAV series + summary stats."""
    m = config.m
    n = config.n
    rebalance_freq = config.rebalance_freq
    sort_method = config.sort_method
    start_date = config.start_date
    end_date = config.end_date
    compositions_df = config.compositions_df
    index_names = config.index_names
    transaction_cost_pct = config.transaction_cost_pct
    min_history_days = config.min_history_days
    apply_volume_filter = config.apply_volume_filter
    band_rule = config.band_rule
    brokerage_per_sale = config.brokerage_per_sale
    initial_capital = config.initial_capital
    ltcg_rate = config.ltcg_rate
    stcg_rate = config.stcg_rate
    stage2_drop_exit = config.stage2_drop_exit
    stage2_drop_threshold = config.stage2_drop_threshold
    stage2_entry_filter = config.stage2_entry_filter
    stage2_entry_threshold = config.stage2_entry_threshold
    max_position_pct = config.max_position_pct

    # ── input validation ──
    if m < 1 or n < 1:
        return {"error": f"M ({m}) and N ({n}) must both be ≥ 1."}
    if m >= n:
        return {"error": f"Entry band M ({m}) must be strictly less than exit band N ({n})."}
    try:
        t0 = pd.Timestamp(start_date)
        t1 = pd.Timestamp(end_date)
    except Exception:
        return {"error": f"Invalid date format: start_date={start_date!r}, end_date={end_date!r}. Use 'YYYY-MM-DD'."}
    if t0 >= t1:
        return {"error": f"start_date ({start_date}) must be before end_date ({end_date})."}
    if not (0.0 <= transaction_cost_pct <= 0.05):
        return {"error": f"transaction_cost_pct ({transaction_cost_pct:.4f}) must be between 0 and 0.05 (5%)."}
    if not (0.0 <= ltcg_rate <= 1.0):
        return {"error": f"ltcg_rate ({ltcg_rate}) must be between 0 and 1."}
    if not (0.0 <= stcg_rate <= 1.0):
        return {"error": f"stcg_rate ({stcg_rate}) must be between 0 and 1."}
    if max_position_pct is not None and not (0.0 < max_position_pct <= 100.0):
        return {"error": f"max_position_pct ({max_position_pct}) must be between 0 (exclusive) and 100."}
    schema_errors = _validate_ohlcv_schema(all_ohlcv)
    if schema_errors:
        return {"error": "Invalid OHLCV input: " + "; ".join(schema_errors[:5])}

    # Warn once if any requested index has no composition data before the start date.
    if compositions_df is not None and not compositions_df.empty and index_names:
        comp_check = compositions_df.copy()
        comp_check["_INDEX_KEY"] = comp_check["INDEX_NAME"].map(_canonical_index_name)
        for idx_name in index_names:
            idx_key = _canonical_index_name(idx_name)
            idx_rows = comp_check[comp_check["_INDEX_KEY"] == idx_key]
            if idx_rows.empty:
                continue  # covered by the existing "None found" warning path
            earliest = idx_rows["TIME_STAMP"].min()
            if earliest > t0:
                logging.warning(
                    "Index %r composition data starts on %s, which is after the backtest start %s. "
                    "This index will be excluded from the universe filter until its data begins.",
                    idx_name,
                    earliest.date(),
                    t0.date(),
                )

    trading_days = _trading_days(all_ohlcv, t0, t1)
    if len(trading_days) < 20:
        return {"error": "Insufficient trading days in selected range (need at least one month of data)."}

    rebalance_dates = get_rebalance_dates(trading_days, rebalance_freq)
    # Exclude the first trading day: we need T-1 close to rank without look-ahead bias.
    rebalance_set = set(rebalance_dates) - {trading_days[0]}

    # Pre-filter compositions once so the per-rebalance call is a cheap date slice
    comp_prepared = _prepare_compositions(compositions_df, index_names or [])

    # Pre-compute rolling metrics once per symbol (O(symbols)) instead of per rebalance date
    precomputed = _precompute_all_metrics(all_ohlcv)
    stage2_precomputed = _precompute_stage2_scores(all_ohlcv) if (stage2_drop_exit or stage2_entry_filter) else {}

    # Pre-build returns matrix so daily NAV update uses O(1) row lookups instead of per-symbol index ops
    returns_matrix = _daily_returns(all_ohlcv, list(all_ohlcv.keys()), trading_days)

    # ── initialise portfolios ──
    full_weights: dict[str, float] = {}
    full_weights_prev: dict[str, float] = {}  # drift-adjusted full weights from prior rebalance
    marg_weights: dict[str, float] = {}  # slot-fill marginal weights
    prop_weights: dict[str, float] = {}  # prop-fill marginal weights
    current_holdings: set[str] = set()
    prev_rebalance_day = None  # needed to drift-adjust weights at each rebalance
    prev_stage2_scores: dict[str, float] = {}  # Stage 2 score at previous rebalance per holding

    nav_full = 100.0
    nav_marg = 100.0
    nav_prop = 100.0

    nav_records: list[dict] = []
    holdings_log: list[dict] = []
    turnover_log_full: list[float] = []
    turnover_log_marg: list[float] = []
    turnover_log_prop: list[float] = []
    cost_log_full: list[float] = []
    cost_log_marg: list[float] = []
    cost_log_prop: list[float] = []
    holdings_sizes: list[int] = []

    lots_full: dict[str, list[dict]] = {}
    lots_marg: dict[str, list[dict]] = {}
    lots_prop: dict[str, list[dict]] = {}
    tax_log_full: list[float] = []
    tax_log_marg: list[float] = []
    tax_log_prop: list[float] = []
    brok_log_full: list[float] = []
    brok_log_marg: list[float] = []
    brok_log_prop: list[float] = []

    # FY-level CGT accumulators (reset each new FY)
    current_fy: int | None = None
    fy_st_g_full = fy_st_l_full = fy_lt_g_full = fy_lt_l_full = 0.0
    fy_st_g_marg = fy_st_l_marg = fy_lt_g_marg = fy_lt_l_marg = 0.0
    fy_st_g_prop = fy_st_l_prop = fy_lt_g_prop = fy_lt_l_prop = 0.0
    # Carry-forward loss buckets: list of (expiry_fy, amount)
    cf_st_full: list[tuple[int, float]] = []
    cf_lt_full: list[tuple[int, float]] = []
    cf_st_marg: list[tuple[int, float]] = []
    cf_lt_marg: list[tuple[int, float]] = []
    cf_st_prop: list[tuple[int, float]] = []
    cf_lt_prop: list[tuple[int, float]] = []

    for i, day in enumerate(trading_days):
        # ── rebalance ──
        if day in rebalance_set:
            # Restrict universe to historically valid members on this date
            valid_syms = _valid_symbols_at_date(comp_prepared, index_names or [], day)

            # Rank using previous day's data to avoid look-ahead bias:
            # rankings are determined from T-1 close; trades execute at T close.
            rank_as_of = trading_days[i - 1] if i > 0 else day
            ranked, _excluded_reasons = rank_universe_at_date(
                all_ohlcv,
                rank_as_of,
                sort_method,
                valid_symbols=valid_syms,
                min_history_days=min_history_days,
                apply_volume_filter=apply_volume_filter,
                precomputed=precomputed,
                return_excluded_reasons=True,
            )
            top_m = set(ranked[:m])
            top_n = set(ranked[:n])

            _s2_kwargs = dict(
                stage2_precomputed=stage2_precomputed,
                rank_as_of=rank_as_of,
                prev_stage2_scores=prev_stage2_scores,
                stage2_entry_filter=stage2_entry_filter,
                stage2_drop_exit=stage2_drop_exit,
                stage2_entry_threshold=stage2_entry_threshold,
                stage2_drop_threshold=stage2_drop_threshold,
                rebalance_freq=rebalance_freq,
            )
            if band_rule == "displacement":
                entries, exits, entry_reasons, exit_reasons = _compute_holdings_displacement(
                    current_holdings, top_m, top_n, ranked, m, **_s2_kwargs
                )
            else:
                entries, exits, entry_reasons, exit_reasons = _compute_holdings_classic(
                    current_holdings, top_m, top_n, ranked, **_s2_kwargs
                )

            new_holdings = (current_holdings - exits) | entries

            if not new_holdings:
                new_holdings = top_m if top_m else (set() if not ranked else current_holdings)

            # Update Stage 2 score baseline for next rebalance.
            # Track full ranked universe (not just holdings) so entry-jump signal
            # can detect score rises on stocks we don't yet own.
            if (stage2_drop_exit or stage2_entry_filter) and stage2_precomputed:
                prev_stage2_scores.clear()
                for sym in ranked:
                    s2_series = stage2_precomputed.get(sym)
                    if s2_series is not None:
                        score = s2_series.asof(rank_as_of)
                        if not pd.isna(score):
                            prev_stage2_scores[sym] = float(score)

            size = len(new_holdings)
            holdings_sizes.append(size)

            # ── drift-adjust all weight trackers to reflect price movement since last rebalance ──
            # This must happen before weight assignment so exit weights use current market values.
            if prev_rebalance_day is not None:
                full_weights_prev = _drift_weights(full_weights_prev, all_ohlcv, prev_rebalance_day, day)
                marg_weights = _drift_weights(marg_weights, all_ohlcv, prev_rebalance_day, day)
                prop_weights = _drift_weights(prop_weights, all_ohlcv, prev_rebalance_day, day)

            # ── save drift-adjusted weights before assignment (needed for turnover diff + CGT) ──
            old_full_weights = dict(full_weights_prev)
            old_marg_weights = dict(marg_weights)
            old_prop_weights = dict(prop_weights)

            # ── compute new weights for all three tracks ──
            new_full, new_slot, new_prop = _compute_weight_variants(
                new_holdings, entries, exits, marg_weights, prop_weights, size
            )

            # ── position cap: trim any overweight position, redistribute to smaller positions ──
            pre_cap_full = {s: round(w * 100, 4) for s, w in new_full.items()}
            pre_cap_slot = {s: round(w * 100, 4) for s, w in new_slot.items()}
            pre_cap_prop = {s: round(w * 100, 4) for s, w in new_prop.items()}
            if max_position_pct:
                _cap = max_position_pct / 100.0
                new_full = _apply_weight_cap(new_full, _cap)
                new_slot = _apply_weight_cap(new_slot, _cap)
                new_prop = _apply_weight_cap(new_prop, _cap)

            # ── weight-based turnover: abs-diff formula captures all implicit weight changes ──
            # For each variant: traded = Σ|old_w(s) - new_w(s)| over all affected stocks.
            # This counts exits (old→0), entries (0→new), incumbent rebalances, and implicit
            # sells/buys caused by normalization when freed capital != entrant allocation.
            universe = new_holdings | exits

            traded_w_full = (
                sum(full_weights_prev.get(s, 0.0) for s in exits)
                + sum(1.0 / size for s in entries)
                + sum(abs(full_weights_prev.get(s, 0.0) - 1.0 / size) for s in (new_holdings - entries))
            )
            turnover_log_full.append(traded_w_full)

            traded_w_marg = sum(abs(old_marg_weights.get(s, 0.0) - new_slot.get(s, 0.0)) for s in universe)
            turnover_log_marg.append(traded_w_marg)

            traded_w_prop = sum(abs(old_prop_weights.get(s, 0.0) - new_prop.get(s, 0.0)) for s in universe)
            turnover_log_prop.append(traded_w_prop)

            # ── transaction cost drag — separate for full, marginal, and prop ──
            if i > 0 and transaction_cost_pct > 0 and size > 0:
                cost_drag_full = traded_w_full * transaction_cost_pct
                cost_drag_marg = traded_w_marg * transaction_cost_pct
                cost_drag_prop = traded_w_prop * transaction_cost_pct
                nav_full *= 1.0 - cost_drag_full
                nav_marg *= 1.0 - cost_drag_marg
                nav_prop *= 1.0 - cost_drag_prop
                cost_log_full.append(cost_drag_full)
                cost_log_marg.append(cost_drag_marg)
                cost_log_prop.append(cost_drag_prop)

            # ── flat brokerage per sale (exits only, no charge on buys) ──
            if i > 0 and brokerage_per_sale > 0 and initial_capital > 0 and exits:
                n_exits = len(exits)
                brok_drag_full = (brokerage_per_sale * n_exits) / (initial_capital * nav_full / 100.0)
                brok_drag_marg = (brokerage_per_sale * n_exits) / (initial_capital * nav_marg / 100.0)
                brok_drag_prop = (brokerage_per_sale * n_exits) / (initial_capital * nav_prop / 100.0)
                nav_full *= 1.0 - brok_drag_full
                nav_marg *= 1.0 - brok_drag_marg
                nav_prop *= 1.0 - brok_drag_prop
                brok_log_full.append(brok_drag_full)
                brok_log_marg.append(brok_drag_marg)
                brok_log_prop.append(brok_drag_prop)
            else:
                brok_log_full.append(0.0)
                brok_log_marg.append(0.0)
                brok_log_prop.append(0.0)

            # ── capital gains tax (India LTCG / STCG, FY-level with carry-forward) ──
            if ltcg_rate > 0 or stcg_rate > 0:
                day_fy = _financial_year(day)

                # ── FY boundary: close out prior FY and apply its tax ──
                if current_fy is not None and day_fy != current_fy:
                    tax_full, cf_st_full, cf_lt_full = _compute_fy_tax(
                        current_fy,
                        fy_st_g_full,
                        fy_st_l_full,
                        fy_lt_g_full,
                        fy_lt_l_full,
                        cf_st_full,
                        cf_lt_full,
                        stcg_rate,
                        ltcg_rate,
                    )
                    tax_marg, cf_st_marg, cf_lt_marg = _compute_fy_tax(
                        current_fy,
                        fy_st_g_marg,
                        fy_st_l_marg,
                        fy_lt_g_marg,
                        fy_lt_l_marg,
                        cf_st_marg,
                        cf_lt_marg,
                        stcg_rate,
                        ltcg_rate,
                    )
                    tax_prop, cf_st_prop, cf_lt_prop = _compute_fy_tax(
                        current_fy,
                        fy_st_g_prop,
                        fy_st_l_prop,
                        fy_lt_g_prop,
                        fy_lt_l_prop,
                        cf_st_prop,
                        cf_lt_prop,
                        stcg_rate,
                        ltcg_rate,
                    )
                    drag_full = tax_full / nav_full if nav_full > 0 else 0.0
                    drag_marg = tax_marg / nav_marg if nav_marg > 0 else 0.0
                    drag_prop = tax_prop / nav_prop if nav_prop > 0 else 0.0
                    nav_full *= 1.0 - drag_full
                    nav_marg *= 1.0 - drag_marg
                    nav_prop *= 1.0 - drag_prop
                    tax_log_full.append(drag_full)
                    tax_log_marg.append(drag_marg)
                    tax_log_prop.append(drag_prop)
                    # Reset FY accumulators
                    fy_st_g_full = fy_st_l_full = fy_lt_g_full = fy_lt_l_full = 0.0
                    fy_st_g_marg = fy_st_l_marg = fy_lt_g_marg = fy_lt_l_marg = 0.0
                    fy_st_g_prop = fy_st_l_prop = fy_lt_g_prop = fy_lt_l_prop = 0.0
                    current_fy = day_fy
                elif current_fy is None:
                    current_fy = day_fy

                # Accumulate realised gains/losses from every weight reduction.
                if i > 0:
                    s_g, s_l, l_g, l_l = _realise_weight_reductions(
                        lots_full, old_full_weights, new_full, all_ohlcv, day, nav_full
                    )
                    fy_st_g_full += s_g
                    fy_st_l_full += s_l
                    fy_lt_g_full += l_g
                    fy_lt_l_full += l_l

                    s_g, s_l, l_g, l_l = _realise_weight_reductions(
                        lots_marg, old_marg_weights, new_slot, all_ohlcv, day, nav_marg
                    )
                    fy_st_g_marg += s_g
                    fy_st_l_marg += s_l
                    fy_lt_g_marg += l_g
                    fy_lt_l_marg += l_l

                    s_g, s_l, l_g, l_l = _realise_weight_reductions(
                        lots_prop, old_prop_weights, new_prop, all_ohlcv, day, nav_prop
                    )
                    fy_st_g_prop += s_g
                    fy_st_l_prop += s_l
                    fy_lt_g_prop += l_g
                    fy_lt_l_prop += l_l

            # Record new FIFO lots for every weight increase.
            _record_weight_increases(lots_full, old_full_weights, new_full, all_ohlcv, day, nav_full)
            _record_weight_increases(lots_marg, old_marg_weights, new_slot, all_ohlcv, day, nav_marg)
            _record_weight_increases(lots_prop, old_prop_weights, new_prop, all_ohlcv, day, nav_prop)

            # ── store pre-computed weights (calculated above before turnover/costs) ──
            full_weights = new_full
            full_weights_prev = dict(new_full)  # store for next rebalance's drift-adjust & turnover
            marg_weights = new_slot
            prop_weights = new_prop

            prev_rebalance_day = day
            current_holdings = new_holdings
            holdings_log.append(
                {
                    "date": day,
                    "holdings": sorted(current_holdings),
                    "entries": [f"{s} ({entry_reasons[s]})" if s in entry_reasons else s for s in sorted(entries)],
                    "exits": [f"{s} ({exit_reasons[s]})" if s in exit_reasons else s for s in sorted(exits)],
                    "full_ranking": ranked,
                    "valid_universe_size": len(valid_syms) if valid_syms is not None else len(all_ohlcv),
                    "index_universe": sorted(valid_syms) if valid_syms is not None else None,
                    "excluded_reasons": _excluded_reasons,
                    "full_turnover_pct": round(traded_w_full * 100, 2),
                    "marg_turnover_pct": round(traded_w_marg * 100, 2),
                    "prop_turnover_pct": round(traded_w_prop * 100, 2),
                    # snapshot weights at this rebalance (copies — originals rebind next iteration)
                    "full_weights": {s: round(w * 100, 4) for s, w in full_weights.items()},
                    "marg_weights": {s: round(w * 100, 4) for s, w in marg_weights.items()},
                    "prop_weights": {s: round(w * 100, 4) for s, w in prop_weights.items()},
                    # pre-cap weights (same as above when max_position_pct is None)
                    "pre_cap_full_weights": pre_cap_full,
                    "pre_cap_marg_weights": pre_cap_slot,
                    "pre_cap_prop_weights": pre_cap_prop,
                }
            )

        # ── daily NAV update ──
        if i > 0 and current_holdings:
            port_ret_full = 0.0
            port_ret_marg = 0.0
            port_ret_prop = 0.0
            if day in returns_matrix.index:
                row = returns_matrix.loc[day]
                for sym in current_holdings:
                    r = row.get(sym, np.nan)
                    if pd.isna(r):
                        continue
                    port_ret_full += full_weights.get(sym, 0.0) * r
                    port_ret_marg += marg_weights.get(sym, 0.0) * r
                    port_ret_prop += prop_weights.get(sym, 0.0) * r
            nav_full *= 1 + port_ret_full
            nav_marg *= 1 + port_ret_marg
            nav_prop *= 1 + port_ret_prop

        nav_records.append(
            {"Date": day, "Full Rebalance": nav_full, "Marginal Rebalance": nav_marg, "Prop Rebalance": nav_prop}
        )

    nav_df = pd.DataFrame(nav_records).set_index("Date")

    # ── apply CGT for the final (possibly partial) financial year ──
    if (ltcg_rate > 0 or stcg_rate > 0) and current_fy is not None:
        tax_full, cf_st_full, cf_lt_full = _compute_fy_tax(
            current_fy,
            fy_st_g_full,
            fy_st_l_full,
            fy_lt_g_full,
            fy_lt_l_full,
            cf_st_full,
            cf_lt_full,
            stcg_rate,
            ltcg_rate,
        )
        tax_marg, cf_st_marg, cf_lt_marg = _compute_fy_tax(
            current_fy,
            fy_st_g_marg,
            fy_st_l_marg,
            fy_lt_g_marg,
            fy_lt_l_marg,
            cf_st_marg,
            cf_lt_marg,
            stcg_rate,
            ltcg_rate,
        )
        tax_prop, cf_st_prop, cf_lt_prop = _compute_fy_tax(
            current_fy,
            fy_st_g_prop,
            fy_st_l_prop,
            fy_lt_g_prop,
            fy_lt_l_prop,
            cf_st_prop,
            cf_lt_prop,
            stcg_rate,
            ltcg_rate,
        )
        drag_full = tax_full / nav_full if nav_full > 0 else 0.0
        drag_marg = tax_marg / nav_marg if nav_marg > 0 else 0.0
        drag_prop = tax_prop / nav_prop if nav_prop > 0 else 0.0
        nav_full *= 1.0 - drag_full
        nav_marg *= 1.0 - drag_marg
        nav_prop *= 1.0 - drag_prop
        tax_log_full.append(drag_full)
        tax_log_marg.append(drag_marg)
        tax_log_prop.append(drag_prop)
        # Update final row in nav_records to reflect post-tax NAV
        if nav_records:
            nav_records[-1]["Full Rebalance"] = nav_full
            nav_records[-1]["Marginal Rebalance"] = nav_marg
            nav_records[-1]["Prop Rebalance"] = nav_prop
        nav_df = pd.DataFrame(nav_records).set_index("Date")

    # ── attach benchmarks ──
    for label, series in benchmarks.items():
        s = series.reindex(trading_days).ffill(limit=5).dropna()
        if s.empty:
            continue
        nav_df[label] = (s / s.iloc[0]) * 100

    # ── stats ──
    stats_df = _compute_summary_stats(nav_df)

    avg_turnover_full = round(np.mean(turnover_log_full) * 100, 1) if turnover_log_full else 0.0
    avg_turnover_marg = round(np.mean(turnover_log_marg) * 100, 1) if turnover_log_marg else 0.0
    avg_turnover_prop = round(np.mean(turnover_log_prop) * 100, 1) if turnover_log_prop else 0.0
    total_cost_full = round(sum(cost_log_full) * 100, 3) if cost_log_full else 0.0
    total_cost_marg = round(sum(cost_log_marg) * 100, 3) if cost_log_marg else 0.0
    total_cost_prop = round(sum(cost_log_prop) * 100, 3) if cost_log_prop else 0.0
    avg_holdings = round(float(np.mean(holdings_sizes)), 1) if holdings_sizes else 0.0
    total_tax_full = round(sum(tax_log_full) * 100, 3) if tax_log_full else 0.0
    total_tax_marg = round(sum(tax_log_marg) * 100, 3) if tax_log_marg else 0.0
    total_tax_prop = round(sum(tax_log_prop) * 100, 3) if tax_log_prop else 0.0
    total_brok_full = round(sum(brok_log_full) * 100, 3) if brok_log_full else 0.0
    total_brok_marg = round(sum(brok_log_marg) * 100, 3) if brok_log_marg else 0.0
    total_brok_prop = round(sum(brok_log_prop) * 100, 3) if brok_log_prop else 0.0

    if "Full Rebalance" in stats_df.index:
        stats_df.loc["Full Rebalance", "Avg Holdings"] = avg_holdings
        stats_df.loc["Full Rebalance", "Avg Turnover (%)"] = avg_turnover_full
        stats_df.loc["Full Rebalance", "Cost Drag (%)"] = total_cost_full
        stats_df.loc["Full Rebalance", "Tax Drag (%)"] = total_tax_full
        stats_df.loc["Full Rebalance", "Brokerage Drag (%)"] = total_brok_full
    if "Marginal Rebalance" in stats_df.index:
        stats_df.loc["Marginal Rebalance", "Avg Holdings"] = avg_holdings
        stats_df.loc["Marginal Rebalance", "Avg Turnover (%)"] = avg_turnover_marg
        stats_df.loc["Marginal Rebalance", "Cost Drag (%)"] = total_cost_marg
        stats_df.loc["Marginal Rebalance", "Tax Drag (%)"] = total_tax_marg
        stats_df.loc["Marginal Rebalance", "Brokerage Drag (%)"] = total_brok_marg
    if "Prop Rebalance" in stats_df.index:
        stats_df.loc["Prop Rebalance", "Avg Holdings"] = avg_holdings
        stats_df.loc["Prop Rebalance", "Avg Turnover (%)"] = avg_turnover_prop
        stats_df.loc["Prop Rebalance", "Cost Drag (%)"] = total_cost_prop
        stats_df.loc["Prop Rebalance", "Tax Drag (%)"] = total_tax_prop
        stats_df.loc["Prop Rebalance", "Brokerage Drag (%)"] = total_brok_prop

    return {
        "nav": nav_df,
        "stats": stats_df,
        "holdings_log": holdings_log,
        "avg_turnover_pct": avg_turnover_full,
        "avg_turnover_pct_marg": avg_turnover_marg,
        "avg_turnover_pct_prop": avg_turnover_prop,
        "total_cost_drag_pct": total_cost_full,
        "total_cost_drag_pct_marg": total_cost_marg,
        "total_cost_drag_pct_prop": total_cost_prop,
        "total_tax_drag_pct": total_tax_full,
        "total_tax_drag_pct_marg": total_tax_marg,
        "total_tax_drag_pct_prop": total_tax_prop,
        "total_brokerage_drag_pct": total_brok_full,
        "total_brokerage_drag_pct_marg": total_brok_marg,
        "total_brokerage_drag_pct_prop": total_brok_prop,
        "rebalance_dates": rebalance_dates,
        "trading_days": trading_days,
    }


# ──────────────────────────────────────────────────────────────
# ROLLING RETURNS
# ──────────────────────────────────────────────────────────────


def rolling_returns(nav_df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    """Return rolling annualised return / CAGR (%) for all columns in nav_df."""
    simple = nav_df.pct_change(periods=window_days)
    return ((1 + simple) ** (252 / window_days) - 1) * 100
