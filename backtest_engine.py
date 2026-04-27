"""
Momentum portfolio backtester.

Entry rule : stock enters portfolio if it ranks in top-M
Exit rule  : stock leaves portfolio if it falls out of top-N  (N > M)
Rebalance  : weekly | biweekly | monthly

Two portfolio variants are tracked simultaneously:
  - Full rebalance   : every rebalance date all holdings reset to equal weight (1/size)
  - Marginal rebalance: only in/out stocks are adjusted; incumbents keep price-drifted weights

Survivorship-bias mitigations applied:
  - Historical constituent filter via compositions.parquet (only stocks in-index at each date)
  - Minimum 750 trading-day history required before a stock can be ranked
  - Stocks with > 5% missing close prices excluded (suspended / bad data)
  - Volume filter: median volume must meet MIN_VOLUME threshold
  - Transaction costs deducted at each rebalance
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from config import MIN_VOLUME
from momentum_engine import _calculate_avg_sharpe, precompute_metrics, score_momentum

# ──────────────────────────────────────────────────────────────
# UTILITY
# ──────────────────────────────────────────────────────────────


def _close_price(all_ohlcv: dict, sym: str, date: pd.Timestamp) -> float | None:
    try:
        c = all_ohlcv[sym]["Close"]
        return float(c.at[date]) if date in c.index else None
    except (KeyError, TypeError):
        return None


def _financial_year(date: pd.Timestamp) -> int:
    """India FY Apr–Mar. Returns start year: Apr 2021–Mar 2022 → 2021."""
    return date.year if date.month >= 4 else date.year - 1


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


def _valid_symbols_at_date(
    comp_df: pd.DataFrame,
    index_names: list[str],
    as_of: pd.Timestamp,
) -> set[str] | None:
    """
    Return the set of symbols that were members of the given indices on or
    before `as_of`, based on the most recent composition snapshot per index.
    Returns None when comp_df is empty (disables the filter so the backtest
    still runs without compositions data).
    """
    if comp_df is None or comp_df.empty or not index_names:
        return None

    eligible = comp_df[comp_df["INDEX_NAME"].isin(index_names) & (comp_df["TIME_STAMP"] <= as_of)]
    if eligible.empty:
        return None

    valid: set[str] = set()
    for idx_name in index_names:
        idx_rows = eligible[eligible["INDEX_NAME"] == idx_name]
        if idx_rows.empty:
            continue
        latest_ts = idx_rows["TIME_STAMP"].max()
        valid.update(idx_rows.loc[idx_rows["TIME_STAMP"] == latest_ts, "SYMBOL"])

    return valid if valid else None


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


def rank_universe_at_date(
    all_ohlcv: dict[str, pd.DataFrame],
    as_of: pd.Timestamp,
    sort_method: str,
    valid_symbols: set[str] | None = None,
    min_history_days: int = 750,
    apply_volume_filter: bool = True,
    precomputed: dict[str, pd.DataFrame] | None = None,
) -> list[str]:
    """
    Score every symbol using data up to `as_of` and return symbols ordered
    best→worst by the chosen sort_method.

    valid_symbols      : if provided, only these symbols are considered
                         (historical constituent filter — prevents survivorship bias)
    min_history_days   : minimum trading days of history required before as_of
                         (default 750 ≈ 3 years; prevents ranking on thin data)
    apply_volume_filter: if True, exclude symbols whose median volume < MIN_VOLUME
    precomputed        : pre-computed metric DataFrames from _precompute_all_metrics;
                         when provided, uses O(log n) date lookup instead of slicing OHLCV
    """
    ranked: list[tuple[str, float]] = []
    for sym, df in all_ohlcv.items():
        if valid_symbols is not None and sym not in valid_symbols:
            continue

        if precomputed is not None:
            # Fast path: O(log n) binary-search lookup in pre-computed DataFrame
            mdf = precomputed.get(sym)
            if mdf is None or mdf.empty:
                continue
            idx = mdf.index.searchsorted(as_of, side="right") - 1
            if idx < 0 or idx >= len(mdf):
                continue
            row = mdf.iloc[idx]
            if row["_count"] < min_history_days:
                continue
            if row["_missing_rate"] > 0.05:
                continue
            if apply_volume_filter:
                vol = row.get("Vol_Median")
                if pd.isna(vol) or vol < MIN_VOLUME:
                    continue
            score = _calculate_avg_sharpe(row, sort_method)
            if score is None or pd.isna(score):
                continue
        else:
            # Original path: slice OHLCV and score on demand
            sub = df[df.index <= as_of]
            if len(sub) < min_history_days:
                continue
            # Reject stocks with > 5% missing close prices (suspended / delisted mid-period)
            if sub["Close"].isna().mean() > 0.05:
                continue
            metrics = score_momentum(sub)
            if metrics is None:
                continue
            if apply_volume_filter:
                vol = metrics.get("Vol_Median")
                if vol is None or vol < MIN_VOLUME:
                    continue
            score = _calculate_avg_sharpe(metrics, sort_method)
            if score is None:
                continue

        ranked.append((sym, score))
    ranked.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in ranked]


# ──────────────────────────────────────────────────────────────
# REBALANCE DATE GENERATION
# ──────────────────────────────────────────────────────────────


def _trading_days(all_ohlcv: dict[str, pd.DataFrame], start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    """Union of all dates present in the OHLCV store within [start, end]."""
    dates: set = set()
    for df in all_ohlcv.values():
        dates.update(df.index[(df.index >= start) & (df.index <= end)].tolist())
    return pd.DatetimeIndex(sorted(dates))


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
            s = all_ohlcv[sym]["Close"].reindex(dates).ffill()
            frames[sym] = s.pct_change()
    if not frames:
        return pd.DataFrame(index=dates)
    return pd.DataFrame(frames, index=dates)


# ──────────────────────────────────────────────────────────────
# CORE BACKTEST
# ──────────────────────────────────────────────────────────────


def run_backtest(
    all_ohlcv: dict[str, pd.DataFrame],
    benchmarks: dict[str, pd.Series],
    m: int,
    n: int,
    rebalance_freq: str,
    sort_method: str,
    start_date: str,
    end_date: str,
    compositions_df: pd.DataFrame | None = None,
    index_names: list[str] | None = None,
    transaction_cost_pct: float = 0.001,
    min_history_days: int = 750,
    apply_volume_filter: bool = True,
    band_rule: str = "classic",
    brokerage_per_sale: float = 0.0,
    initial_capital: float = 1_000_000.0,
    ltcg_rate: float = 0.0,
    stcg_rate: float = 0.0,
) -> dict:
    """
    Run both portfolio variants and return NAV series + summary stats.

    Parameters
    ----------
    all_ohlcv            : symbol → OHLCV DataFrame (full history, index = DatetimeIndex)
    benchmarks           : label  → close price Series (e.g. 'NIFTY50', 'NIFTY500')
    m                    : enter portfolio if ranked ≤ m  (1-based)
    n                    : exit  portfolio if ranked >  n  (n > m)
    rebalance_freq       : 'weekly' | 'biweekly' | 'monthly' | 'quarterly' | 'half-yearly'
    sort_method          : passed to _calculate_avg_sharpe
    start_date           : 'YYYY-MM-DD'
    end_date             : 'YYYY-MM-DD'
    compositions_df      : historical index compositions from load_compositions()
                           used to restrict the universe to stocks that were actually
                           in-index on each rebalance date (eliminates survivorship bias)
    index_names          : list of index names to use for composition lookup
                           (e.g. ['NIFTY 50', 'NIFTY NEXT 50'])
    transaction_cost_pct : one-way cost per trade as a fraction of traded value
                           (default 0.001 = 0.1%)
    min_history_days     : minimum trading-day history required before a stock
                           can be ranked (default 750 ≈ 3 years)
    apply_volume_filter  : exclude stocks with median volume < MIN_VOLUME
    band_rule            : 'classic'      — exit if rank > N, enter if rank ≤ M (may exceed M)
                           'displacement' — N is WRH (Worst Rank Held): rank > N exits
                                            unconditionally; rank M+1..N incumbents are a
                                            buffer zone and stay until their rank exceeds N;
                                            new top-M entrants only fill slots freed by WRH
                                            exits — they do not displace buffer incumbents
    brokerage_per_sale   : flat brokerage in INR charged per stock sold (exits only); 0 = disabled
    initial_capital      : portfolio size in INR used to convert flat Rs brokerage → NAV drag
    ltcg_rate            : long-term capital gains tax rate (fraction) applied to gains on
                           holdings held > 12 calendar months (e.g. 0.125 for 12.5%)
    stcg_rate            : short-term capital gains tax rate (fraction) applied to gains on
                           holdings held ≤ 12 calendar months (e.g. 0.20 for 20%)
    """
    t0 = pd.Timestamp(start_date)
    t1 = pd.Timestamp(end_date)

    trading_days = _trading_days(all_ohlcv, t0, t1)
    if len(trading_days) < 20:
        return {"error": "Insufficient trading days in selected range (need at least one month of data)."}

    rebalance_dates = get_rebalance_dates(trading_days, rebalance_freq)
    # Exclude the first trading day: we need T-1 close to rank without look-ahead bias.
    rebalance_set = set(rebalance_dates) - {trading_days[0]}

    # Pre-compute rolling metrics once per symbol (O(symbols)) instead of per rebalance date
    precomputed = _precompute_all_metrics(all_ohlcv)

    # ── initialise portfolios ──
    full_weights: dict[str, float] = {}
    full_weights_prev: dict[str, float] = {}  # drift-adjusted full weights from prior rebalance
    marg_weights: dict[str, float] = {}
    current_holdings: set[str] = set()
    prev_rebalance_day = None  # needed to drift-adjust weights at each rebalance

    nav_full = 100.0
    nav_marg = 100.0

    nav_records: list[dict] = []
    holdings_log: list[dict] = []
    turnover_log_full: list[float] = []
    turnover_log_marg: list[float] = []
    cost_log_full: list[float] = []
    cost_log_marg: list[float] = []
    holdings_sizes: list[int] = []

    entry_prices: dict[str, float] = {}
    entry_dates: dict[str, pd.Timestamp] = {}
    tax_log_full: list[float] = []
    tax_log_marg: list[float] = []
    brok_log_full: list[float] = []
    brok_log_marg: list[float] = []

    # FY-level CGT accumulators (reset each new FY)
    current_fy: int | None = None
    fy_st_g_full = fy_st_l_full = fy_lt_g_full = fy_lt_l_full = 0.0
    fy_st_g_marg = fy_st_l_marg = fy_lt_g_marg = fy_lt_l_marg = 0.0
    # Carry-forward loss buckets: list of (expiry_fy, amount)
    cf_st_full: list[tuple[int, float]] = []
    cf_lt_full: list[tuple[int, float]] = []
    cf_st_marg: list[tuple[int, float]] = []
    cf_lt_marg: list[tuple[int, float]] = []

    for i, day in enumerate(trading_days):
        # ── rebalance ──
        if day in rebalance_set:
            # Restrict universe to historically valid members on this date
            valid_syms = _valid_symbols_at_date(compositions_df, index_names or [], day)

            # Rank using previous day's data to avoid look-ahead bias:
            # rankings are determined from T-1 close; trades execute at T close.
            rank_as_of = trading_days[i - 1] if i > 0 else day
            ranked = rank_universe_at_date(
                all_ohlcv,
                rank_as_of,
                sort_method,
                valid_symbols=valid_syms,
                min_history_days=min_history_days,
                apply_volume_filter=apply_volume_filter,
                precomputed=precomputed,
            )
            top_m = set(ranked[:m])
            top_n = set(ranked[:n])

            if band_rule == "displacement":
                # N = WRH (Worst Rank Held): no stock ranked > N may be held — hard cap.
                # Stocks ranked M+1..N are a buffer zone: they stay until their rank
                # exceeds N, at which point they exit and a top-M entrant fills the slot.
                # A new top-M stock may ONLY enter through a slot freed by a WRH exit;
                # it does not actively displace buffer-zone incumbents ranked M+1..N.
                #
                # rank_of gives O(1) lookup; stocks absent from `ranked` this period
                # (delisted / data gap) get rank=len(ranked) — worst possible, so they
                # are first in line to exit and last in line to enter.
                rank_of = {s: i for i, s in enumerate(ranked)}
                _worst = len(ranked)

                # Step 1 — WRH exits (unconditional): rank > N must leave
                wrh_exits = current_holdings - top_n
                holdings_after_wrh = current_holdings - wrh_exits

                # Step 2 — fill free slots opened by WRH exits with best top-M entrants
                entries_wanted = sorted(top_m - holdings_after_wrh, key=lambda s: rank_of.get(s, _worst))
                free_slots = max(0, m - len(holdings_after_wrh))
                entries = set(entries_wanted[:free_slots])

                exits = wrh_exits
            else:
                # classic: exit if rank > N, enter if rank ≤ M (may briefly exceed M)
                exits = current_holdings - top_n
                entries = top_m - current_holdings

            new_holdings = (current_holdings - exits) | entries

            if not new_holdings:
                new_holdings = top_m if top_m else current_holdings

            size = len(new_holdings)
            holdings_sizes.append(size)

            # ── drift-adjust both weight trackers to reflect price movement since last rebalance ──
            # This must happen before turnover calculations so exit weights use current market values.
            if prev_rebalance_day is not None:

                def _drift_weights(weights: dict[str, float]) -> dict[str, float]:
                    if not weights:
                        return weights
                    drifted: dict[str, float] = {}
                    for s, w in weights.items():
                        try:
                            c = all_ohlcv[s]["Close"]
                            p_prev = float(c.at[prev_rebalance_day]) if prev_rebalance_day in c.index else None
                            p_now = float(c.at[day]) if day in c.index else None
                            drifted[s] = w * (p_now / p_prev) if (p_prev and p_now and p_prev > 0) else w
                        except (KeyError, TypeError, ZeroDivisionError):
                            drifted[s] = w
                    total_d = sum(drifted.values())
                    return {s: w / total_d for s, w in drifted.items()} if total_d > 0 else drifted

                full_weights_prev = _drift_weights(full_weights_prev)
                marg_weights = _drift_weights(marg_weights)

            # ── weight-based turnover: separate for full and marginal ──
            # Marginal: only exits are sold and entries bought; incumbents untouched.
            traded_w_marg = sum(marg_weights.get(s, 0.0) for s in exits) + sum(1.0 / size for s in entries)
            turnover_log_marg.append(traded_w_marg)

            # Full: exits + entries + incumbents rebalanced back to 1/M.
            traded_w_full = (
                sum(full_weights_prev.get(s, 0.0) for s in exits)
                + sum(1.0 / size for s in entries)
                + sum(abs(full_weights_prev.get(s, 0.0) - 1.0 / size) for s in (new_holdings - entries))
            )
            turnover_log_full.append(traded_w_full)

            # ── transaction cost drag — separate for full and marginal ──
            if i > 0 and transaction_cost_pct > 0 and size > 0:
                cost_drag_full = traded_w_full * transaction_cost_pct
                cost_drag_marg = traded_w_marg * transaction_cost_pct
                nav_full *= 1.0 - cost_drag_full
                nav_marg *= 1.0 - cost_drag_marg
                cost_log_full.append(cost_drag_full)
                cost_log_marg.append(cost_drag_marg)

            # ── flat brokerage per sale (exits only, no charge on buys) ──
            if i > 0 and brokerage_per_sale > 0 and initial_capital > 0 and exits:
                n_exits = len(exits)
                brok_drag_full = (brokerage_per_sale * n_exits) / (initial_capital * nav_full / 100.0)
                brok_drag_marg = (brokerage_per_sale * n_exits) / (initial_capital * nav_marg / 100.0)
                nav_full *= 1.0 - brok_drag_full
                nav_marg *= 1.0 - brok_drag_marg
                brok_log_full.append(brok_drag_full)
                brok_log_marg.append(brok_drag_marg)
            else:
                brok_log_full.append(0.0)
                brok_log_marg.append(0.0)

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
                    drag_full = tax_full / nav_full if nav_full > 0 else 0.0
                    drag_marg = tax_marg / nav_marg if nav_marg > 0 else 0.0
                    nav_full *= 1.0 - drag_full
                    nav_marg *= 1.0 - drag_marg
                    tax_log_full.append(drag_full)
                    tax_log_marg.append(drag_marg)
                    # Reset FY accumulators
                    fy_st_g_full = fy_st_l_full = fy_lt_g_full = fy_lt_l_full = 0.0
                    fy_st_g_marg = fy_st_l_marg = fy_lt_g_marg = fy_lt_l_marg = 0.0
                    current_fy = day_fy
                elif current_fy is None:
                    current_fy = day_fy

                # ── Accumulate this rebalance's realised gains/losses into FY buckets ──
                if i > 0 and exits:
                    for sym in exits:
                        ep = entry_prices.get(sym)
                        ed = entry_dates.get(sym)
                        if not ep or ep <= 0 or ed is None:
                            continue
                        xp = _close_price(all_ohlcv, sym, day)
                        if xp is None:
                            continue
                        gain_pct = xp / ep - 1.0
                        denom = 1.0 + gain_pct
                        if denom == 0:
                            continue
                        is_long_term = day > ed + relativedelta(months=12)

                        # Full variant
                        pos_full = nav_full * full_weights_prev.get(sym, 0.0)
                        gain_full = pos_full * gain_pct / denom
                        if is_long_term:
                            if gain_full >= 0:
                                fy_lt_g_full += gain_full
                            else:
                                fy_lt_l_full += abs(gain_full)
                        else:
                            if gain_full >= 0:
                                fy_st_g_full += gain_full
                            else:
                                fy_st_l_full += abs(gain_full)

                        # Marginal variant
                        pos_marg = nav_marg * marg_weights.get(sym, 0.0)
                        gain_marg = pos_marg * gain_pct / denom
                        if is_long_term:
                            if gain_marg >= 0:
                                fy_lt_g_marg += gain_marg
                            else:
                                fy_lt_l_marg += abs(gain_marg)
                        else:
                            if gain_marg >= 0:
                                fy_st_g_marg += gain_marg
                            else:
                                fy_st_l_marg += abs(gain_marg)

            # ── record entry prices/dates for new entrants; clear exits ──
            for sym in entries:
                p = _close_price(all_ohlcv, sym, day)
                if p is not None:
                    entry_prices[sym] = p
                    entry_dates[sym] = day
            for sym in exits:
                entry_prices.pop(sym, None)
                entry_dates.pop(sym, None)

            # full rebalance: equal weight all holdings
            full_weights = {s: 1.0 / size for s in new_holdings}
            full_weights_prev = dict(full_weights)  # store for next rebalance's drift-adjust & turnover

            # marginal rebalance: redistribute only exited weight to entrants.
            # marg_weights is already drift-adjusted above, so freed weight reflects current prices.
            freed = sum(marg_weights.get(s, 0.0) for s in exits)
            new_marg = {s: marg_weights[s] for s in new_holdings - entries if s in marg_weights}
            if entries:
                per_entry = (freed / len(entries)) if freed > 0 else (1.0 / size)
                for s in entries:
                    new_marg[s] = per_entry
            # if portfolio was empty before, seed equal weight
            if not new_marg:
                new_marg = {s: 1.0 / size for s in new_holdings}
            # normalise so weights sum to 1
            total_w = sum(new_marg.values())
            if total_w > 0:
                new_marg = {s: w / total_w for s, w in new_marg.items()}
            marg_weights = new_marg

            prev_rebalance_day = day
            current_holdings = new_holdings
            holdings_log.append(
                {
                    "date": day,
                    "holdings": sorted(current_holdings),
                    "entries": sorted(entries),
                    "exits": sorted(exits),
                    "valid_universe_size": len(valid_syms) if valid_syms else len(all_ohlcv),
                }
            )

        # ── daily NAV update ──
        if i > 0 and current_holdings:
            prev_day = trading_days[i - 1]
            port_ret_full = 0.0
            port_ret_marg = 0.0
            for sym in current_holdings:
                if sym not in all_ohlcv:
                    continue
                closes = all_ohlcv[sym]["Close"]
                if day not in closes.index or prev_day not in closes.index:
                    continue
                r = closes[day] / closes[prev_day] - 1
                if pd.isna(r):
                    continue
                port_ret_full += full_weights.get(sym, 0.0) * r
                port_ret_marg += marg_weights.get(sym, 0.0) * r
            nav_full *= 1 + port_ret_full
            nav_marg *= 1 + port_ret_marg

        nav_records.append({"Date": day, "Full Rebalance": nav_full, "Marginal Rebalance": nav_marg})

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
        drag_full = tax_full / nav_full if nav_full > 0 else 0.0
        drag_marg = tax_marg / nav_marg if nav_marg > 0 else 0.0
        nav_full *= 1.0 - drag_full
        nav_marg *= 1.0 - drag_marg
        tax_log_full.append(drag_full)
        tax_log_marg.append(drag_marg)
        # Update final row in nav_records to reflect post-tax NAV
        if nav_records:
            nav_records[-1]["Full Rebalance"] = nav_full
            nav_records[-1]["Marginal Rebalance"] = nav_marg
        nav_df = pd.DataFrame(nav_records).set_index("Date")

    # ── attach benchmarks ──
    for label, series in benchmarks.items():
        s = series.reindex(trading_days).ffill(limit=5).dropna()
        if s.empty:
            continue
        nav_df[label] = (s / s.iloc[0]) * 100

    # ── stats ──
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

    stats_df = pd.DataFrame(stats).T

    avg_turnover_full = round(np.mean(turnover_log_full) * 100, 1) if turnover_log_full else 0.0
    avg_turnover_marg = round(np.mean(turnover_log_marg) * 100, 1) if turnover_log_marg else 0.0
    total_cost_full = round(sum(cost_log_full) * 100, 3) if cost_log_full else 0.0
    total_cost_marg = round(sum(cost_log_marg) * 100, 3) if cost_log_marg else 0.0
    avg_holdings = round(float(np.mean(holdings_sizes)), 1) if holdings_sizes else 0.0
    total_tax_full = round(sum(tax_log_full) * 100, 3) if tax_log_full else 0.0
    total_tax_marg = round(sum(tax_log_marg) * 100, 3) if tax_log_marg else 0.0
    total_brok_full = round(sum(brok_log_full) * 100, 3) if brok_log_full else 0.0
    total_brok_marg = round(sum(brok_log_marg) * 100, 3) if brok_log_marg else 0.0

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

    return {
        "nav": nav_df,
        "stats": stats_df,
        "holdings_log": holdings_log,
        "avg_turnover_pct": avg_turnover_full,
        "total_cost_drag_pct": total_cost_full,
        "total_tax_drag_pct": total_tax_full,
        "total_brokerage_drag_pct": total_brok_full,
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
