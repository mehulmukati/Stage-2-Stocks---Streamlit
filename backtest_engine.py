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

import numpy as np
import pandas as pd

from config import MIN_VOLUME
from momentum_engine import _calculate_avg_sharpe, precompute_metrics, score_momentum


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

    eligible = comp_df[
        comp_df["INDEX_NAME"].isin(index_names) & (comp_df["TIME_STAMP"] <= as_of)
    ]
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
        except Exception:
            pass
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
            idx = mdf.index.searchsorted(as_of, side='right') - 1
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
      'weekly'   – last trading day of each calendar week
      'biweekly' – last trading day of every other calendar week
      'monthly'  – last trading day of each calendar month
    """
    if trading_days.empty:
        return []

    series = pd.Series(trading_days, index=trading_days)

    if freq == "monthly":
        grouped = series.groupby([series.dt.year, series.dt.month])
        return [grp.iloc[-1] for _, grp in grouped]

    # week number per year
    week_key = trading_days.isocalendar().week.values
    year_key = trading_days.isocalendar().year.values

    dates_df = pd.DataFrame(
        {"date": trading_days, "year": year_key, "week": week_key}
    )
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
) -> dict:
    """
    Run both portfolio variants and return NAV series + summary stats.

    Parameters
    ----------
    all_ohlcv            : symbol → OHLCV DataFrame (full history, index = DatetimeIndex)
    benchmarks           : label  → close price Series (e.g. 'NIFTY50', 'NIFTY500')
    m                    : enter portfolio if ranked ≤ m  (1-based)
    n                    : exit  portfolio if ranked >  n  (n > m)
    rebalance_freq       : 'weekly' | 'biweekly' | 'monthly'
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
                           'displacement' — exit only when displaced by a rank-≤M entrant;
                                            portfolio size is always ≤ M
    """
    t0 = pd.Timestamp(start_date)
    t1 = pd.Timestamp(end_date)

    trading_days = _trading_days(all_ohlcv, t0, t1)
    if len(trading_days) < 5:
        return {"error": "Insufficient trading days in selected range."}

    rebalance_dates = get_rebalance_dates(trading_days, rebalance_freq)
    rebalance_set = set(rebalance_dates)

    # Pre-compute rolling metrics once per symbol (O(symbols)) instead of per rebalance date
    precomputed = _precompute_all_metrics(all_ohlcv)

    # ── initialise portfolios ──
    full_weights: dict[str, float] = {}
    marg_weights: dict[str, float] = {}
    current_holdings: set[str] = set()
    prev_rebalance_day = None   # needed to drift-adjust marg_weights at each rebalance

    nav_full = 100.0
    nav_marg = 100.0

    nav_records: list[dict] = []
    holdings_log: list[dict] = []
    turnover_log: list[float] = []
    cost_log: list[float] = []
    holdings_sizes: list[int] = []

    for i, day in enumerate(trading_days):
        # ── rebalance ──
        if day in rebalance_set:
            # Restrict universe to historically valid members on this date
            valid_syms = _valid_symbols_at_date(compositions_df, index_names or [], day)

            # Rank using previous day's data to avoid look-ahead bias:
            # rankings are determined from T-1 close; trades execute at T close.
            rank_as_of = trading_days[i - 1] if i > 0 else day
            ranked = rank_universe_at_date(
                all_ohlcv, rank_as_of, sort_method,
                valid_symbols=valid_syms,
                min_history_days=min_history_days,
                apply_volume_filter=apply_volume_filter,
                precomputed=precomputed,
            )
            top_m = set(ranked[:m])
            top_n = set(ranked[:n])

            if band_rule == "displacement":
                # Exit only when displaced by a rank-≤M entrant; portfolio stays ≤ M.
                #
                # Build a rank-lookup dict so we never call list.index() on a symbol
                # that disappeared from `ranked` this period (e.g. delisted stock,
                # data gap, failed volume filter). Such a stock gets rank=len(ranked)
                # (worst possible) so it is treated as most-eligible for exit and
                # least-eligible for entry — the correct behaviour.
                rank_of = {s: i for i, s in enumerate(ranked)}
                _worst = len(ranked)
                entries_wanted = sorted(
                    top_m - current_holdings, key=lambda s: rank_of.get(s, _worst)
                )
                exits_eligible = sorted(
                    current_holdings - top_n,
                    key=lambda s: rank_of.get(s, _worst),
                    reverse=True,
                )
                free_slots = max(0, m - len(current_holdings))
                entries = set(entries_wanted[:free_slots])
                exits: set[str] = set()
                for _i, stock in enumerate(entries_wanted[free_slots:]):
                    if _i < len(exits_eligible):
                        entries.add(stock)
                        exits.add(exits_eligible[_i])
            else:
                # classic: exit if rank > N, enter if rank ≤ M (may briefly exceed M)
                exits = current_holdings - top_n
                entries = top_m - current_holdings

            new_holdings = (current_holdings - exits) | entries

            if not new_holdings:
                new_holdings = top_m if top_m else current_holdings

            size = len(new_holdings)
            holdings_sizes.append(size)
            traded = len(exits) + len(entries)
            turnover = traded / max(len(current_holdings), 1) if current_holdings else 1.0
            turnover_log.append(turnover)

            # ── transaction cost drag ──
            # Deduct cost proportional to the fraction of portfolio traded.
            # traded_fraction = (exits + entries) / portfolio_size (one-way)
            if i > 0 and transaction_cost_pct > 0 and size > 0:
                traded_fraction = traded / size
                cost_drag = traded_fraction * transaction_cost_pct
                nav_full *= (1.0 - cost_drag)
                nav_marg *= (1.0 - cost_drag)
                cost_log.append(cost_drag)

            # full rebalance: equal weight all holdings
            full_weights = {s: 1.0 / size for s in new_holdings}

            # marginal rebalance: redistribute only exited weight to entrants.
            #
            # Before computing freed weight, drift-adjust marg_weights to reflect
            # price movement since the previous rebalance.  Without this, all
            # incumbent weights stay at their last-rebalance value (1/M), so
            # freed = exits/M and per_entry = 1/M — every stock lands back at 1/M
            # and Marginal collapses to Full.  With drift adjustment, winners carry
            # higher weight into the redistribution, making the two strategies
            # genuinely distinct.
            if marg_weights and prev_rebalance_day is not None:
                drifted: dict[str, float] = {}
                for s, w in marg_weights.items():
                    try:
                        c = all_ohlcv[s]["Close"]
                        p_prev = float(c.at[prev_rebalance_day]) if prev_rebalance_day in c.index else None
                        p_now  = float(c.at[day])               if day  in c.index else None
                        drifted[s] = w * (p_now / p_prev) if (p_prev and p_now and p_prev > 0) else w
                    except Exception:
                        drifted[s] = w
                total_d = sum(drifted.values())
                if total_d > 0:
                    marg_weights = {s: w / total_d for s, w in drifted.items()}

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
            holdings_log.append({
                "date": day,
                "holdings": sorted(current_holdings),
                "entries": sorted(entries),
                "exits": sorted(exits),
                "valid_universe_size": len(valid_syms) if valid_syms else len(all_ohlcv),
            })

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
                port_ret_full += full_weights.get(sym, 0.0) * r
                port_ret_marg += marg_weights.get(sym, 0.0) * r
            nav_full *= (1 + port_ret_full)
            nav_marg *= (1 + port_ret_marg)

        nav_records.append({"Date": day, "Full Rebalance": nav_full, "Marginal Rebalance": nav_marg})

    nav_df = pd.DataFrame(nav_records).set_index("Date")

    # ── attach benchmarks ──
    for label, series in benchmarks.items():
        s = series.reindex(trading_days).ffill().dropna()
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
        sortino = (daily_ret.mean() / neg_ret.std() * np.sqrt(252)) if len(neg_ret) > 1 and neg_ret.std() > 0 else np.nan
        stats[col] = {
            "CAGR (%)": round(cagr * 100, 2),
            "Sharpe": round(float(sharpe), 3) if not np.isnan(sharpe) else np.nan,
            "Max Drawdown (%)": round(max_dd * 100, 2),
            "Calmar": round(float(calmar), 3) if not np.isnan(calmar) else np.nan,
            "Sortino": round(float(sortino), 3) if not np.isnan(sortino) else np.nan,
            "Final NAV": round(s.iloc[-1], 2),
        }

    stats_df = pd.DataFrame(stats).T

    avg_turnover = round(np.mean(turnover_log) * 100, 1) if turnover_log else 0.0
    total_cost_drag = round(sum(cost_log) * 100, 3) if cost_log else 0.0
    avg_holdings = round(float(np.mean(holdings_sizes)), 1) if holdings_sizes else 0.0

    for col in ["Full Rebalance", "Marginal Rebalance"]:
        if col in stats_df.index:
            stats_df.loc[col, "Avg Holdings"] = avg_holdings
            stats_df.loc[col, "Avg Turnover (%)"] = avg_turnover
            stats_df.loc[col, "Cost Drag (%)"] = total_cost_drag

    return {
        "nav": nav_df,
        "stats": stats_df,
        "holdings_log": holdings_log,
        "avg_turnover_pct": avg_turnover,
        "total_cost_drag_pct": total_cost_drag,
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
