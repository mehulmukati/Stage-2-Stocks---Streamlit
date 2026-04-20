"""
Momentum portfolio backtester.

Entry rule : stock enters portfolio if it ranks in top-M
Exit rule  : stock leaves portfolio if it falls out of top-N  (N > M)
Rebalance  : weekly | biweekly | monthly

Two portfolio variants are tracked simultaneously:
  - Full rebalance   : every rebalance date all holdings reset to equal weight (1/size)
  - Marginal rebalance: only in/out stocks are adjusted; incumbents keep price-drifted weights
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from momentum_engine import _calculate_avg_sharpe, score_momentum


# ──────────────────────────────────────────────────────────────
# RANKING
# ──────────────────────────────────────────────────────────────

def rank_universe_at_date(
    all_ohlcv: dict[str, pd.DataFrame],
    as_of: pd.Timestamp,
    sort_method: str,
) -> list[str]:
    """
    Score every symbol using data up to `as_of` and return symbols ordered
    best→worst by the chosen sort_method.  Symbols with insufficient data are excluded.
    """
    ranked: list[tuple[str, float]] = []
    for sym, df in all_ohlcv.items():
        sub = df[df.index <= as_of]
        if len(sub) < 250:
            continue
        metrics = score_momentum(sub)
        if metrics is None:
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
) -> dict:
    """
    Run both portfolio variants and return NAV series + summary stats.

    Parameters
    ----------
    all_ohlcv      : symbol → OHLCV DataFrame (full history, index = DatetimeIndex)
    benchmarks     : label  → close price Series (e.g. 'NIFTY50', 'NIFTY500')
    m              : enter portfolio if ranked ≤ m  (1-based)
    n              : exit  portfolio if ranked >  n  (n > m)
    rebalance_freq : 'weekly' | 'biweekly' | 'monthly'
    sort_method    : passed to _calculate_avg_sharpe
    start_date     : 'YYYY-MM-DD'
    end_date       : 'YYYY-MM-DD'
    """
    t0 = pd.Timestamp(start_date)
    t1 = pd.Timestamp(end_date)

    trading_days = _trading_days(all_ohlcv, t0, t1)
    if len(trading_days) < 5:
        return {"error": "Insufficient trading days in selected range."}

    rebalance_dates = get_rebalance_dates(trading_days, rebalance_freq)
    rebalance_set = set(rebalance_dates)

    # ── initialise portfolios ──
    full_weights: dict[str, float] = {}
    marg_weights: dict[str, float] = {}
    current_holdings: set[str] = set()

    nav_full = 100.0
    nav_marg = 100.0

    nav_records: list[dict] = []
    holdings_log: list[dict] = []
    turnover_log: list[float] = []

    for i, day in enumerate(trading_days):
        # ── rebalance ──
        if day in rebalance_set:
            ranked = rank_universe_at_date(all_ohlcv, day, sort_method)
            top_m = set(ranked[:m])
            top_n = set(ranked[:n])

            # determine new holdings via band rule
            exits = current_holdings - top_n          # held but fell out of top-N
            entries = top_m - current_holdings        # not held but now in top-M
            new_holdings = (current_holdings - exits) | entries

            if not new_holdings:
                new_holdings = top_m if top_m else current_holdings

            size = len(new_holdings)
            turnover = (len(exits) + len(entries)) / max(len(current_holdings), 1) if current_holdings else 1.0
            turnover_log.append(turnover)

            # full rebalance: equal weight all holdings
            full_weights = {s: 1.0 / size for s in new_holdings}

            # marginal rebalance: redistribute only exited weight to entrants
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

            current_holdings = new_holdings
            holdings_log.append({"date": day, "holdings": sorted(current_holdings), "entries": sorted(entries), "exits": sorted(exits)})

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
        cagr = (s.iloc[-1] / s.iloc[0]) ** (252 / n_days) - 1
        sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() > 0 else np.nan
        rolling_max = s.cummax()
        drawdown = (s - rolling_max) / rolling_max
        max_dd = drawdown.min()
        stats[col] = {
            "CAGR (%)": round(cagr * 100, 2),
            "Sharpe": round(sharpe, 3),
            "Max Drawdown (%)": round(max_dd * 100, 2),
            "Final NAV": round(s.iloc[-1], 2),
        }

    stats_df = pd.DataFrame(stats).T

    # avg turnover only for portfolio variants
    avg_turnover = round(np.mean(turnover_log) * 100, 1) if turnover_log else 0.0

    return {
        "nav": nav_df,
        "stats": stats_df,
        "holdings_log": holdings_log,
        "avg_turnover_pct": avg_turnover,
        "rebalance_dates": rebalance_dates,
        "trading_days": trading_days,
    }


# ──────────────────────────────────────────────────────────────
# ROLLING RETURNS
# ──────────────────────────────────────────────────────────────

def rolling_returns(nav_df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    """Return rolling N-day return (%) for all columns in nav_df."""
    return nav_df.pct_change(periods=window_days) * 100
