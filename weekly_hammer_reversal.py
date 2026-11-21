"""Completed-week hammer event study. Research only, with independent signal outcomes."""

from dataclasses import dataclass

import numpy as np
import pandas as pd

PRICE_FIELDS = ["Open", "High", "Low", "Close"]
OHLCV = PRICE_FIELDS + ["Volume"]
WEEK = "W-SUN"
HORIZONS = (1, 2, 4, 8, 12, 26)
VARIANTS = tuple("ABCDE")
SEGMENTS = {
    "development": ("2016-01-01", "2022-12-31"),
    "validation": ("2023-01-01", "2024-12-31"),
    "holdout": ("2025-01-01", "2262-04-10"),
}


@dataclass(frozen=True)
class HammerGeometry:
    lower_wick_ratio: float = 2.0
    upper_wick_ratio: float = 1.0
    min_close_position: float = 0.70
    max_body_fraction: float | None = None
    require_green: bool = True


def exchange_weeks(sessions, cutoff, closed_through=None):
    """Group exchange sessions, keeping empty calendar weeks for bounded offsets."""
    sessions = pd.DatetimeIndex(sessions).sort_values().unique()
    cutoff = pd.Timestamp(cutoff)
    closed_through = cutoff if closed_through is None else pd.Timestamp(closed_through)
    weeks = pd.Series(sessions, index=sessions.to_period(WEEK)).groupby(level=0).agg(["max", "size"])
    weeks = weeks.rename(columns={"max": "signal_date", "size": "sessions"})
    weeks = weeks.reindex(pd.period_range(weeks.index.min(), cutoff.to_period(WEEK), freq=WEEK))
    weeks["complete"] = weeks.signal_date.le(cutoff) & (weeks.index.end_time.normalize() <= closed_through)
    return weeks


def membership_schedule(membership, weeks, start, end):
    """Resolve complete snapshots as of each signal close, inside the supported interval."""
    members = membership[["TIME_STAMP", "security_id"]].drop_duplicates()
    decisions = weeks.loc[weeks.complete & weeks.signal_date.between(start, end), ["signal_date"]].copy()
    decisions["week"] = decisions.index
    snapshots = members[["TIME_STAMP"]].drop_duplicates().sort_values("TIME_STAMP")
    decisions = pd.merge_asof(decisions, snapshots, left_on="signal_date", right_on="TIME_STAMP")
    return decisions.merge(members, on="TIME_STAMP", how="inner", validate="many_to_many")


def valid_daily_geometry(daily):
    prices = daily[PRICE_FIELDS]
    return (
        np.isfinite(prices).all(axis=1)
        & prices.gt(0).all(axis=1)
        & daily.High.ge(daily[["Open", "Close", "Low"]].max(axis=1))
        & daily.Low.le(daily[["Open", "Close"]].min(axis=1))
    )


def weekly_candles(daily, weeks):
    """Require a complete set of tradable, geometrically valid session candles."""
    finite_volume = np.isfinite(daily.Volume) & daily.Volume.gt(0)
    good = valid_daily_geometry(daily) & finite_volume & daily.is_tradable
    bars = (
        daily.loc[good, OHLCV]
        .groupby(daily.index[good].to_period(WEEK))
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
    )
    bars = bars.reindex(weeks.index)
    counts = good.groupby(daily.index.to_period(WEEK)).sum().reindex(weeks.index, fill_value=0)
    bars["valid_sessions"] = counts
    bars["missing_sessions"] = weeks.sessions.fillna(0).astype(int) - counts
    bars["complete_candle"] = weeks.complete & counts.eq(weeks.sessions)
    bars.loc[~bars.complete_candle, OHLCV] = np.nan
    return bars


def wilder_rsi(close, period=14):
    """Arithmetic seed over the first 14 changes, then Wilder smoothing."""
    delta = close.diff().dropna()
    rsi = pd.Series(np.nan, index=close.index)
    if len(delta) < period:
        return rsi
    gains, losses = delta.clip(lower=0), -delta.clip(upper=0)

    def smooth(values):
        seed = pd.Series([values.iloc[:period].mean()], index=values.index[period - 1 : period])
        return pd.concat([seed, values.iloc[period:]]).ewm(alpha=1 / period, adjust=False).mean()

    gain, loss = smooth(gains), smooth(losses)
    denominator = gain + loss
    rsi.loc[gain.index] = (100 * gain / denominator).where(denominator.ne(0), 50)
    return rsi


def hammer_features(bars, volume_break=(), price_break=(), geometry=HammerGeometry()):
    """Reset price features at gaps/seams. Reset the volume reference at share-count changes."""
    result = bars.copy()
    price_reset = result.index.isin(price_break)
    usable = result.complete_candle & ~price_reset
    price_block = (~usable).cumsum()
    volume_reset = ~usable | result.index.isin(volume_break)
    volume_block = volume_reset.cumsum()
    result["price_basis_flag"] = price_reset
    result["volume_basis_flag"] = result.index.isin(volume_break)
    for name in OHLCV:
        result[name] = result[name].where(usable)
    body = (result.Close - result.Open).abs()
    candle_range = result.High - result.Low
    lower = result[["Open", "Close"]].min(axis=1) - result.Low
    upper = result.High - result[["Open", "Close"]].max(axis=1)
    result["green"] = result.Close.gt(result.Open)
    result["body"] = body
    result["range"] = candle_range
    result["lower_wick"] = lower
    result["upper_wick"] = upper
    result["close_position"] = (result.Close - result.Low) / candle_range.where(candle_range.gt(0))
    result["A"] = (
        usable
        & body.gt(0)
        & candle_range.gt(0)
        & lower.ge(0)
        & upper.ge(0)
        & lower.ge(geometry.lower_wick_ratio * body)
        & upper.le(geometry.upper_wick_ratio * body)
        & result.close_position.ge(geometry.min_close_position)
    )
    if geometry.require_green:
        result["A"] &= result.green
    if geometry.max_body_fraction is not None:
        result["A"] &= body.le(candle_range * geometry.max_body_fraction)

    result["rsi"] = result.Close.groupby(price_block).transform(wilder_rsi)
    result["rsi_ema"] = result.rsi.groupby(price_block).transform(
        lambda osc: osc.ewm(span=14, adjust=False, min_periods=14).mean()
    )
    spread = result.rsi - result.rsi_ema
    result["rsi_spread"] = spread
    result["rsi_slope"] = result.rsi.diff()
    result["rsi_ema_slope"] = result.rsi_ema.diff()
    result["cross_above"] = spread.gt(0) & spread.shift().le(0)
    result["cross_below"] = spread.lt(0) & spread.shift().ge(0)
    below = spread.lt(0)
    result["weeks_below_ema"] = below.groupby((~below).cumsum()).cumsum()
    lagged_volume = (
        result.Volume.where(~volume_reset)
        .groupby(volume_block)
        .transform(lambda values: values.shift().rolling(20).mean())
    )
    result["rvol20"] = result.Volume.where(~volume_reset) / lagged_volume.where(lagged_volume.gt(0))
    lagged_high = result.High.groupby(price_block).transform(lambda values: values.shift().rolling(26).max())
    result["drawdown26"] = result.Close / lagged_high.where(lagged_high.gt(0)) - 1
    result["B"] = result.A & result.rsi.le(35)
    result["C"] = result.B & below
    result["D"] = result.C & result.rvol20.ge(1.5)
    result["E"] = result.D & result.drawdown26.le(-0.15)
    result["common_ready"] = result[["rsi", "rsi_ema", "rvol20", "drawdown26"]].notna().all(axis=1)
    return result


def evaluate_outcomes(decisions, daily, weeks, cutoff, benchmark, price_break=(), horizons=HORIZONS):
    """Execute independently in the designated entry and exit weeks, without a stale-price fallback."""
    daily = daily.loc[: pd.Timestamp(cutoff)]
    quote = daily.loc[daily.is_tradable & np.isfinite(daily.Open) & daily.Open.gt(0)]
    opens = quote.Open.groupby(quote.index.to_period(WEEK)).first()
    days = quote.index.to_series().groupby(quote.index.to_period(WEEK)).first()
    entry_week = pd.PeriodIndex(decisions.week, freq=WEEK) + 1
    entry_date = days.reindex(entry_week).to_numpy(dtype="datetime64[ns]")
    entry_price = opens.reindex(entry_week).to_numpy(dtype=float)
    entry_complete = weeks.complete.reindex(entry_week, fill_value=False).to_numpy()
    dates = daily.index.to_numpy(dtype="datetime64[ns]")
    intraday_valid = valid_daily_geometry(daily) & daily.is_tradable
    lows = daily.Low.where(intraday_valid).to_numpy(dtype=float)
    highs = daily.High.where(intraday_valid).to_numpy(dtype=float)
    boundary = decisions.segment.map({k: np.datetime64(v[1], "ns") for k, v in SEGMENTS.items()})
    boundary_week = pd.PeriodIndex(boundary, freq=WEEK)
    rows = []
    for horizon in horizons:
        exit_week = entry_week + horizon
        exit_date = days.reindex(exit_week).to_numpy(dtype="datetime64[ns]")
        exit_price = opens.reindex(exit_week).to_numpy(dtype=float)
        exit_complete = weeks.complete.reindex(exit_week, fill_value=False).to_numpy()
        crosses_segment = (exit_week > boundary_week) | (
            (exit_week == boundary_week) & (exit_date > boundary.to_numpy(dtype="datetime64[ns]"))
        )
        crosses_price = np.zeros(len(decisions), dtype=bool)
        for event in price_break:
            event = np.datetime64(event, "ns")
            crosses_price |= (entry_date < event) & (event <= exit_date)
        entry_missing = np.isnan(entry_price)
        exit_missing = np.isnan(exit_price)
        status = np.select(
            [
                crosses_segment,
                entry_missing & ~entry_complete,
                entry_missing,
                exit_missing & ~exit_complete,
                exit_missing,
                crosses_price,
            ],
            [
                "segment_censored",
                "right_censored_entry",
                "missing_entry",
                "right_censored_exit",
                "missing_exit",
                "unresolved_price_event",
            ],
            default="filled",
        )
        returns = np.where(status == "filled", exit_price / entry_price - 1, np.nan)
        mae = np.full(len(decisions), np.nan)
        mfe = mae.copy()
        excursion_sessions = np.zeros(len(decisions), dtype=int)
        for i in np.flatnonzero(status == "filled"):
            start, end = dates.searchsorted([entry_date[i], exit_date[i]])
            available = np.isfinite(lows[start:end]) & np.isfinite(highs[start:end])
            excursion_sessions[i] = available.sum()
            mae[i] = min(np.min(lows[start:end][available], initial=entry_price[i]), exit_price[i]) / entry_price[i] - 1
            mfe[i] = (
                max(np.max(highs[start:end][available], initial=entry_price[i]), exit_price[i]) / entry_price[i] - 1
            )
        bench_entry = benchmark.reindex(entry_date).to_numpy(dtype=float)
        bench_exit = benchmark.reindex(exit_date).to_numpy(dtype=float)
        benchmark_return = np.where(
            (status == "filled") & (bench_entry > 0) & (bench_exit > 0),
            bench_exit / bench_entry - 1,
            np.nan,
        )
        rows.append(
            pd.DataFrame(
                {
                    "signal_id": decisions.signal_id,
                    "holding_weeks": horizon,
                    "entry_week": entry_week,
                    "exit_week": exit_week,
                    "status": status,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_open": entry_price,
                    "exit_open": exit_price,
                    "gross_return": returns,
                    "benchmark_open_return": benchmark_return,
                    "relative_return": returns - benchmark_return,
                    "mae": mae,
                    "mfe": mfe,
                    "excursion_sessions": excursion_sessions,
                }
            )
        )
    return pd.concat(rows, ignore_index=True)
