"""Separate retained observations, trusted session prices and evidence of trading."""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

EQUITY_POLICY_VERSION = 2
RECENT_SESSION_WINDOW = 252
MIN_RECENT_COVERAGE = 0.95
ROOT = Path(__file__).resolve().parent
QUALITY_COLUMNS = [
    "observation_status",
    "price_status",
    "volume_status",
    "is_tradable",
    "verification_source",
    "verified_at",
]


@lru_cache(maxsize=1)
def calendar_definition() -> dict:
    return json.loads((ROOT / "data" / "nse_session_calendar.json").read_text(encoding="utf-8"))


def calendar_provenance() -> dict:
    content = (ROOT / "data" / "nse_session_calendar.json").read_bytes()
    definition = calendar_definition()
    return {
        "version": definition["version"],
        "sha256": hashlib.sha256(content).hexdigest(),
        "verified_years": definition["verified_years"],
        "sources": definition["sources"],
        "historical_fallback": definition["historical_fallback"],
    }


def _cash_holidays() -> frozenset:
    return frozenset(pd.to_datetime(calendar_definition()["closed_sessions"]))


def holiday_placeholder_mask(frame: pd.DataFrame) -> pd.Series:
    """Only non-trading rows on independently verified complete closures."""
    dates = pd.to_datetime(frame["date"] if "date" in frame else frame.index)
    volume = pd.to_numeric(frame["Volume"], errors="coerce")
    return pd.Series(dates.isin(_cash_holidays()), index=frame.index) & (volume.eq(0) | volume.isna())


def classify_equity_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Retain uncertain quotes with explicit quality; never guess no-trade marks."""
    result = frame.copy()
    close = pd.to_numeric(result["Close"], errors="coerce")
    volume = pd.to_numeric(result["Volume"], errors="coerce")
    valid_price = np.isfinite(close) & close.gt(0)
    traded = valid_price & np.isfinite(volume) & volume.gt(0)
    source = result.get("verification_source", pd.Series("", index=result.index)).fillna("")
    verified_at = result.get("verified_at", pd.Series("", index=result.index)).fillna("")
    previous_status = result.get("price_status", pd.Series("", index=result.index))
    verified_mark = (
        valid_price & volume.eq(0) & previous_status.eq("verified_mark") & source.ne("") & verified_at.ne("")
    )
    result["observation_status"] = pd.Categorical(
        np.select(
            [traded, verified_mark, valid_price & volume.eq(0), valid_price & volume.isna()],
            ["traded", "verified_no_trade", "unverified_zero_volume", "unknown_volume"],
            default="invalid",
        )
    )
    result["price_status"] = pd.Categorical(
        np.select([traded, verified_mark], ["observed", "verified_mark"], default="unverified"),
        categories=["observed", "verified_mark", "unverified"],
    )
    result["volume_status"] = pd.Categorical(
        np.select(
            [np.isfinite(volume) & volume.gt(0), volume.eq(0), volume.isna()],
            ["reported_positive", "reported_zero", "unknown"],
            default="invalid",
        )
    )
    result["is_tradable"] = traded.astype(bool)
    result["verification_source"] = source
    result["verified_at"] = verified_at
    closed = pd.Series(
        pd.to_datetime(result["date"] if "date" in result else result.index).isin(_cash_holidays()), index=result.index
    )
    result["observation_status"] = result.observation_status.cat.add_categories(["calendar_conflict"])
    result.loc[closed, "observation_status"] = "calendar_conflict"
    result.loc[closed, "price_status"] = "unverified"
    result.loc[closed, "is_tradable"] = False
    return result


def accepted_equity_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Retained observations, including genuine or uncertain zero volume."""
    result = frame.copy() if set(QUALITY_COLUMNS).issubset(frame.columns) else classify_equity_rows(frame)
    close = pd.to_numeric(result["Close"], errors="coerce")
    return result.loc[np.isfinite(close) & close.gt(0) & ~holiday_placeholder_mask(result)].copy()


def tradable_equity_rows(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame if set(QUALITY_COLUMNS).issubset(frame.columns) else classify_equity_rows(frame)
    return result.loc[result.is_tradable].copy()


def observed_sessions(frames) -> pd.DatetimeIndex:
    indices = [tradable_equity_rows(frame).index for frame in frames]
    return (
        pd.DatetimeIndex(np.concatenate(indices)).normalize().sort_values().unique()
        if indices
        else pd.DatetimeIndex([])
    )


def session_calendar(observed: pd.DatetimeIndex, through=None) -> pd.DatetimeIndex:
    """Verified yearly sessions plus an explicitly identified historical fallback."""
    calendar = pd.DatetimeIndex(observed).normalize().sort_values().unique()
    if calendar.empty:
        return calendar
    end = pd.Timestamp(through) if through is not None else calendar[-1]
    start = calendar[0]
    definition = calendar_definition()
    for year in definition["verified_years"]:
        lo, hi = max(start, pd.Timestamp(year=year, month=1, day=1)), min(
            end, pd.Timestamp(year=year, month=12, day=31)
        )
        if lo <= hi:
            calendar = calendar.union(pd.bdate_range(lo, hi))
    special = pd.DatetimeIndex(pd.to_datetime(definition["special_sessions"]))
    calendar = calendar.union(special[(special >= start) & (special <= end)])
    if end > calendar[-1]:
        calendar = calendar.union(pd.bdate_range(calendar[-1] + pd.Timedelta(days=1), end))
    return calendar[(calendar <= end) & ~calendar.isin(_cash_holidays())].sort_values()


def extend_sessions(calendar: pd.DatetimeIndex, through) -> pd.DatetimeIndex:
    return session_calendar(calendar, through)


def session_frame(frame: pd.DataFrame, calendar=None, as_of=None) -> pd.DataFrame:
    """Fixed session grid with missing/unverified prices kept missing.

    Verified no-trade marks supply only Close and known zero volume. No candle
    or execution price is fabricated. Stored observations are never modified.
    """
    observations = accepted_equity_rows(frame).sort_index()
    if observations.empty:
        return observations
    end = (
        pd.Timestamp(as_of)
        if as_of is not None
        else (pd.DatetimeIndex(calendar)[-1] if calendar is not None and len(calendar) else observations.index[-1])
    )
    sessions = session_calendar(observations.index, end) if calendar is None else pd.DatetimeIndex(calendar)
    sessions = sessions[(sessions >= observations.index[0]) & (sessions <= end)]
    result = observations.reindex(sessions).copy()
    usable = result["price_status"].isin(["observed", "verified_mark"])
    traded = result["is_tradable"].fillna(False).astype(bool)
    result["Close"] = result["Close"].where(usable)
    for column in ("Open", "High", "Low"):
        if column in result:
            result[column] = result[column].where(traded)
    result["is_tradable"] = traded
    result.index = pd.DatetimeIndex(result.index.to_numpy(), name=frame.index.name)
    result.attrs.update(frame.attrs)
    result.attrs["equity_policy_version"] = EQUITY_POLICY_VERSION
    result.attrs["session_grid"] = True
    return result


def recent_coverage(
    dates: pd.DatetimeIndex,
    as_of,
    calendar: pd.DatetimeIndex,
    window: int = RECENT_SESSION_WINDOW,
    first_observation=None,
) -> float:
    if dates.empty or calendar.empty:
        return 0.0
    end = calendar.searchsorted(pd.Timestamp(as_of), side="right")
    first = dates[0] if first_observation is None else pd.Timestamp(first_observation)
    start = max(0, end - window, calendar.searchsorted(first, side="left"))
    if end <= start:
        return 0.0
    count = dates.searchsorted(calendar[end - 1], side="right") - dates.searchsorted(calendar[start], side="left")
    return min(1.0, float(count / (end - start)))


def coverage_by_date(dates: pd.DatetimeIndex, calendar: pd.DatetimeIndex) -> pd.Series:
    if dates.empty:
        return pd.Series(index=dates, dtype=float)
    ends = calendar.searchsorted(dates, side="right")
    starts = np.maximum(np.maximum(0, ends - RECENT_SESSION_WINDOW), calendar.searchsorted(dates[0]))
    counts = np.arange(1, len(dates) + 1) - dates.searchsorted(calendar.take(starts))
    return pd.Series(np.minimum(1.0, counts / np.maximum(1, ends - starts)), index=dates)


def trading_coverage_series(frame: pd.DataFrame, calendar) -> pd.Series:
    sessions = pd.DatetimeIndex(calendar)
    if frame.empty:
        return pd.Series(index=sessions, dtype=float)
    sessions = sessions[sessions >= frame.index[0]]
    traded = pd.Series(True, index=tradable_equity_rows(frame).index).reindex(sessions, fill_value=False)
    return traded.astype(float).rolling(RECENT_SESSION_WINDOW, min_periods=1).mean()


def apply_verifications(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Apply narrowly scoped, source-backed repairs; leave adjusted OHLC intact."""
    path = ROOT / "data" / "equity_observation_verifications.json"
    if not path.exists() or "symbol" not in frame or "date" not in frame:
        return frame, []
    result = frame.copy()
    changes = []
    for entry in json.loads(path.read_text(encoding="utf-8"))["entries"]:
        mask = (
            result.symbol.eq(entry["symbol"])
            & result.date.eq(pd.Timestamp(entry["date"]))
            & result.series_type.eq("equity")
        )
        for index in result.index[mask]:
            row = result.loc[index]
            if entry["kind"] == "volume_correction" and (pd.isna(row.Volume) or row.Volume == 0):
                ratios = row[["Open", "High", "Low", "Close"]].to_numpy(dtype=float) / np.asarray(
                    entry["unadjusted_ohlc"]
                )
                # Confirm the candle shape on a common adjustment basis. The
                # exchange's raw prices must never overwrite adjusted quotes.
                if not np.isfinite(ratios).all() or not np.allclose(ratios, np.median(ratios), rtol=0.002):
                    continue
                result.loc[index, "Volume"] = entry["volume"]
                for column in ("verification_source", "verified_at"):
                    if column in result:
                        result[column] = result[column].astype(object)
                result.loc[index, "verification_source"] = entry["source"]
                result.loc[index, "verified_at"] = entry["verified_at"]
                changes.append(
                    {
                        "symbol": entry["symbol"],
                        "date": entry["date"],
                        "old_volume": None if pd.isna(row.Volume) else float(row.Volume),
                        "new_volume": entry["volume"],
                        "source": entry["source"],
                    }
                )
    return result, changes


def prepare_publication(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    """The same idempotent cleanup for migration and all subsequent publishers."""
    corrected, repairs = apply_verifications(frame)
    equity = corrected.series_type.eq("equity")
    remove = equity & holiday_placeholder_mask(corrected)
    removed = corrected.loc[remove].copy()
    retained = classify_equity_rows(corrected.loc[equity & ~remove])
    return pd.concat([corrected.loc[~equity], retained], ignore_index=True), removed, repairs


def observation_diagnostics(prices: pd.DataFrame) -> dict:
    equities = classify_equity_rows(prices[prices.series_type.eq("equity")])
    zero = equities[equities.Volume.eq(0)]
    return {
        "policy_version": EQUITY_POLICY_VERSION,
        "raw_rows": len(equities),
        "accepted_rows": int(equities.price_status.isin(["observed", "verified_mark"]).sum()),
        "excluded_rows": int(equities.price_status.eq("unverified").sum()),
        "zero_volume_rows": len(zero),
        "unknown_volume_rows": int(equities.Volume.isna().sum()),
        "nonflat_zero_volume_rows": int(
            zero[[c for c in ("Open", "High", "Low", "Close") if c in zero]].nunique(axis=1).gt(1).sum()
        ),
        "classifications": {str(k): int(v) for k, v in equities.observation_status.value_counts().items()},
    }
