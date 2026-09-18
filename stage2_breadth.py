"""Historical Stage 2 market-breadth calculation and durable cache."""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading

import pandas as pd

import config
from stage2_engine import compute_rolling_stage2

BREADTH_CACHE_VERSION = 3
_DATA_DIR = os.path.dirname(config.SCREENER_OHLCV_PARQUET)
SCREENER_OHLCV_PARQUET = config.SCREENER_OHLCV_PARQUET
BACKTEST_HISTORY_PARQUET = getattr(
    config, "BACKTEST_HISTORY_PARQUET", os.path.join(_DATA_DIR, "backtest_history.parquet")
)
STAGE2_BREADTH_CACHE_PARQUET = getattr(
    config, "STAGE2_BREADTH_CACHE_PARQUET", os.path.join(_DATA_DIR, "stage2_breadth_scores.parquet")
)
STAGE2_BREADTH_CACHE_META = getattr(
    config, "STAGE2_BREADTH_CACHE_META", os.path.join(_DATA_DIR, "stage2_breadth_scores.meta.json")
)
_PHASE_TO_COLUMN = {
    "Not Stage 2": "Not Stage 2",
    "Early/Weak Stage 2": "Early",
    "Likely Stage 2": "Likely",
    "Strong Stage 2": "Strong",
}
_score_history_lock = threading.RLock()
_score_history_memory: tuple[dict, pd.DataFrame] | None = None
_aggregate_lock = threading.RLock()
_aggregate_memory: dict[tuple, pd.DataFrame] = {}


def _available_ohlcv_sources() -> list[str]:
    """Use long history for the lookback and the screener baseline for its fresher tail."""
    paths = [path for path in (BACKTEST_HISTORY_PARQUET, SCREENER_OHLCV_PARQUET) if os.path.exists(path)]
    if not paths:
        raise FileNotFoundError("No local OHLCV parquet is available for Stage 2 breadth.")
    return paths


def _source_fingerprint() -> dict:
    sources = []
    for path in _available_ohlcv_sources():
        stat = os.stat(path)
        sources.append({"path": os.path.basename(path), "mtime_ns": stat.st_mtime_ns, "size": stat.st_size})
    return {"version": BREADTH_CACHE_VERSION, "sources": sources}


def _atomic_write_parquet(df: pd.DataFrame, path: str) -> None:
    directory = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(prefix=".stage2_breadth_", suffix=".parquet", dir=directory)
    os.close(fd)
    try:
        df.to_parquet(tmp, compression="snappy", index=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _atomic_write_json(payload: dict, path: str) -> None:
    directory = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(prefix=".stage2_breadth_", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _read_valid_cache(fingerprint: dict) -> pd.DataFrame | None:
    if not (os.path.exists(STAGE2_BREADTH_CACHE_PARQUET) and os.path.exists(STAGE2_BREADTH_CACHE_META)):
        return None
    try:
        with open(STAGE2_BREADTH_CACHE_META, encoding="utf-8") as handle:
            if json.load(handle) != fingerprint:
                return None
        cached = pd.read_parquet(STAGE2_BREADTH_CACHE_PARQUET)
        expected = {"date", "symbol", "Score", "Phase"}
        if expected.issubset(cached.columns):
            cached["date"] = pd.to_datetime(cached["date"])
            return cached
    except (OSError, ValueError, TypeError):
        return None
    return None


def build_breadth_score_history(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Return one mature Stage 2 classification per available stock/session.

    A classification begins only once 250 sessions are available, matching the
    screener's minimum-history contract.  Missing trading bars are not filled:
    a stock is eligible only on sessions with an observed close and volume.
    """
    required = {"symbol", "date", "Close", "Volume"}
    missing = required.difference(ohlcv.columns)
    if missing:
        raise ValueError(f"OHLCV is missing required breadth columns: {sorted(missing)}")

    pieces: list[pd.DataFrame] = []
    source = ohlcv[["symbol", "date", "Close", "Volume"]].copy()
    source["date"] = pd.to_datetime(source["date"])
    source = source.dropna(subset=["symbol", "date", "Close", "Volume"])
    for symbol, stock in source.groupby("symbol", sort=False):
        stock = stock.sort_values("date").drop_duplicates("date", keep="last").set_index("date")
        if len(stock) < 250:
            continue
        rolled = compute_rolling_stage2(stock)
        # The 200-day MA's 50-session slope becomes available on row 251 in
        # the vectorised representation.  Earlier rows must not be treated as
        # a valid "Not Stage 2" observation.
        rolled = rolled.iloc[250:].dropna(subset=["MA50", "MA150", "MA200"])
        if rolled.empty:
            continue
        piece = rolled[["Score", "Phase"]].reset_index(names="date")
        piece.insert(1, "symbol", str(symbol))
        pieces.append(piece)
    if not pieces:
        return pd.DataFrame(columns=["date", "symbol", "Score", "Phase"])
    out = pd.concat(pieces, ignore_index=True)
    out["Score"] = out["Score"].astype("int8")
    out["Phase"] = out["Phase"].astype(str)
    return out.sort_values(["date", "symbol"]).reset_index(drop=True)


def load_breadth_score_history() -> tuple[pd.DataFrame, bool]:
    """Load scores from memory/disk, rebuilding only after OHLCV changes."""
    global _score_history_memory
    fingerprint = _source_fingerprint()
    with _score_history_lock:
        if _score_history_memory is not None and _score_history_memory[0] == fingerprint:
            return _score_history_memory[1], True
        cached = _read_valid_cache(fingerprint)
        if cached is not None:
            _score_history_memory = (fingerprint, cached)
            return cached, True
        frames = [
            pd.read_parquet(path, columns=["symbol", "date", "Close", "Volume"]) for path in _available_ohlcv_sources()
        ]
        # The fresher screener baseline is deliberately last, so it overrides
        # overlapping rows from the long research baseline.
        ohlcv = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["symbol", "date"], keep="last")
        # Three calendar years supply the ~250-session lookback needed to show
        # two full years of mature Stage 2 classifications without doing a
        # costly ten-year calculation.
        newest = pd.to_datetime(ohlcv["date"]).max()
        ohlcv = ohlcv[pd.to_datetime(ohlcv["date"]) >= newest - pd.DateOffset(years=3)]
        scores = build_breadth_score_history(ohlcv)
        _atomic_write_parquet(scores, STAGE2_BREADTH_CACHE_PARQUET)
        _atomic_write_json(fingerprint, STAGE2_BREADTH_CACHE_META)
        _score_history_memory = (fingerprint, scores)
        return scores, False


def _prepare_compositions(compositions: pd.DataFrame, selected_indices: list[str]) -> pd.DataFrame | None:
    """Canonicalise and filter historical membership once per selection."""
    if compositions.empty or not selected_indices:
        return None
    required = {"INDEX_NAME", "TIME_STAMP", "SYMBOL"}
    if not required.issubset(compositions.columns):
        return None
    selected_keys = {re.sub(r"[^A-Z0-9]", "", str(name).upper()) for name in selected_indices}
    comp = compositions.copy()
    comp["_index_key"] = comp["INDEX_NAME"].map(lambda name: re.sub(r"[^A-Z0-9]", "", str(name).upper()))
    comp = comp[comp["_index_key"].isin(selected_keys)].copy()
    if comp.empty:
        return None
    comp["TIME_STAMP"] = pd.to_datetime(comp["TIME_STAMP"], errors="coerce")
    return comp.dropna(subset=["TIME_STAMP", "SYMBOL"])


def _symbols_for_date(compositions: pd.DataFrame | None, date: pd.Timestamp) -> set[str] | None:
    """Return historical union membership, or None when no usable snapshot exists."""
    if compositions is None:
        return None
    eligible = compositions[compositions["TIME_STAMP"] <= date]
    if eligible.empty:
        return set()
    latest = eligible.groupby("_index_key")["TIME_STAMP"].transform("max")
    return set(eligible.loc[eligible["TIME_STAMP"] == latest, "SYMBOL"].astype(str))


def aggregate_breadth(
    scores: pd.DataFrame,
    selected_indices: list[str] | None = None,
    compositions: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate cached stock classifications into daily breadth observations."""
    selected_indices = selected_indices or []
    compositions = compositions if compositions is not None else pd.DataFrame()
    columns = [
        "date",
        "Eligible",
        "Not Stage 2",
        "Early",
        "Likely",
        "Strong",
        "Stage 2",
        "Stage 2 %",
        "Strong %",
        "Average Score",
    ]
    if scores.empty:
        return pd.DataFrame(columns=columns)

    cache_key = (
        tuple(sorted(selected_indices)),
        len(scores),
        str(scores["date"].min()),
        str(scores["date"].max()),
        int(scores["Score"].sum()),
        len(compositions),
        str(compositions["TIME_STAMP"].max()) if "TIME_STAMP" in compositions else "",
    )
    with _aggregate_lock:
        cached = _aggregate_memory.get(cache_key)
    if cached is not None:
        return cached.copy()

    prepared_compositions = _prepare_compositions(compositions, selected_indices)
    rows: list[dict] = []
    for date, daily in scores.groupby("date", sort=True):
        active_symbols = _symbols_for_date(prepared_compositions, pd.Timestamp(date))
        if active_symbols is not None:
            daily = daily[daily["symbol"].isin(active_symbols)]
        counts = daily["Phase"].value_counts()
        eligible = len(daily)
        row = {"date": pd.Timestamp(date), "Eligible": eligible}
        for phase, col in _PHASE_TO_COLUMN.items():
            row[col] = int(counts.get(phase, 0))
        row["Stage 2"] = row["Early"] + row["Likely"] + row["Strong"]
        row["Stage 2 %"] = (100 * row["Stage 2"] / eligible) if eligible else 0.0
        row["Strong %"] = (100 * row["Strong"] / eligible) if eligible else 0.0
        row["Average Score"] = float(daily["Score"].mean()) if eligible else 0.0
        rows.append(row)
    result = pd.DataFrame(rows, columns=columns)
    with _aggregate_lock:
        _aggregate_memory[cache_key] = result
    return result.copy()
