"""Cached Yahoo Finance index prices for the Stage 2 breadth overlays."""

from __future__ import annotations

import os
import tempfile
import threading

import pandas as pd
import yfinance as yf

from config import INDEX_OVERLAY_CACHE_PARQUET

# These labels intentionally match constituents.json and the left sidebar.
NSE_INDEX_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty Next 50": "^NSMIDCP",
    "Nifty Midcap 150": "NIFTYMIDCAP150.NS",
    "Nifty Smallcap 250": "NIFTYSMLCAP250.NS",
    "Nifty Microcap 250": "NIFTYMICROCAP250.NS",
}

_lock = threading.RLock()
_unavailable_in_process: set[str] = set()


def _write_atomic(df: pd.DataFrame) -> None:
    directory = os.path.dirname(INDEX_OVERLAY_CACHE_PARQUET)
    fd, tmp = tempfile.mkstemp(prefix=".index_overlay_", suffix=".parquet", dir=directory)
    os.close(fd)
    try:
        df.to_parquet(tmp, compression="snappy", index=False)
        os.replace(tmp, INDEX_OVERLAY_CACHE_PARQUET)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _read_cache() -> pd.DataFrame:
    if not os.path.exists(INDEX_OVERLAY_CACHE_PARQUET):
        return pd.DataFrame(columns=["index", "date", "Close"])
    try:
        cached = pd.read_parquet(INDEX_OVERLAY_CACHE_PARQUET)
        cached["date"] = pd.to_datetime(cached["date"])
        return cached[["index", "date", "Close"]]
    except Exception:
        return pd.DataFrame(columns=["index", "date", "Close"])


def _download_index(name: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    raw = yf.download(
        NSE_INDEX_TICKERS[name],
        start=start.date(),
        end=(end + pd.Timedelta(days=1)).date(),
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if raw is None or raw.empty or "Close" not in raw:
        return pd.DataFrame(columns=["index", "date", "Close"])
    close = raw["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    return pd.DataFrame({"index": name, "date": pd.to_datetime(close.index), "Close": close.astype(float).to_numpy()})


def load_index_overlay_series(
    names: list[str], start: pd.Timestamp, end: pd.Timestamp
) -> tuple[dict[str, pd.Series], list[str]]:
    """Return requested sidebar-index price series, fetching only cache gaps."""
    requested = [name for name in names if name in NSE_INDEX_TICKERS]
    if not requested:
        return {}, []
    with _lock:
        cached = _read_cache()
        additions: list[pd.DataFrame] = []
        unavailable: list[str] = []
        for name in requested:
            existing = cached[cached["index"] == name]
            # Date sliders can begin/end on weekends or NSE holidays.  A
            # nearby observed session is sufficient coverage; requiring an
            # exact calendar-date bound caused an unnecessary Yahoo request
            # on every Streamlit rerun.
            covered = (
                not existing.empty
                and existing["date"].min() <= start + pd.Timedelta(days=7)
                and existing["date"].max() >= end - pd.Timedelta(days=7)
            )
            if not covered:
                if name in _unavailable_in_process:
                    unavailable.append(name)
                    continue
                fresh = _download_index(name, start, end)
                if fresh.empty:
                    _unavailable_in_process.add(name)
                    unavailable.append(name)
                else:
                    additions.append(fresh)
        if additions:
            cached = pd.concat([cached, *additions], ignore_index=True)
            cached = cached.drop_duplicates(subset=["index", "date"], keep="last").sort_values(["index", "date"])
            _write_atomic(cached)

        result: dict[str, pd.Series] = {}
        for name in requested:
            series = cached.loc[cached["index"] == name].set_index("date")["Close"].sort_index().loc[start:end]
            if series.empty:
                if name not in unavailable:
                    unavailable.append(name)
                continue
            result[name] = series
        return result, unavailable
