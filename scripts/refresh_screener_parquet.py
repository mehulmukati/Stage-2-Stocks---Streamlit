"""
Build or incrementally update the screener OHLCV baseline parquet.

Run this once to seed the initial data file, then daily (e.g. via cron
or a scheduled task) to keep it current.  The app itself also does an
incremental delta fetch at startup, but running this script ensures the
committed baseline stays fresh for new deployments.

Outputs
-------
  data/screener_ohlcv.parquet
      long-form {symbol, date, Open, High, Low, Close, Volume}
      dtypes: string, datetime64, float32, float32, float32, float32, int64
      ~5–15 MB for ~750 symbols × 2 years

Usage
-----
  python scripts/refresh_screener_parquet.py           # full 2y rebuild or incremental
  python scripts/refresh_screener_parquet.py --full    # force full 2y rebuild

After running, commit the updated file:
  git add data/screener_ohlcv.parquet && git commit -m "refresh screener parquet"
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from config import HISTORY_PERIOD, IST, SCREENER_OHLCV_PARQUET  # noqa: E402

DATA_DIR = os.path.join(REPO_ROOT, "data")


# ──────────────────────────────────────────────
# Universe
# ──────────────────────────────────────────────
def _load_symbols() -> list[str]:
    """Return sorted list of all symbols across all indices in constituents.json."""
    path = os.path.join(REPO_ROOT, "constituents.json")
    with open(path, "r") as f:
        const = json.load(f)
    symbols: set[str] = set()
    for syms in const.values():
        symbols.update(syms)
    print(f"  Universe: {len(symbols)} symbols from constituents.json")
    return sorted(symbols)


# ──────────────────────────────────────────────
# Download helpers
# ──────────────────────────────────────────────
def _fetch_full(symbols: list[str]) -> pd.DataFrame:
    """Download HISTORY_PERIOD of OHLCV for all symbols; return long-form DataFrame."""
    tickers = [f"{s}.NS" for s in symbols]
    print(f"▸ Downloading {HISTORY_PERIOD} of OHLCV for {len(tickers)} symbols…")
    t0 = time.time()
    raw = yf.download(
        tickers,
        period=HISTORY_PERIOD,
        group_by="ticker",
        threads=True,
        progress=True,
        auto_adjust=True,
    )
    print(f"  Downloaded in {time.time() - t0:.1f}s")
    if raw is None or raw.empty:
        raise RuntimeError("yfinance returned empty response")
    return _reshape(raw, tickers)


def _fetch_delta(symbols: list[str], from_date: str) -> pd.DataFrame:
    """Download OHLCV from from_date to today for all symbols; return long-form DataFrame."""
    tickers = [f"{s}.NS" for s in symbols]
    today = datetime.now(IST).strftime("%Y-%m-%d")
    print(f"▸ Incremental fetch: {from_date} → {today} for {len(tickers)} symbols…")
    t0 = time.time()
    raw = yf.download(
        tickers,
        start=from_date,
        end=today,
        group_by="ticker",
        threads=True,
        progress=True,
        auto_adjust=True,
    )
    print(f"  Downloaded in {time.time() - t0:.1f}s")
    if raw is None or raw.empty:
        print("  ⚠️  No new data returned — baseline is already up to date.")
        return pd.DataFrame()
    return _reshape(raw, tickers)


def _reshape(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Reshape yfinance multi-ticker download into long-form {symbol, date, Open, High, Low, Close, Volume}."""
    available = (
        raw.columns.get_level_values(0).unique().tolist()
        if isinstance(raw.columns, pd.MultiIndex)
        else tickers
    )
    records = []
    for t in tickers:
        sym = t.replace(".NS", "")
        if t not in available:
            continue
        try:
            sub = raw[t].dropna(how="all") if len(tickers) > 1 else raw.dropna(how="all")
            sub.columns = [c[0] if isinstance(c, tuple) else c for c in sub.columns]
            for dt, row in sub.iterrows():
                close = row.get("Close")
                if pd.isna(close):
                    continue
                def _f(v):
                    try:
                        fv = float(v)
                        return None if pd.isna(fv) else fv
                    except (TypeError, ValueError):
                        return None
                records.append({
                    "symbol": sym,
                    "date": dt.date(),
                    "Open":  _f(row.get("Open")),
                    "High":  _f(row.get("High")),
                    "Low":   _f(row.get("Low")),
                    "Close": float(close),
                    "Volume": int(row.get("Volume") or 0),
                })
        except Exception as exc:
            print(f"  ⚠️  Skipped {sym}: {exc}")
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)
    df["date"] = pd.to_datetime(df["date"])
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = df[col].astype("float32")
    df["Volume"] = df["Volume"].astype("int64")
    return df.sort_values(["symbol", "date"]).reset_index(drop=True)


# ──────────────────────────────────────────────
# Atomic write (same pattern as data.py)
# ──────────────────────────────────────────────
def _write_atomic(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=os.path.dirname(path), suffix=".tmp", delete=False) as f:
        tmp = f.name
    try:
        df.to_parquet(tmp, compression="snappy", index=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main(force_full: bool = False) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    symbols = _load_symbols()

    existing: pd.DataFrame | None = None
    global_max: str | None = None

    if not force_full and os.path.exists(SCREENER_OHLCV_PARQUET):
        print("▸ Loading existing baseline…")
        existing = pd.read_parquet(SCREENER_OHLCV_PARQUET)
        existing["date"] = pd.to_datetime(existing["date"])
        global_max = existing["date"].max().strftime("%Y-%m-%d")
        print(f"  Baseline: {len(existing):,} rows · max date {global_max}")

    earliest_needed = (
        datetime.now(IST) - timedelta(days=750)
    ).strftime("%Y-%m-%d")

    needs_full = (
        force_full
        or existing is None
        or existing.empty
        or global_max < earliest_needed
    )

    if needs_full:
        new_data = _fetch_full(symbols)
        merged = new_data
    else:
        fetch_from = (
            datetime.strptime(global_max, "%Y-%m-%d") - timedelta(days=5)
        ).strftime("%Y-%m-%d")
        delta = _fetch_delta(symbols, from_date=fetch_from)
        if delta.empty:
            print("▸ Nothing to update.")
            return
        merged = pd.concat([existing, delta], ignore_index=True)
        merged = merged.drop_duplicates(subset=["symbol", "date"], keep="last")
        merged = merged.sort_values(["symbol", "date"]).reset_index(drop=True)

    print(f"▸ Writing {len(merged):,} rows to {SCREENER_OHLCV_PARQUET}…")
    _write_atomic(merged, SCREENER_OHLCV_PARQUET)

    size_mb = os.path.getsize(SCREENER_OHLCV_PARQUET) / (1024 * 1024)
    n_syms = merged["symbol"].nunique()
    d_min, d_max = merged["date"].min(), merged["date"].max()
    print(
        f"  ✅ screener_ohlcv.parquet: {len(merged):,} rows · "
        f"{n_syms} symbols · {d_min:%Y-%m-%d} → {d_max:%Y-%m-%d} · {size_mb:.1f} MB"
    )
    print(f"▸ Done at {datetime.now(IST):%Y-%m-%d %H:%M %Z}.")
    print("  Next: git add data/screener_ohlcv.parquet && git commit -m 'refresh screener parquet'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh screener_ohlcv.parquet")
    parser.add_argument("--full", action="store_true", help="Force full 2y rebuild")
    args = parser.parse_args()
    main(force_full=args.full)
