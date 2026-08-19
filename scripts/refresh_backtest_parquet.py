"""
Full or incremental refresh of the backtest parquet baselines.

Modes
-----
  Default (incremental):
      Fetches only the tail delta (last 30 days) and merges into the existing
      parquet.  Fast and suitable for running daily.

  --full:
      Full 10-year rebuild. Symbols are downloaded in batches; progress is
      checkpointed after each batch so a network failure can be resumed without
      restarting from scratch.  Suitable for monthly runs to catch retroactive
      split/dividend adjustments from yfinance.

Survivorship-bias handling
--------------------------
The symbol universe is the UNION of:
  (a) constituents.json — current index members (750 symbols)
  (b) data/compositions.parquet SYMBOL column — all HISTORICAL members
      ever included in any tracked index (1,144 symbols as of 2026)

Outputs
-------
  data/backtest_history.parquet
      long-form {symbol, date, Close, High, Volume}
      dtypes: string, date32, float32, float32, int64
      ~25–30 MB for ~1,144 symbols × 10 y (ex-members have partial history)

  data/benchmarks.parquet
      wide-form {date, "Nifty 50", "Nifty 500"}   (Close only)
      <1 MB

Usage
-----
  python scripts/refresh_backtest_parquet.py              # incremental (fast)
  python scripts/refresh_backtest_parquet.py --full       # full rebuild (slow)

Streamlit Cloud picks up the new baselines on its next redeploy.
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

from config import IST  # noqa: E402

DATA_DIR = os.path.join(REPO_ROOT, "data")
OUT_OHLCV = os.path.join(DATA_DIR, "backtest_history.parquet")
OUT_BENCH = os.path.join(DATA_DIR, "benchmarks.parquet")
CHECKPOINT_FILE = os.path.join(DATA_DIR, ".backtest_rebuild_checkpoint.json")

BENCHMARK_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty 500": "^CRSLDX",
}

FULL_HISTORY_PERIOD = "10y"
INCREMENTAL_DAYS = 30
BATCH_SIZE = 200
MAX_RETRIES = 3


# ──────────────────────────────────────────────
# Universe
# ──────────────────────────────────────────────


def _load_symbols() -> tuple[list[str], int, int]:
    path = os.path.join(REPO_ROOT, "constituents.json")
    with open(path, "r") as f:
        const = json.load(f)
    current: set[str] = set()
    for syms in const.values():
        current.update(syms)

    comp_path = os.path.join(DATA_DIR, "compositions.parquet")
    historical: set[str] = set()
    if os.path.exists(comp_path):
        comp_df = pd.read_parquet(comp_path, columns=["SYMBOL"])
        historical = set(comp_df["SYMBOL"].dropna().unique())
    else:
        print("  ⚠️  compositions.parquet not found — using current members only (survivorship bias risk!)")

    ex_members = historical - current
    universe = current | historical
    print(f"  Current members      : {len(current)}")
    print(f"  Historical ex-members: {len(ex_members)}")
    print(f"  Full universe        : {len(universe)} symbols")
    return sorted(universe), len(current), len(ex_members)


def _load_current_symbols() -> set[str]:
    with open(os.path.join(REPO_ROOT, "constituents.json"), encoding="utf-8") as fh:
        constituents = json.load(fh)
    return {symbol for members in constituents.values() for symbol in members}


def _validate_current_tradability(
    df: pd.DataFrame,
    max_stale_sessions: int = 3,
    current_symbols: set[str] | None = None,
) -> None:
    """Fail closed when a current constituent lacks a recent positive-volume bar."""
    dates = pd.DatetimeIndex(pd.to_datetime(df["date"]).dropna().unique()).sort_values()
    if dates.empty:
        raise RuntimeError("OHLCV refresh produced no trading dates")
    cutoff_position = max(0, len(dates) - max_stale_sessions - 1)
    cutoff = dates[cutoff_position]
    valid = df[(pd.to_numeric(df["Close"], errors="coerce") > 0) & (pd.to_numeric(df["Volume"], errors="coerce") > 0)]
    last_by_symbol = pd.to_datetime(valid["date"]).groupby(valid["symbol"]).max()
    symbols_to_check = current_symbols if current_symbols is not None else _load_current_symbols()
    stale = sorted(
        symbol
        for symbol in symbols_to_check
        if symbol not in last_by_symbol.index or pd.Timestamp(last_by_symbol[symbol]) < cutoff
    )
    if stale:
        sample = ", ".join(stale[:20])
        suffix = f" (+{len(stale) - 20} more)" if len(stale) > 20 else ""
        raise RuntimeError(
            f"Current-constituent tradability validation failed at cutoff {cutoff.date()}: {sample}{suffix}. "
            "Refresh constituents or register the relevant corporate action before publishing data."
        )


# ──────────────────────────────────────────────
# Download helpers
# ──────────────────────────────────────────────


def _reshape_batch(raw: pd.DataFrame, tickers: list[str]) -> list[dict]:
    """Reshape a yfinance multi-ticker result into a list of row dicts."""
    records: list[dict] = []
    if raw is None or raw.empty:
        return records
    available = raw.columns.get_level_values(0).unique().tolist() if isinstance(raw.columns, pd.MultiIndex) else tickers
    for ticker in tickers:
        if ticker not in available:
            continue
        sym = ticker.replace(".NS", "")
        sub = raw[ticker].dropna(how="all") if len(tickers) > 1 else raw.dropna(how="all")
        sub.columns = [c[0] if isinstance(c, tuple) else c for c in sub.columns]
        for dt, row in sub.iterrows():
            close = row.get("Close")
            if pd.isna(close):
                continue
            high = row.get("High")
            vol = row.get("Volume")
            records.append(
                {
                    "symbol": sym,
                    "date": dt.date(),
                    "Close": float(close),
                    "High": float(high) if not pd.isna(high) else float("nan"),
                    "Volume": int(vol) if not pd.isna(vol) else 0,
                }
            )
    return records


def _download_batch(
    tickers: list[str],
    period: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    """Download one batch with up to MAX_RETRIES retries on failure."""
    kwargs: dict = dict(group_by="ticker", threads=True, progress=False, auto_adjust=True)
    if period:
        kwargs["period"] = period
    else:
        kwargs["start"] = start
        kwargs["end"] = end

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw = yf.download(tickers, **kwargs)
            return _reshape_batch(raw, tickers)
        except Exception as exc:
            wait = 2**attempt
            print(f"    ⚠️  Attempt {attempt}/{MAX_RETRIES} failed: {exc}. Retrying in {wait}s…")
            time.sleep(wait)
    print(f"    ✗  Batch failed after {MAX_RETRIES} attempts — skipping {len(tickers)} symbols.")
    return []


# ──────────────────────────────────────────────
# Checkpoint helpers
# ──────────────────────────────────────────────


def _load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"completed_batches": [], "partial_parquets": []}


def _save_checkpoint(state: dict) -> None:
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(state, f)


def _clear_checkpoint() -> None:
    if os.path.exists(CHECKPOINT_FILE):
        os.unlink(CHECKPOINT_FILE)
    # Clean up any leftover partial parquets
    for f in os.listdir(DATA_DIR):
        if f.startswith(".backtest_batch_") and f.endswith(".parquet"):
            os.unlink(os.path.join(DATA_DIR, f))


# ──────────────────────────────────────────────
# Full rebuild (batched + checkpointed)
# ──────────────────────────────────────────────


def _fetch_ohlcv_full(symbols: list[str]) -> pd.DataFrame:
    tickers = [f"{s}.NS" for s in symbols]
    batches = [tickers[i : i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    total = len(batches)

    state = _load_checkpoint()
    done_batches: set[int] = set(state.get("completed_batches", []))
    partial_parquets: list[str] = state.get("partial_parquets", [])

    if done_batches:
        print(f"  ↩  Resuming from checkpoint — {len(done_batches)}/{total} batches already done.")

    t0 = time.time()

    for i, batch in enumerate(batches):
        if i in done_batches:
            continue
        print(f"  Batch {i + 1}/{total} ({len(batch)} symbols)…", end=" ", flush=True)
        bt = time.time()
        records = _download_batch(batch, period=FULL_HISTORY_PERIOD)
        elapsed = time.time() - bt
        print(f"{len(records):,} rows in {elapsed:.1f}s")

        if records:
            # Write this batch to a temp parquet for crash safety
            batch_path = os.path.join(DATA_DIR, f".backtest_batch_{i}.parquet")
            pd.DataFrame(records).to_parquet(batch_path, compression="snappy", index=False)
            partial_parquets.append(batch_path)

        done_batches.add(i)
        _save_checkpoint({"completed_batches": list(done_batches), "partial_parquets": partial_parquets})

    print(f"  All batches done in {time.time() - t0:.1f}s. Combining…")

    # Combine all batch parquets
    frames = []
    for path in partial_parquets:
        if os.path.exists(path):
            frames.append(pd.read_parquet(path))
    if not frames:
        raise RuntimeError("No OHLCV rows survived — check yfinance response")

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["Close"] = df["Close"].astype("float32")
    df["High"] = df["High"].astype("float32")
    df["Volume"] = df["Volume"].astype("int64")
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    _clear_checkpoint()
    return df


# ──────────────────────────────────────────────
# Incremental fetch
# ──────────────────────────────────────────────


def _fetch_ohlcv_incremental(symbols: list[str], existing: pd.DataFrame) -> pd.DataFrame:
    existing["date"] = pd.to_datetime(existing["date"])
    global_max = existing["date"].max()
    fetch_from = (global_max - timedelta(days=5)).strftime("%Y-%m-%d")
    today = datetime.now(IST).strftime("%Y-%m-%d")

    if fetch_from >= today:
        print("  Baseline is already up to date — nothing to fetch.")
        return existing

    tickers = [f"{s}.NS" for s in symbols]
    print(f"▸ Incremental fetch: {fetch_from} → {today} for {len(tickers)} symbols…")
    t0 = time.time()
    records = _download_batch(tickers, start=fetch_from, end=today)
    print(f"  {len(records):,} rows in {time.time() - t0:.1f}s")

    if not records:
        print("  ⚠️  No new data returned.")
        return existing

    delta = pd.DataFrame(records)
    delta["date"] = pd.to_datetime(delta["date"])
    delta["Close"] = delta["Close"].astype("float32")
    delta["High"] = delta["High"].astype("float32")
    delta["Volume"] = delta["Volume"].astype("int64")

    merged = pd.concat([existing, delta], ignore_index=True)
    merged = merged.drop_duplicates(subset=["symbol", "date"], keep="last")
    merged = merged.sort_values(["symbol", "date"]).reset_index(drop=True)
    return merged


# ──────────────────────────────────────────────
# Benchmarks
# ──────────────────────────────────────────────


def _fetch_benchmarks() -> pd.DataFrame:
    print(f"▸ Downloading {FULL_HISTORY_PERIOD} of benchmark data…")
    series: dict[str, pd.Series] = {}
    for label, ticker in BENCHMARK_TICKERS.items():
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                raw = yf.download(ticker, period=FULL_HISTORY_PERIOD, auto_adjust=True, progress=False)
                if raw is None or raw.empty:
                    raise RuntimeError(f"Empty response for {ticker}")
                raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
                s = raw["Close"].copy()
                s.index = pd.to_datetime(s.index).date
                s.name = label
                series[label] = s.astype("float32")
                break
            except Exception as exc:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Failed to fetch {ticker} after {MAX_RETRIES} attempts: {exc}") from exc
                time.sleep(2**attempt)
    df = pd.concat(series.values(), axis=1)
    df.columns = list(series.keys())
    df.index.name = "date"
    return df.reset_index().sort_values("date").reset_index(drop=True)


# ──────────────────────────────────────────────
# Atomic write
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


def _report(label: str, path: str, df: pd.DataFrame) -> None:
    size_mb = os.path.getsize(path) / (1024 * 1024)
    if "symbol" in df.columns:
        symbols = df["symbol"].nunique()
        dmin, dmax = df["date"].min(), df["date"].max()
        print(f"  ✅ {label}: {len(df):,} rows · {symbols} symbols · {dmin} → {dmax} · {size_mb:.1f} MB")
    else:
        dmin, dmax = df["date"].min(), df["date"].max()
        print(f"  ✅ {label}: {len(df):,} rows · {dmin} → {dmax} · {size_mb:.2f} MB")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────


def main(force_full: bool = False) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

    print("▸ Building full historical universe…")
    symbols, n_current, n_ex = _load_symbols()

    existing: pd.DataFrame | None = None
    if not force_full and os.path.exists(OUT_OHLCV):
        print("▸ Loading existing baseline…")
        existing = pd.read_parquet(OUT_OHLCV)
        print(f"  Baseline: {len(existing):,} rows · {existing['symbol'].nunique()} symbols")

    if force_full or existing is None or existing.empty:
        print(f"▸ Full {FULL_HISTORY_PERIOD} rebuild (batched, BATCH_SIZE={BATCH_SIZE})…")
        ohlcv_df = _fetch_ohlcv_full(symbols)
    else:
        ohlcv_df = _fetch_ohlcv_incremental(symbols, existing)

    _validate_current_tradability(ohlcv_df)
    _write_atomic(ohlcv_df, OUT_OHLCV)
    _report("backtest_history.parquet", OUT_OHLCV, ohlcv_df)

    # Always refresh benchmarks (tiny download)
    bench_df = _fetch_benchmarks()
    _write_atomic(bench_df, OUT_BENCH)
    _report("benchmarks.parquet", OUT_BENCH, bench_df)

    n_with_data = ohlcv_df["symbol"].nunique()
    print(
        f"\n▸ Coverage: {n_with_data} symbols had data "
        f"({n_current} current + {n_with_data - n_current} ex-members with data "
        f"out of {n_ex} ex-members attempted)"
    )
    print(
        f"▸ Done at {datetime.now(IST):%Y-%m-%d %H:%M %Z}. " f"Next: git add data/*.parquet && git commit && git push."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh backtest_history.parquet")
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Force full {FULL_HISTORY_PERIOD} rebuild (batched+checkpointed). "
        "Default: incremental (last 30 days only).",
    )
    args = parser.parse_args()
    main(force_full=args.full)
