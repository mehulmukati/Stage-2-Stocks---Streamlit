"""
Full 10-year rebase of the backtest parquet baselines.

Run this manually (or on a local cron) — monthly is a reasonable cadence —
to catch retroactive split/dividend adjustments from yfinance. The app itself
never rewrites these files; it only fetches a tail delta into memory at
runtime.

Survivorship-bias handling
--------------------------
The symbol universe is the UNION of:
  (a) constituents.json — current index members (750 symbols)
  (b) data/compositions.parquet SYMBOL column — all HISTORICAL members
      ever included in any tracked index (1,144 symbols as of 2026)

The extra 394 ex-members are stocks that were in an index at some point
in the last 10 years but have since been removed (fallen to a smaller cap
bucket, merged, or delisted). Without their historical price data, any
backtest period covering their time in the index would silently skip them
— a classic survivorship bias.

  For stocks still listed today: yfinance returns the full 10 y history.
  For delisted stocks: yfinance returns data up to the delisting date,
    which is exactly what we need for periods when they were valid holdings.
  For stocks yfinance has no record of: they are omitted with a warning.

The compositions filter in backtest_engine._valid_symbols_at_date() then
ensures we only consider a stock as eligible during the date ranges when
it was actually a constituent of the requested index — so having its price
history beyond its membership period is harmless.

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
  python scripts/refresh_backtest_parquet.py
  git add data/*.parquet && git commit -m "refresh parquet baselines" && git push

Streamlit Cloud picks up the new baselines on its next redeploy.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

# Make repo root importable so we can pull shared constants.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from config import HISTORY_PERIOD, IST  # noqa: E402

DATA_DIR = os.path.join(REPO_ROOT, "data")
OUT_OHLCV = os.path.join(DATA_DIR, "backtest_history.parquet")
OUT_BENCH = os.path.join(DATA_DIR, "benchmarks.parquet")

BENCHMARK_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty 500": "^CRSLDX",
}


def _load_symbols() -> tuple[list[str], int, int]:
    """
    Build the full historical universe:
      current members (constituents.json) ∪ all-time members (compositions.parquet)

    Returns (sorted_symbols, n_current, n_ex_members).
    """
    # Current members
    path = os.path.join(REPO_ROOT, "constituents.json")
    with open(path, "r") as f:
        const = json.load(f)
    current: set[str] = set()
    for syms in const.values():
        current.update(syms)

    # Historical ex-members from compositions.parquet
    comp_path = os.path.join(DATA_DIR, "compositions.parquet")
    historical: set[str] = set()
    if os.path.exists(comp_path):
        comp_df = pd.read_parquet(comp_path, columns=["SYMBOL"])
        historical = set(comp_df["SYMBOL"].dropna().unique())
    else:
        print("  ⚠️  compositions.parquet not found — using current members only (survivorship bias risk!)")

    ex_members = historical - current
    universe = current | historical
    print(f"  Current members : {len(current)}")
    print(f"  Historical ex-members: {len(ex_members)} " f"(in compositions.parquet but not in constituents.json)")
    print(f"  Full universe   : {len(universe)} symbols")
    return sorted(universe), len(current), len(ex_members)


def _fetch_ohlcv(symbols: list[str]) -> pd.DataFrame:
    """
    Batched yfinance download for 10 y of OHLCV. Reshape multi-indexed result
    into long-form {symbol, date, Close, High, Volume} with compact dtypes.

    Ex-member (delisted) symbols are silently skipped by yfinance if it has
    no data for them; their presence in the ticker list is harmless.
    """
    tickers = [f"{s}.NS" for s in symbols]
    print(
        f"▸ Downloading {HISTORY_PERIOD} of OHLCV for {len(tickers)} symbols…"
        f"\n  (includes ex-members; some may be delisted and return partial/no data)"
    )
    t0 = time.time()
    raw = yf.download(
        tickers,
        period=HISTORY_PERIOD,
        group_by="ticker",
        threads=True,
        progress=True,
        auto_adjust=True,
    )
    print(f"  downloaded in {time.time() - t0:.1f}s")

    if raw is None or raw.empty:
        raise RuntimeError("yfinance returned empty frame")

    records: list[dict] = []
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

    if not records:
        raise RuntimeError("No OHLCV rows survived filtering — check yfinance response")

    df = pd.DataFrame.from_records(records)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["Close"] = df["Close"].astype("float32")
    df["High"] = df["High"].astype("float32")
    df["Volume"] = df["Volume"].astype("int64")
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df


def _fetch_benchmarks() -> pd.DataFrame:
    """10 y Close series for each index, returned wide {date, "Nifty 50", "Nifty 500"}."""
    print(f"▸ Downloading {HISTORY_PERIOD} of benchmark data…")
    series: dict[str, pd.Series] = {}
    for label, ticker in BENCHMARK_TICKERS.items():
        raw = yf.download(ticker, period=HISTORY_PERIOD, auto_adjust=True, progress=False)
        if raw is None or raw.empty:
            raise RuntimeError(f"yfinance returned empty for {ticker}")
        raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
        s = raw["Close"].copy()
        s.index = pd.to_datetime(s.index).date
        s.name = label
        series[label] = s.astype("float32")
    df = pd.concat(series.values(), axis=1)
    df.columns = list(series.keys())
    df.index.name = "date"
    df = df.reset_index().sort_values("date").reset_index(drop=True)
    return df


def _report(label: str, path: str, df: pd.DataFrame) -> None:
    size_mb = os.path.getsize(path) / (1024 * 1024)
    if "symbol" in df.columns:
        symbols = df["symbol"].nunique()
        dmin, dmax = df["date"].min(), df["date"].max()
        print(f"  ✅ {label}: {len(df):,} rows · {symbols} symbols · " f"{dmin} → {dmax} · {size_mb:.1f} MB")
    else:
        dmin, dmax = df["date"].min(), df["date"].max()
        print(f"  ✅ {label}: {len(df):,} rows · {dmin} → {dmax} · {size_mb:.2f} MB")


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

    print("▸ Building full historical universe…")
    symbols, n_current, n_ex = _load_symbols()

    ohlcv_df = _fetch_ohlcv(symbols)
    ohlcv_df.to_parquet(OUT_OHLCV, compression="snappy", index=False)
    _report("backtest_history.parquet", OUT_OHLCV, ohlcv_df)

    bench_df = _fetch_benchmarks()
    bench_df.to_parquet(OUT_BENCH, compression="snappy", index=False)
    _report("benchmarks.parquet", OUT_BENCH, bench_df)

    # Coverage report: how many ex-members actually had yfinance data?
    n_with_data = ohlcv_df["symbol"].nunique()
    print(
        f"\n▸ Coverage: {n_with_data} symbols had yfinance data "
        f"({n_current} current + {n_with_data - n_current} ex-members with data "
        f"out of {n_ex} ex-members attempted)"
    )
    print(
        f"▸ Done at {datetime.now(IST):%Y-%m-%d %H:%M %Z}. "
        f"Next steps: git add data/*.parquet && git commit && git push."
    )


if __name__ == "__main__":
    main()
