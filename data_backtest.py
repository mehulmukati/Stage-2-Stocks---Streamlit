"""
Parquet-backed data layer for the backtest app (App 2).

Provides the same public surface that `workers.backtest_worker` already uses:
  - `_load_constituents()`
  - `load_compositions()`
  - `load_benchmark_series()`
  - `load_ohlcv_for_backtest(emit=...)`
  - `sync_benchmark_data()`  (no-op — benchmarks live in the parquet)

No DB. Ever. Caching tiers:

  Tier 1  module-level dict keyed by today_IST   — serves hot reruns in <1 ms
  Tier 1b module-level baseline DataFrame        — amortizes pd.read_parquet
  Tier 2  data/backtest_history.parquet on disk  — committed to repo
  Tier 3  yfinance                               — tail delta only (last_date+1 → today)

Survivorship bias:
  The parquet is built from the UNION of current constituents AND historical
  ex-members (stocks that were in an index at some point in the last 10 years
  but have since been removed). The backtest_engine compositions filter
  (_valid_symbols_at_date) then restricts eligibility at each rebalance date
  to only stocks actually in the index at that time.

Runtime flow:
  1. Baseline parquet → memory (once per container lifetime).
  2. Compute gap vs today_IST; if > 0, yfinance-download only the missing tail
     and merge in memory. No disk writes.
  3. Cache the merged per-symbol dict under today_IST for the rest of the day.

The parquet itself is rebuilt out-of-band by `scripts/refresh_backtest_parquet.py`.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
import yfinance as yf

from config import IST
from data import _load_constituents, get_last_valid_trading_date, load_nse_holidays  # noqa: F401

_NOOP_EMIT: Callable[[str, str], None] = lambda _lv, _msg: None

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
OHLCV_PARQUET = os.path.join(REPO_ROOT, "data", "backtest_history.parquet")
BENCH_PARQUET = os.path.join(REPO_ROOT, "data", "benchmarks.parquet")

BENCHMARK_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty 500": "^CRSLDX",
}

# ──────────────────────────────────────────────
# Module-level caches (thread-safe via _lock)
# ──────────────────────────────────────────────
_lock = threading.RLock()

# Tier 1b: long-form DataFrame materialized from parquet on first access.
_baseline_ohlcv: pd.DataFrame | None = None
_baseline_bench: pd.DataFrame | None = None

# Tier 1: merged (baseline + yfinance delta) cache, keyed by trading-day string.
_merged_ohlcv: dict[str, dict[str, pd.DataFrame]] = {}  # {today_key: {symbol: df}}
_merged_bench: dict[str, dict[str, pd.Series]] = {}  # {today_key: {label: series}}


# ──────────────────────────────────────────────
# Trading-day key (delegates to data.py — single authoritative implementation)
# ──────────────────────────────────────────────
def _get_target_key() -> str:
    now = datetime.now(IST)
    start = now.strftime("%Y-%m-%d") if now.hour >= 19 else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return get_last_valid_trading_date(start, load_nse_holidays())


# ──────────────────────────────────────────────
# Compositions (backtest-specific parquet; constituents shared via data.py)
# ──────────────────────────────────────────────
def load_compositions() -> pd.DataFrame:
    """Load historical index compositions for survivorship-bias-aware backtesting."""
    path = os.path.join(REPO_ROOT, "data", "compositions.parquet")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_parquet(path, columns=["INDEX_NAME", "TIME_STAMP", "SYMBOL"])
    return df.dropna(subset=["SYMBOL"])


# ──────────────────────────────────────────────
# Baseline parquet load (Tier 2 → Tier 1b)
# ──────────────────────────────────────────────
def _ensure_baseline_ohlcv(emit: Callable[[str, str], None]) -> pd.DataFrame:
    global _baseline_ohlcv
    with _lock:
        if _baseline_ohlcv is not None:
            return _baseline_ohlcv
    if not os.path.exists(OHLCV_PARQUET):
        raise RuntimeError(f"Missing {OHLCV_PARQUET}. Run: python scripts/refresh_backtest_parquet.py")
    emit("info", f"📦 Loading 10y backtest baseline from {os.path.basename(OHLCV_PARQUET)}…")
    df = pd.read_parquet(OHLCV_PARQUET)
    df["date"] = pd.to_datetime(df["date"])
    with _lock:
        _baseline_ohlcv = df
    emit("info", f"  ✅ {len(df):,} rows · {df['symbol'].nunique()} symbols · " f"through {df['date'].max().date()}")
    return df


def _ensure_baseline_bench(emit: Callable[[str, str], None]) -> pd.DataFrame:
    global _baseline_bench
    with _lock:
        if _baseline_bench is not None:
            return _baseline_bench
    if not os.path.exists(BENCH_PARQUET):
        raise RuntimeError(f"Missing {BENCH_PARQUET}. Run: python scripts/refresh_backtest_parquet.py")
    df = pd.read_parquet(BENCH_PARQUET)
    df["date"] = pd.to_datetime(df["date"])
    with _lock:
        _baseline_bench = df
    return df


# ──────────────────────────────────────────────
# Delta fetch (Tier 3, memory-only)
# ──────────────────────────────────────────────
def _fetch_ohlcv_delta(
    all_symbols: list[str],
    last_date: pd.Timestamp,
    today_key: str,
    emit: Callable[[str, str], None],
) -> pd.DataFrame:
    """Download {last_date+1 → today} from yfinance. Returns long-form DF, or empty."""
    start_dt = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    end_dt = (datetime.strptime(today_key, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    if start_dt >= end_dt:
        return pd.DataFrame(columns=["symbol", "date", "Close", "High", "Volume"])

    tickers = [f"{s}.NS" for s in all_symbols]
    emit("info", f"🌐 Fetching yfinance delta {start_dt} → {today_key} ({len(tickers)} symbols)…")
    try:
        raw = yf.download(
            tickers,
            start=start_dt,
            end=end_dt,
            group_by="ticker",
            threads=True,
            progress=False,
            auto_adjust=True,
        )
    except Exception as exc:
        emit("warning", f"⚠️ yfinance delta fetch failed — using stale baseline: {exc}")
        return pd.DataFrame(columns=["symbol", "date", "Close", "High", "Volume"])

    if raw is None or raw.empty:
        return pd.DataFrame(columns=["symbol", "date", "Close", "High", "Volume"])

    available = raw.columns.get_level_values(0).unique().tolist() if isinstance(raw.columns, pd.MultiIndex) else tickers

    records = []
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
                    "date": pd.Timestamp(dt.date()),
                    "Close": float(close),
                    "High": float(high) if not pd.isna(high) else float("nan"),
                    "Volume": int(vol) if not pd.isna(vol) else 0,
                }
            )
    if not records:
        return pd.DataFrame(columns=["symbol", "date", "Close", "High", "Volume"])
    df = pd.DataFrame.from_records(records)
    df["Close"] = df["Close"].astype("float32")
    df["High"] = df["High"].astype("float32")
    df["Volume"] = df["Volume"].astype("int64")
    return df


def _fetch_bench_delta(
    last_date: pd.Timestamp,
    today_key: str,
    emit: Callable[[str, str], None],
) -> pd.DataFrame:
    start_dt = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    end_dt = (datetime.strptime(today_key, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    if start_dt >= end_dt:
        return pd.DataFrame()
    series: dict[str, pd.Series] = {}
    for label, ticker in BENCHMARK_TICKERS.items():
        try:
            raw = yf.download(ticker, start=start_dt, end=end_dt, auto_adjust=True, progress=False)
            if raw is None or raw.empty:
                continue
            raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
            s = raw["Close"].copy()
            s.index = pd.to_datetime(s.index)
            s.name = label
            series[label] = s.astype("float32")
        except Exception as exc:
            emit("warning", f"⚠️ benchmark delta fetch failed for {label}: {exc}")
    if not series:
        return pd.DataFrame()
    df = pd.concat(series.values(), axis=1)
    df.columns = list(series.keys())
    df.index.name = "date"
    return df.reset_index()


# ──────────────────────────────────────────────
# Public API (matches data.py surface)
# ──────────────────────────────────────────────
def _long_to_symbol_dict(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Convert long-form {symbol, date, Close, High, Volume} → {symbol: DF indexed by date}."""
    result: dict[str, pd.DataFrame] = {}
    for sym, grp in df.groupby("symbol", sort=False):
        sub = grp.drop(columns="symbol").copy()
        sub = sub.set_index("date").sort_index()
        sub.columns = [c for c in sub.columns]  # already ["Close", "High", "Volume"]
        sub["Volume"] = sub["Volume"].astype("Int64")
        result[sym] = sub
    return result


def load_ohlcv_for_backtest(
    emit: Callable[[str, str], None] = _NOOP_EMIT,
) -> tuple[dict, str, str]:
    """
    Return (symbol_data, target_date, source).
      source ∈ {'memory', 'parquet', 'parquet+delta', 'error'}

    Tier 1  today's merged cache hit → 'memory'
    Tier 2  parquet-only (no gap or delta fetch failed) → 'parquet'
    Tier 3  parquet + yfinance delta → 'parquet+delta'
    """
    target_key = _get_target_key()

    # Tier 1 — hot per-day cache
    with _lock:
        hit = _merged_ohlcv.get(target_key)
    if hit is not None:
        return hit, target_key, "memory"

    # Tier 1b + Tier 2 — load baseline from parquet
    try:
        base = _ensure_baseline_ohlcv(emit)
    except RuntimeError as exc:
        emit("error", f"❌ {exc}")
        return {}, target_key, "error"

    last_date = base["date"].max()
    gap_days = (datetime.strptime(target_key, "%Y-%m-%d") - last_date.to_pydatetime()).days

    if gap_days <= 0:
        merged = base
        source = "parquet"
    else:
        emit("info", f"📅 Baseline through {last_date.date()} · target {target_key} · gap {gap_days}d")
        all_symbols = sorted(base["symbol"].unique().tolist())
        delta = _fetch_ohlcv_delta(all_symbols, last_date, target_key, emit)
        if delta.empty:
            merged = base
            source = "parquet"
            emit("warning", f"⚠️ Using baseline through {last_date.date()} (delta unavailable)")
        else:
            # dtype-align baseline chunk to match delta so concat stays float32
            merged = pd.concat([base, delta], ignore_index=True)
            merged = merged.drop_duplicates(subset=["symbol", "date"], keep="last")
            merged = merged.sort_values(["symbol", "date"]).reset_index(drop=True)
            source = "parquet+delta"
            emit("info", f"  ✅ Merged {len(delta):,} delta rows")

    symbol_data = _long_to_symbol_dict(merged)
    with _lock:
        _merged_ohlcv[target_key] = symbol_data
    return symbol_data, target_key, source


def load_benchmark_series() -> dict[str, pd.Series]:
    """Return close-price Series per benchmark label, indexed by date."""
    target_key = _get_target_key()
    with _lock:
        hit = _merged_bench.get(target_key)
    if hit is not None:
        return hit

    try:
        base = _ensure_baseline_bench(_NOOP_EMIT)
    except RuntimeError:
        return {}

    last_date = base["date"].max()
    gap_days = (datetime.strptime(target_key, "%Y-%m-%d") - last_date.to_pydatetime()).days

    if gap_days > 0:
        delta = _fetch_bench_delta(last_date, target_key, _NOOP_EMIT)
        if not delta.empty:
            delta["date"] = pd.to_datetime(delta["date"])
            base = pd.concat([base, delta], ignore_index=True)
            base = base.drop_duplicates(subset=["date"], keep="last").sort_values("date")

    result: dict[str, pd.Series] = {}
    for col in base.columns:
        if col == "date":
            continue
        s = base.set_index("date")[col].dropna()
        s.name = col
        result[col] = s

    with _lock:
        _merged_bench[target_key] = result
    return result


def sync_benchmark_data() -> bool:
    """
    No-op — benchmarks live in the parquet and are refreshed via
    `load_benchmark_series` on first access. Kept for API parity with data.py
    so existing workers can be rewired without signature churn.
    """
    return True
