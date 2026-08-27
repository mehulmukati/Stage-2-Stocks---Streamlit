"""
Parquet-backed data layer for the backtest app (App 2).

Provides the same public surface that `workers.backtest_worker` already uses:
  - `_load_constituents()`
  - `load_compositions()`
  - `load_benchmark_series()`
  - `load_ohlcv_for_backtest(emit=...)`
  - `sync_benchmark_data()`  (no-op — benchmarks live in the parquet)

No DB. Ever. Caching tiers:

  Tier 1   module-level dict keyed by today_IST   — serves hot reruns in <1 ms
  Tier 1b  module-level baseline DataFrame        — amortizes pd.read_parquet
  Tier 2   data/backtest_history.parquet on disk  — committed to repo
  Tier 2.5 data/backtest_delta.parquet on disk    — gitignored local delta cache;
             accumulates yfinance tail rows so restarts skip re-fetching known dates
  Tier 3   yfinance                               — only truly new dates not in Tier 2/2.5

Survivorship bias:
  The parquet is built from the UNION of current constituents AND historical
  ex-members (stocks that were in an index at some point in the last 10 years
  but have since been removed). The backtest_engine compositions filter
  (_valid_symbols_at_date) then restricts eligibility at each rebalance date
  to only stocks actually in the index at that time.

Runtime flow:
  1. Baseline parquet + delta cache (if present) → memory (once per container).
  2. Compute gap vs today_IST; if > 0, yfinance-download only the missing tail,
     merge in memory, and persist those rows to the delta cache parquet.
  3. Cache the merged per-symbol dict under today_IST for the rest of the day.

The committed parquet is rebuilt out-of-band by `scripts/refresh_backtest_parquet.py`.
After a rebuild the delta cache becomes redundant (overlapping rows are deduped on load).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
import yfinance as yf

from config import IST
from data import _load_constituents, get_last_valid_trading_date, load_nse_holidays  # noqa: F401

_NOOP_EMIT: Callable[[str, str], None] = lambda _lv, _msg: None


def _get_target_key(now: datetime | None = None) -> str:
    """Latest completed NSE session using the established 19:00 IST data cutoff."""
    now = now or datetime.now(IST)
    market_data_ready = (now.hour, now.minute) >= (19, 0)
    start = now.strftime("%Y-%m-%d") if market_data_ready else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return get_last_valid_trading_date(start, load_nse_holidays())


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
OHLCV_PARQUET = os.path.join(REPO_ROOT, "data", "backtest_history.parquet")
BENCH_PARQUET = os.path.join(REPO_ROOT, "data", "benchmarks.parquet")

# Gitignored local delta caches — accumulate yfinance tail rows across restarts.
DELTA_PARQUET = os.path.join(REPO_ROOT, "data", "backtest_delta.parquet")
BENCH_DELTA_PARQUET = os.path.join(REPO_ROOT, "data", "benchmarks_delta.parquet")

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

# Single-flight refreshes. Failed refreshes are deliberately not put in
# ``_merged_ohlcv`` so a later button click can retry without a process restart.
_ohlcv_refresh_latches: dict[str, threading.Event] = {}
_ohlcv_refresh_results: dict[str, "OHLCVLoadResult"] = {}


@dataclass
class DeltaFetchResult:
    data: pd.DataFrame
    requested_symbols: list[str]
    returned_symbols: list[str] = field(default_factory=list)
    attempts: int = 0
    error: str | None = None


@dataclass
class OHLCVLoadResult:
    """OHLCV data plus an explicit, observable freshness contract.

    Iteration preserves the legacy ``symbol_data, date, source = ...`` API.  The
    legacy date is the observed coverage date, never the requested cache key.
    """

    symbol_data: dict[str, pd.DataFrame]
    target_date: str
    actual_latest_date: str | None
    max_price_date: str | None
    source: str
    refresh_status: str
    refresh_error: str | None = None
    requested_symbols: list[str] = field(default_factory=list)
    updated_symbols: list[str] = field(default_factory=list)
    missing_target_symbols: list[str] = field(default_factory=list)
    stale_symbols: list[str] = field(default_factory=list)
    attempts: int = 0

    @property
    def is_fresh(self) -> bool:
        return (
            bool(self.symbol_data)
            and self.actual_latest_date == self.target_date
            and not self.missing_target_symbols
            and not self.stale_symbols
            and self.refresh_status in {"fresh", "not_needed", "memory"}
        )

    def __iter__(self):
        yield self.symbol_data
        yield self.actual_latest_date or self.target_date
        yield self.source


@dataclass
class BenchmarkLoadResult:
    series: dict[str, pd.Series]
    target_date: str
    actual_latest_date: str | None
    status: str
    missing: list[str] = field(default_factory=list)


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
    # Tier 2.5 — merge local delta cache (gitignored) to extend baseline without re-fetching.
    if os.path.exists(DELTA_PARQUET):
        try:
            delta_df = pd.read_parquet(DELTA_PARQUET)
            delta_df["date"] = pd.to_datetime(delta_df["date"])
            if not delta_df.empty:
                df = pd.concat([df, delta_df], ignore_index=True)
                df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
                df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
                emit("info", f"  📂 Delta cache: +{len(delta_df):,} rows through {delta_df['date'].max().date()}")
        except Exception as exc:
            emit("warning", f"⚠️ Delta cache unreadable, ignoring: {exc}")
    with _lock:
        _baseline_ohlcv = df
    emit("info", f"  ✅ {len(df):,} rows · {df['symbol'].nunique()} symbols · through {df['date'].max().date()}")
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
    # Tier 2.5 — merge benchmark delta cache.
    if os.path.exists(BENCH_DELTA_PARQUET):
        try:
            delta_df = pd.read_parquet(BENCH_DELTA_PARQUET)
            delta_df["date"] = pd.to_datetime(delta_df["date"])
            if not delta_df.empty:
                df = pd.concat([df, delta_df], ignore_index=True)
                df = df.drop_duplicates(subset=["date"], keep="last")
                df = df.sort_values("date").reset_index(drop=True)
        except Exception as exc:
            logging.warning("Benchmark delta cache unreadable, ignoring: %s", exc)
    with _lock:
        _baseline_bench = df
    return df


# ──────────────────────────────────────────────
# Delta cache writers (Tier 2.5 — gitignored local parquets)
# ──────────────────────────────────────────────
def _save_ohlcv_delta(new_df: pd.DataFrame, emit: Callable[[str, str], None]) -> None:
    """Persist freshly fetched OHLCV rows to the local delta cache (non-fatal on error)."""
    if new_df.empty:
        return
    with _lock:
        try:
            if os.path.exists(DELTA_PARQUET):
                existing = pd.read_parquet(DELTA_PARQUET)
                existing["date"] = pd.to_datetime(existing["date"])
                combined = pd.concat([existing, new_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=["symbol", "date"], keep="last")
                combined = combined.sort_values(["symbol", "date"]).reset_index(drop=True)
            else:
                combined = new_df.copy()
            combined.to_parquet(DELTA_PARQUET, index=False)
            emit("info", f"  💾 Delta cache saved ({len(combined):,} rows through {combined['date'].max().date()})")
        except Exception as exc:
            emit("warning", f"⚠️ Could not save delta cache: {exc}")


def _save_bench_delta(new_df: pd.DataFrame) -> None:
    """Persist freshly fetched benchmark rows to the local delta cache (non-fatal on error)."""
    if new_df.empty:
        return
    with _lock:
        try:
            if os.path.exists(BENCH_DELTA_PARQUET):
                existing = pd.read_parquet(BENCH_DELTA_PARQUET)
                existing["date"] = pd.to_datetime(existing["date"])
                combined = pd.concat([existing, new_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=["date"], keep="last")
                combined = combined.sort_values("date").reset_index(drop=True)
            else:
                combined = new_df.copy()
            combined.to_parquet(BENCH_DELTA_PARQUET, index=False)
        except Exception:
            pass  # non-fatal


# ──────────────────────────────────────────────
# Delta fetch (Tier 3 — yfinance, only truly new dates)
# ──────────────────────────────────────────────
def _fetch_ohlcv_delta(
    all_symbols: list[str],
    last_date: pd.Timestamp,
    today_key: str,
    emit: Callable[[str, str], None],
    max_attempts: int = 3,
) -> DeltaFetchResult:
    """Download the missing tail, retrying failures and partial symbol responses."""
    start_dt = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    end_dt = (datetime.strptime(today_key, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    empty = pd.DataFrame(columns=["symbol", "date", "Close", "High", "Volume"])
    if start_dt >= end_dt:
        return DeltaFetchResult(empty, list(all_symbols))

    requested = sorted(set(all_symbols))
    pending = set(requested)
    records: list[dict] = []
    errors: list[str] = []
    attempts = 0

    for attempt in range(1, max_attempts + 1):
        if not pending:
            break
        attempts = attempt
        if attempt > 1:
            delay = 0.5 * (2 ** (attempt - 2))
            emit("info", f"🔁 Yahoo retry {attempt}/{max_attempts} in {delay:g}s ({len(pending)} symbols)…")
            time.sleep(delay)

        # Smaller retry batches are more reliable when Yahoo partially rejects a
        # large multi-ticker request.
        pending_list = sorted(pending)
        batch_size = len(pending_list) if attempt == 1 else 100
        for offset in range(0, len(pending_list), batch_size):
            symbols = pending_list[offset : offset + batch_size]
            tickers = [f"{s}.NS" for s in symbols]
            emit(
                "info",
                f"🌐 Fetching Yahoo delta {start_dt} → {today_key} "
                f"({len(tickers)} symbols, attempt {attempt}/{max_attempts})…",
            )
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
                errors.append(str(exc))
                emit("warning", f"⚠️ Yahoo attempt {attempt} failed: {exc}")
                continue
            if raw is None or raw.empty:
                errors.append("Yahoo returned an empty response")
                emit("warning", f"⚠️ Yahoo attempt {attempt} returned no rows")
                continue

            if isinstance(raw.columns, pd.MultiIndex):
                level0 = set(raw.columns.get_level_values(0).unique().tolist())
                level1 = set(raw.columns.get_level_values(1).unique().tolist())
                available = (level0 | level1) & set(tickers)
            else:
                available = set(tickers)
            for ticker in tickers:
                if ticker not in available:
                    continue
                sym = ticker.removesuffix(".NS")
                try:
                    if isinstance(raw.columns, pd.MultiIndex) and ticker in level0:
                        sub = raw[ticker].dropna(how="all")
                    elif isinstance(raw.columns, pd.MultiIndex) and ticker in level1:
                        sub = raw.xs(ticker, axis=1, level=1).dropna(how="all")
                    else:
                        sub = raw.dropna(how="all")
                except (KeyError, TypeError):
                    continue
                sub = sub.copy()
                sub.columns = [c[0] if isinstance(c, tuple) else c for c in sub.columns]
                symbol_had_row = False
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
                    symbol_had_row = True
                if symbol_had_row:
                    pending.discard(sym)

    if not records:
        error = errors[-1] if errors else "Yahoo returned no usable OHLCV rows"
        return DeltaFetchResult(empty, requested, attempts=attempts, error=error)

    df = pd.DataFrame.from_records(records).drop_duplicates(subset=["symbol", "date"], keep="last")
    df["Close"] = df["Close"].astype("float32")
    df["High"] = df["High"].astype("float32")
    df["Volume"] = df["Volume"].astype("int64")
    returned = sorted(set(df["symbol"]))
    error = None
    if pending:
        sample = ", ".join(sorted(pending)[:20])
        suffix = f" (+{len(pending) - 20} more)" if len(pending) > 20 else ""
        error = f"Yahoo returned no usable rows for {sample}{suffix}"
    return DeltaFetchResult(df, requested, returned, attempts, error)


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
        for attempt in range(1, 4):
            if attempt > 1:
                time.sleep(0.5 * (2 ** (attempt - 2)))
            try:
                raw = yf.download(ticker, start=start_dt, end=end_dt, auto_adjust=True, progress=False)
                if raw is None or raw.empty:
                    raise RuntimeError("empty Yahoo response")
                raw.columns = [c[0] if isinstance(c, tuple) else c for c in raw.columns]
                s = raw["Close"].copy()
                s.index = pd.to_datetime(s.index)
                s.name = label
                series[label] = s.astype("float32")
                break
            except Exception as exc:
                emit("warning", f"⚠️ benchmark fetch {label} attempt {attempt}/3 failed: {exc}")
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
        sub["Volume"] = sub["Volume"].astype("Int64")
        result[sym] = sub
    return result


def _active_symbols(base: pd.DataFrame, global_max: pd.Timestamp) -> list[str]:
    """Symbols trading recently enough to merit a runtime tail refresh."""
    maxima = base.groupby("symbol", sort=False)["date"].max()
    cutoff = global_max - timedelta(days=14)
    return sorted(maxima[maxima >= cutoff].index.astype(str).tolist())


def _assess_ohlcv_freshness(
    merged: pd.DataFrame,
    target_key: str,
    required_symbols: list[str],
    source: str,
    fetch: DeltaFetchResult | None = None,
) -> OHLCVLoadResult:
    valid = merged[merged["Close"].notna()].copy()
    if "Volume" in valid.columns:
        valid = valid[pd.to_numeric(valid["Volume"], errors="coerce").fillna(0) > 0]
    max_price_date = valid["date"].max() if not valid.empty else None
    maxima = valid.groupby("symbol")["date"].max() if not valid.empty else pd.Series(dtype="datetime64[ns]")
    target = pd.Timestamp(target_key)
    required = sorted(set(required_symbols))
    missing_target = sorted(sym for sym in required if sym not in maxima.index or maxima[sym] < target)

    # A symbol more than three completed NSE sessions behind is explicitly stale.
    holidays = set(load_nse_holidays())
    cutoff = target
    sessions = 0
    while sessions < 3:
        cutoff -= timedelta(days=1)
        if cutoff.weekday() < 5 and cutoff.strftime("%Y-%m-%d") not in holidays:
            sessions += 1
    stale = sorted(sym for sym in required if sym not in maxima.index or maxima[sym] < cutoff)

    if required:
        observed = [maxima[sym] for sym in required if sym in maxima.index]
        actual = min(observed) if len(observed) == len(required) else None
    else:
        actual = max_price_date

    fetch_error = fetch.error if fetch else None
    if not required and max_price_date is None:
        status = "failed"
    elif missing_target:
        status = "failed" if fetch is not None and fetch.data.empty else "partial"
    elif fetch is None:
        status = "not_needed"
    else:
        status = "fresh"

    actual_key = actual.strftime("%Y-%m-%d") if actual is not None else None
    max_key = max_price_date.strftime("%Y-%m-%d") if max_price_date is not None else None
    return OHLCVLoadResult(
        symbol_data=_long_to_symbol_dict(merged),
        target_date=target_key,
        actual_latest_date=actual_key,
        max_price_date=max_key,
        source=source,
        refresh_status=status,
        refresh_error=fetch_error,
        requested_symbols=required,
        updated_symbols=sorted(set(fetch.returned_symbols) & set(required)) if fetch else [],
        missing_target_symbols=missing_target,
        stale_symbols=stale,
        attempts=fetch.attempts if fetch else 0,
    )


def load_ohlcv_for_backtest(
    emit: Callable[[str, str], None] = _NOOP_EMIT,
    required_symbols: list[str] | set[str] | None = None,
) -> OHLCVLoadResult:
    """
    Return data plus freshness metadata. Tuple unpacking remains backward compatible.
      source ∈ {'memory', 'parquet', 'parquet+delta', 'error'}

    Tier 1  today's merged cache hit → 'memory'
    Tier 2  parquet-only (no gap or delta fetch failed) → 'parquet'
    Tier 3  parquet + yfinance delta → 'parquet+delta'
    """
    target_key = _get_target_key()

    # Tier 1b + Tier 2 — load baseline from parquet
    try:
        base = _ensure_baseline_ohlcv(emit)
    except RuntimeError as exc:
        emit("error", f"❌ {exc}")
        return OHLCVLoadResult({}, target_key, None, None, "error", "failed", str(exc))

    global_max = pd.Timestamp(base["date"].max())
    active = _active_symbols(base, global_max)
    required = sorted(set(required_symbols if required_symbols is not None else active))

    # A hot cache is usable only if it satisfies this caller's required universe.
    with _lock:
        hit = _merged_ohlcv.get(target_key)
    if hit is not None:
        hit_long = pd.concat(
            [frame.reset_index().assign(symbol=sym) for sym, frame in hit.items()],
            ignore_index=True,
        )
        cached = _assess_ohlcv_freshness(hit_long, target_key, required, "memory")
        if cached.is_fresh:
            cached.refresh_status = "memory"
            return cached

    refresh_symbols = sorted(set(active) | set(required))
    maxima = base.groupby("symbol")["date"].max()
    refresh_maxima = [pd.Timestamp(maxima[s]) for s in refresh_symbols if s in maxima.index]
    last_date = min(refresh_maxima) if refresh_maxima else global_max
    gap_days = (datetime.strptime(target_key, "%Y-%m-%d") - last_date.to_pydatetime()).days

    if gap_days <= 0:
        merged = base
        source = "parquet"
        fetch = None
    else:
        emit("info", f"📅 Baseline through {last_date.date()} · target {target_key} · gap {gap_days}d")
        latch_key = target_key + ":" + ",".join(required)
        with _lock:
            latch = _ohlcv_refresh_latches.get(latch_key)
            if latch is None:
                latch = threading.Event()
                _ohlcv_refresh_latches[latch_key] = latch
                leader = True
            else:
                leader = False
        if not leader:
            emit("info", "⏳ An OHLCV refresh is already running — waiting…")
            latch.wait(timeout=300)
            with _lock:
                waited = _ohlcv_refresh_results.get(latch_key)
            if waited is not None:
                return waited

        fetch = _fetch_ohlcv_delta(refresh_symbols, last_date, target_key, emit)
        if fetch.data.empty:
            merged = base
            source = "parquet"
            emit("error", f"❌ Yahoo refresh unavailable; prices remain through {global_max.date()}")
        else:
            # dtype-align baseline chunk to match delta so concat stays float32
            merged = pd.concat([base, fetch.data], ignore_index=True)
            merged = merged.drop_duplicates(subset=["symbol", "date"], keep="last")
            merged = merged.sort_values(["symbol", "date"]).reset_index(drop=True)
            source = "parquet+delta"
            emit("info", f"  ✅ Merged {len(fetch.data):,} delta rows")
            _save_ohlcv_delta(fetch.data, emit)

    result = _assess_ohlcv_freshness(merged, target_key, required, source, fetch)
    if result.is_fresh:
        with _lock:
            _merged_ohlcv[target_key] = result.symbol_data
    elif result.missing_target_symbols:
        sample = ", ".join(result.missing_target_symbols[:20])
        suffix = (
            f" (+{len(result.missing_target_symbols) - 20} more)" if len(result.missing_target_symbols) > 20 else ""
        )
        emit("error", f"❌ Required target-session prices are missing: {sample}{suffix}")

    if gap_days > 0:
        with _lock:
            _ohlcv_refresh_results[latch_key] = result
            event = _ohlcv_refresh_latches.pop(latch_key, None)
            if event is not None:
                event.set()
    return result


def load_benchmark_series(with_status: bool = False) -> dict[str, pd.Series] | BenchmarkLoadResult:
    """Return close-price Series per benchmark label, indexed by date."""
    target_key = _get_target_key()
    with _lock:
        hit = _merged_bench.get(target_key)
    if hit is not None:
        actual = min((s.index.max() for s in hit.values() if not s.empty), default=None)
        result = BenchmarkLoadResult(
            hit,
            target_key,
            actual.strftime("%Y-%m-%d") if actual is not None else None,
            "memory",
        )
        return result if with_status else hit

    try:
        base = _ensure_baseline_bench(_NOOP_EMIT)
    except RuntimeError:
        result = BenchmarkLoadResult({}, target_key, None, "failed", list(BENCHMARK_TICKERS))
        return result if with_status else {}

    benchmark_maxima = [base.loc[base[col].notna(), "date"].max() for col in BENCHMARK_TICKERS if col in base]
    benchmark_maxima = [pd.Timestamp(value) for value in benchmark_maxima if pd.notna(value)]
    last_date = min(benchmark_maxima) if benchmark_maxima else pd.Timestamp(base["date"].max())
    gap_days = (datetime.strptime(target_key, "%Y-%m-%d") - last_date.to_pydatetime()).days

    fetched = False
    if gap_days > 0:
        delta = _fetch_bench_delta(last_date, target_key, _NOOP_EMIT)
        if not delta.empty:
            fetched = True
            delta["date"] = pd.to_datetime(delta["date"])
            base = pd.concat([base, delta], ignore_index=True)
            base = base.drop_duplicates(subset=["date"], keep="last").sort_values("date")
            _save_bench_delta(delta)

    result: dict[str, pd.Series] = {}
    for col in base.columns:
        if col == "date":
            continue
        s = base.set_index("date")[col].dropna()
        s.name = col
        result[col] = s

    missing = sorted(
        label
        for label in BENCHMARK_TICKERS
        if label not in result or result[label].empty or result[label].index.max() < pd.Timestamp(target_key)
    )
    observed = [s.index.max() for s in result.values() if not s.empty]
    actual = min(observed) if len(observed) == len(BENCHMARK_TICKERS) else None
    status = "fresh" if not missing else ("partial" if result else "failed")
    if not missing:
        with _lock:
            _merged_bench[target_key] = result
    load_result = BenchmarkLoadResult(
        result,
        target_key,
        actual.strftime("%Y-%m-%d") if actual is not None else None,
        "not_needed" if not fetched and not missing else status,
        missing,
    )
    return load_result if with_status else result


def sync_benchmark_data() -> bool:
    """
    No-op — benchmarks live in the parquet and are refreshed via
    `load_benchmark_series` on first access. Kept for API parity with data.py
    so existing workers can be rewired without signature churn.
    """
    return True
