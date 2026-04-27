"""
Batch backtest: grid search over M, N, rebalance frequency, and band rule.

Usage:
  python batch_backtest.py [--sort-method METHOD] [--output FILE]

Fixed parameters:
  Universe    : All 5 indices (Nifty 750)
  Date range  : 2017-07-01 to 2026-04-27
  Tx cost     : 0.1% one-way (0.001 fraction)
  STCG        : 20%  | LTCG: 12.5%
  Min history : 252 trading days
  Compositions: enabled (survivorship-bias mitigation)

Grid:
  M in {15, 20, 25, 30}
  N in {30, 40, 50, 60, 75, 100}, N > M
  Freq in {weekly, biweekly, monthly, quarterly, half-yearly}
  Band rules: classic + displacement

Total backtest calls: 23 M/N pairs x 5 freqs x 2 bands = 230
"""

import argparse
import itertools
import sys
import time
import warnings

import pandas as pd

warnings.filterwarnings("ignore")

# ── CLI args ──────────────────────────────────────────────────────────────────
_parser = argparse.ArgumentParser()
_parser.add_argument(
    "--sort-method",
    default="Average of 3/6/9/12 months",
    help="Sharpe ranking method passed to the backtest engine",
)
_parser.add_argument(
    "--output",
    default="batch_results.csv",
    help="Output CSV file path",
)
_args = _parser.parse_args()

SORT_METHOD = _args.sort_method
FINAL_FILE = _args.output
CHECKPOINT_FILE = FINAL_FILE.replace(".csv", "_ckpt.csv")

# ── fixed grid parameters ─────────────────────────────────────────────────────
M_VALUES = [15, 20, 25, 30]
N_VALUES = [30, 40, 50, 60, 75, 100]
FREQS = ["weekly", "biweekly", "monthly", "quarterly", "half-yearly"]
BANDS = ["classic", "displacement"]
ALL_5_INDICES = [
    "Nifty 50",
    "Nifty Next 50",
    "Nifty Midcap 150",
    "Nifty Smallcap 250",
    "Nifty Microcap 250",
]
START_DATE = "2017-07-01"
END_DATE = "2026-04-27"
TX_COST = 0.001
MIN_HISTORY = 252
STCG = 0.20
LTCG = 0.125
INITIAL_CAPITAL = 1_000_000.0

STAT_COLS = [
    "CAGR (%)",
    "Sharpe",
    "Max Drawdown (%)",
    "Calmar",
    "Sortino",
    "Final NAV",
    "Avg Holdings",
    "Avg Turnover (%)",
    "Cost Drag (%)",
    "Tax Drag (%)",
    "Brokerage Drag (%)",
]


def emit(level: str, msg: str) -> None:
    safe = msg.encode("ascii", errors="replace").decode("ascii")
    print(f"  [{level.upper()}] {safe}", flush=True)


def load_data():
    print(f"Loading data for sort_method={SORT_METHOD!r} ...", flush=True)
    from data_backtest import (
        _load_constituents,
        load_benchmark_series,
        load_compositions,
        load_ohlcv_for_backtest,
        sync_benchmark_data,
    )

    sync_benchmark_data()
    symbol_data, ohlcv_date, src = load_ohlcv_for_backtest(emit=emit)
    if not symbol_data:
        sys.exit("ERROR: OHLCV parquet missing. Run scripts/refresh_backtest_parquet.py first.")

    constituents = _load_constituents()
    allowed = {s for idx, syms in constituents.items() if idx in ALL_5_INDICES for s in syms}
    symbol_data = {s: df for s, df in symbol_data.items() if s in allowed}

    compositions_df = load_compositions()
    benchmarks = load_benchmark_series()
    print(f"  {len(symbol_data)} symbols | as-of {ohlcv_date} | source={src}", flush=True)
    return symbol_data, compositions_df, benchmarks


def run_one(symbol_data, compositions_df, benchmarks, m, n, freq, band):
    from backtest_engine import run_backtest

    result = run_backtest(
        all_ohlcv=symbol_data,
        benchmarks=benchmarks,
        m=m,
        n=n,
        rebalance_freq=freq,
        sort_method=SORT_METHOD,
        start_date=START_DATE,
        end_date=END_DATE,
        compositions_df=compositions_df,
        index_names=ALL_5_INDICES,
        transaction_cost_pct=TX_COST,
        min_history_days=MIN_HISTORY,
        apply_volume_filter=True,
        brokerage_per_sale=0.0,
        initial_capital=INITIAL_CAPITAL,
        ltcg_rate=LTCG,
        stcg_rate=STCG,
        band_rule=band,
    )
    if "error" in result:
        raise RuntimeError(result["error"])
    return result


def extract_rows(result, m, n, freq, band):
    rows = []
    stats_df = result["stats"]
    for variant in ["Full Rebalance", "Marginal Rebalance"]:
        if variant not in stats_df.index:
            continue
        s = stats_df.loc[variant]
        row = {
            "SortMethod": SORT_METHOD,
            "M": m,
            "N": n,
            "Freq": freq,
            "Band": band,
            "Variant": variant,
        }
        for col in STAT_COLS:
            row[col] = s.get(col, float("nan"))
        rows.append(row)
    return rows


def build_grid():
    return [(m, n, freq, band) for m, n, freq, band in itertools.product(M_VALUES, N_VALUES, FREQS, BANDS) if n > m]


def main():
    t0 = time.time()
    print(f"\n{'='*80}", flush=True)
    print(f"  BATCH BACKTEST  |  sort_method={SORT_METHOD!r}", flush=True)
    print(f"  output -> {FINAL_FILE}", flush=True)
    print(f"{'='*80}\n", flush=True)

    symbol_data, compositions_df, benchmarks = load_data()

    combos = build_grid()
    total = len(combos)
    print(f"\nGrid: {total} runs\n", flush=True)

    all_rows = []
    for i, (m, n, freq, band) in enumerate(combos, 1):
        label = f"M={m} N={n} freq={freq} band={band}"
        print(f"[{i:3d}/{total}] {label}", end="", flush=True)
        t1 = time.time()
        try:
            result = run_one(symbol_data, compositions_df, benchmarks, m, n, freq, band)
            rows = extract_rows(result, m, n, freq, band)
            all_rows.extend(rows)
            elapsed = time.time() - t1
            sharpe_vals = [r["Sharpe"] for r in rows if not pd.isna(r.get("Sharpe", float("nan")))]
            sharpe_str = " | ".join(f"{s:.3f}" for s in sharpe_vals)
            print(f"  OK {elapsed:.1f}s  Sharpe=[{sharpe_str}]", flush=True)
        except Exception as exc:
            elapsed = time.time() - t1
            print(f"  ERR {elapsed:.1f}s  {exc}", flush=True)

        if all_rows:
            pd.DataFrame(all_rows).to_csv(CHECKPOINT_FILE, index=False)

    if not all_rows:
        print("No results collected -- aborting.")
        return

    df = pd.DataFrame(all_rows)
    df.to_csv(FINAL_FILE, index=False)
    elapsed_total = (time.time() - t0) / 60
    print(f"\nDone. {len(df)} rows -> {FINAL_FILE}  ({elapsed_total:.1f} min)")


if __name__ == "__main__":
    main()
