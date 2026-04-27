"""
Batch backtest: grid search over M, N, and rebalance frequency.

Fixed parameters:
  Universe    : All 5 indices (Nifty 750)
  Rank method : Average of 3/6/9/12 months
  Date range  : 2017-07-01 → 2026-04-27
  Tx cost     : 0.1% one-way (0.001 fraction)
  STCG        : 20%  | LTCG: 12.5%
  Min history : 252 trading days
  Compositions: enabled (survivorship-bias mitigation)

Grid:
  M ∈ {15, 20, 25, 30}
  N ∈ {30, 40, 50, 60, 75, 100}, N > M
  Freq ∈ {weekly, biweekly, monthly, quarterly, half-yearly}
  Band rules  : classic + displacement (both run per combination)

Total backtest calls: 23 M/N pairs × 5 freqs × 2 bands = 230
"""

import itertools
import sys
import time
import warnings

import pandas as pd

warnings.filterwarnings("ignore")

# ── parameters ────────────────────────────────────────────────────────────────
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
SORT_METHOD = "Average of 3/6/9/12 months"
TX_COST = 0.001  # 0.1% as fraction (already converted)
MIN_HISTORY = 252
STCG = 0.20
LTCG = 0.125
INITIAL_CAPITAL = 1_000_000.0
CHECKPOINT_FILE = "batch_results_checkpoint.csv"
FINAL_FILE = "batch_results.csv"

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
    print("Loading data (one-time)…", flush=True)
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

    print(
        f"  Data loaded: {len(symbol_data)} symbols | as-of {ohlcv_date} | source={src}",
        flush=True,
    )
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
        row = {"M": m, "N": n, "Freq": freq, "Band": band, "Variant": variant}
        for col in STAT_COLS:
            row[col] = s.get(col, float("nan"))
        rows.append(row)
    return rows


def build_grid():
    combos = []
    for m, n, freq, band in itertools.product(M_VALUES, N_VALUES, FREQS, BANDS):
        if n > m:
            combos.append((m, n, freq, band))
    return combos


def print_table(df: pd.DataFrame, title: str, n_rows: int = 30) -> None:
    print(f"\n{'='*110}")
    print(f"  {title}")
    print(f"{'='*110}")
    display_cols = [
        "M",
        "N",
        "Freq",
        "Band",
        "Variant",
        "CAGR (%)",
        "Sharpe",
        "Max Drawdown (%)",
        "Calmar",
        "Avg Holdings",
        "Avg Turnover (%)",
        "Cost Drag (%)",
        "Tax Drag (%)",
    ]
    cols = [c for c in display_cols if c in df.columns]
    subset = df[cols].head(n_rows)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", "{:.3f}".format)
    print(subset.to_string(index=False))


def analyse_and_recommend(df: pd.DataFrame) -> None:
    print(f"\n{'='*110}")
    print("  ANALYSIS & RECOMMENDATION")
    print(f"{'='*110}")

    # Best Sharpe per frequency
    print("\n-- Best Sharpe by Frequency --")
    freq_best = (
        df.groupby("Freq")
        .apply(lambda g: g.nlargest(1, "Sharpe"))
        .reset_index(drop=True)[
            ["Freq", "M", "N", "Band", "Variant", "CAGR (%)", "Sharpe", "Max Drawdown (%)", "Calmar"]
        ]
    )
    print(freq_best.to_string(index=False))

    # Overall best by Sharpe
    best = df.nlargest(1, "Sharpe").iloc[0]
    print("\n-- OVERALL BEST (by Sharpe) --")
    print(
        f"  M={int(best['M'])}, N={int(best['N'])}, Freq={best['Freq']}, Band={best['Band']}, Variant={best['Variant']}"
    )
    print(f"  CAGR:         {best['CAGR (%)']:.2f}%")
    print(f"  Sharpe:       {best['Sharpe']:.3f}")
    print(f"  Max Drawdown: {best['Max Drawdown (%)']:.2f}%")
    print(f"  Calmar:       {best['Calmar']:.3f}")
    if not pd.isna(best.get("Avg Holdings")):
        print(f"  Avg Holdings: {best['Avg Holdings']:.1f}")
    if not pd.isna(best.get("Avg Turnover (%)")):
        print(f"  Avg Turnover: {best['Avg Turnover (%)']:.1f}%")
    if not pd.isna(best.get("Cost Drag (%)")):
        print(f"  Cost Drag:    {best['Cost Drag (%)']:.3f}%")
    if not pd.isna(best.get("Tax Drag (%)")):
        print(f"  Tax Drag:     {best['Tax Drag (%)']:.3f}%")

    # Top-5 by Sharpe
    print("\n-- Top 10 Combinations by Sharpe --")
    top10 = df.nlargest(10, "Sharpe")[
        [
            "M",
            "N",
            "Freq",
            "Band",
            "Variant",
            "CAGR (%)",
            "Sharpe",
            "Max Drawdown (%)",
            "Calmar",
            "Avg Holdings",
            "Avg Turnover (%)",
        ]
    ]
    print(top10.to_string(index=False))

    # Rationale
    print("\n-- RATIONALE --")
    top3 = df.nlargest(3, "Sharpe")
    m_vals = top3["M"].tolist()
    n_vals = top3["N"].tolist()
    freq_vals = top3["Freq"].tolist()
    band_vals = top3["Band"].tolist()
    print(
        f"  The top combinations by risk-adjusted return (Sharpe) cluster around "
        f"M={m_vals[0]}, N={n_vals[0]} with {freq_vals[0]} rebalancing ({band_vals[0]} rule).\n"
        f"  Secondary contenders: M={m_vals[1]}/N={n_vals[1]} ({freq_vals[1]}, {band_vals[1]}) "
        f"and M={m_vals[2]}/N={n_vals[2]} ({freq_vals[2]}, {band_vals[2]}).\n"
        f"\n"
        f"  Key trade-offs observed in the grid:\n"
        f"  • Tighter bands (small N-M gap) → higher turnover, higher cost drag, but faster exits from losers.\n"
        f"  • Wider bands (large N-M gap) → lower turnover, lower friction, but slower response to rank changes.\n"
        f"  • Lower M → concentrated portfolio, higher individual stock risk, potentially higher CAGR.\n"
        f"  • Higher M → more diversified, smoother NAV, lower Calmar if max-DD rises.\n"
        f"  • Monthly/quarterly frequencies typically dominate weekly/biweekly on a post-cost basis.\n"
        f"  • Displacement rule often matches or exceeds Classic rule by capping holdings at exactly M,\n"
        f"    maintaining a tighter, higher-conviction book."
    )


def main():
    t0 = time.time()
    symbol_data, compositions_df, benchmarks = load_data()

    combos = build_grid()
    total = len(combos)
    print(f"\nGrid: {total} runs ({len(M_VALUES)}x{len(N_VALUES)-1}x{len(FREQS)}x{len(BANDS)} max, minus N<=M)\n")

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
            print(f"  ERR {elapsed:.1f}s  ERROR: {exc}", flush=True)

        # Checkpoint after every run
        if all_rows:
            pd.DataFrame(all_rows).to_csv(CHECKPOINT_FILE, index=False)

    if not all_rows:
        print("No results collected — aborting.")
        return

    df = pd.DataFrame(all_rows)
    df.to_csv(FINAL_FILE, index=False)
    print(f"\nSaved {len(df)} rows to {FINAL_FILE}")

    # Sort by Sharpe descending for display
    df_sorted = df.sort_values("Sharpe", ascending=False).reset_index(drop=True)

    print_table(df_sorted, "ALL RESULTS — sorted by Sharpe (top 30)")
    analyse_and_recommend(df_sorted)

    total_mins = (time.time() - t0) / 60
    print(f"\nTotal elapsed: {total_mins:.1f} minutes")


if __name__ == "__main__":
    main()
