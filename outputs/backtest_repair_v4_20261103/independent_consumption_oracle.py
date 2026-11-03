"""Independent raw-count and hand-window metric checks; no engine/metric imports."""

from pathlib import Path
import json
import sys

import numpy as np
import pandas as pd

OUT = Path(__file__).parent


def main(model_name, audit_name):
    target = OUT / audit_name
    prices = pd.read_parquet(OUT / model_name / "primary_adjusted_research_candidate.parquet")
    calendar = pd.DatetimeIndex(sorted(prices.date.unique()))
    groups = {s: g.sort_values("date") for s, g in prices.groupby("research_security_id")}
    signals = pd.read_parquet(target / "all_scheduled_signal_consumption.parquet")
    checked = 0
    for family, rows in signals.groupby("research_security_id"):
        g = groups[family]
        counts = (g.Close.gt(0) & g.Volume.gt(0)).cumsum().to_numpy()
        positions = np.searchsorted(g.date.to_numpy(), rows.rank_date.to_numpy(), side="right") - 1
        actual = np.where(positions < 0, 0, counts[np.maximum(positions, 0)])
        assert np.array_equal(actual, rows["count"]), family
        checked += len(rows)
    warmup = pd.read_csv(target / "initial_consumption_warmup.csv", parse_dates=["rank_date"])
    scalar_checks = 0
    for r in warmup.to_dict("records"):
        g = groups[r["research_security_id"]].set_index("date")
        end = calendar.get_loc(r["rank_date"]) + 1
        days = calendar[end - 252 : end]
        w = g.reindex(days)
        close = w.Close.where(w.Close.gt(0) & w.Volume.gt(0))
        expected = {}
        for name, n in [("Sharpe_3M", 63), ("Sharpe_6M", 126), ("Sharpe_9M", 189), ("Sharpe_1Y", 252)]:
            returns = close.iloc[-n:].pct_change(fill_method=None).iloc[1:]
            expected[name] = (
                round(returns.mean() / returns.std(ddof=1) * np.sqrt(252), 3)
                if len(returns) == n - 1 and returns.notna().all() and returns.std(ddof=1) > 0
                else np.nan
            )
        expected["DMA200"] = round(close.iloc[-200:].mean(), 2) if close.iloc[-200:].notna().all() else np.nan
        expected["Vol_Median"] = w.Volume.median() if close.notna().all() else np.nan
        expected["52w_High"] = round(w.High.max(), 2) if close.notna().all() else np.nan
        change = close.diff().iloc[1:]
        expected["Pos_Days_12M"] = round((change > 0).sum() / 251 * 100, 0) if change.notna().all() else np.nan
        returns = close.pct_change(fill_method=None).iloc[1:] * 100
        hit = sum(
            ((returns.abs() >= level - 0.1) & (returns.abs() <= level + 0.1)).astype(int) for level in [5.0, 10.0, 20.0]
        )
        expected["Circuit_Count"] = float(hit.sum()) if returns.notna().all() else np.nan
        for column, value in expected.items():
            assert np.isclose(value, r[column], equal_nan=True, atol=1e-8), (
                r["research_security_id"],
                r["rank_date"],
                column,
                value,
                r[column],
            )
            scalar_checks += 1
    report = dict(
        passed=True,
        source_supported_history_count_checks=checked,
        independent_initial_metric_scalar_checks=scalar_checks,
        production_engine_or_metric_imported=False,
        acceptance=False,
        limits="Computability and arithmetic only; unsupported corporate actions and source disagreements remain separate blockers",
    )
    (target / "independent_consumption_reconciliation.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(*sys.argv[1:])
