"""Freeze exact configurations before any new performance; not permission to run."""

from pathlib import Path
import dataclasses
import hashlib
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from backtest_engine import BacktestConfig

OUT = Path(__file__).parent
quality = dict(
    min_annual_return=7.0,
    pct_from_52w_high=25.0,
    max_circuits=18,
    close_above_200dma=True,
    pos_days_3m_min=45.0,
    pos_days_6m_min=45.0,
    pos_days_12m_min=45.0,
)
profiles = []
for frequency in ["weekly", "monthly"]:
    for name, filters in [("original_batch_defaults", {}), ("separate_quality_diagnostic_sensitivity", quality)]:
        cfg = BacktestConfig(
            m=15,
            n=30,
            rebalance_freq=frequency,
            sort_method="Average of 3/6/9/12 months",
            start_date="2017-07-01",
            end_date="2020-09-01",
            index_names=["NIFTY 500"],
            min_history_days=252,
            apply_volume_filter=True,
            minimum_median_volume=100000.0,
            transaction_cost_pct=0.001,
            brokerage_per_sale=0.0,
            initial_capital=1000000.0,
            ltcg_rate=0.125,
            stcg_rate=0.20,
            band_rule="classic",
            stage2_drop_exit=False,
            max_position_pct=None,
            **filters
        )
        profiles.append(
            dict(
                name=name,
                config=dataclasses.asdict(cfg),
                variants=["Full Rebalance", "Marginal Rebalance", "Prop Rebalance"],
            )
        )
sources = [
    "run_batch_csv.py",
    "outputs/backtest_repair_v1_20261102/backtest_engine_baseline.py",
    "outputs/backtest_repair_v1_20261102/run_accounting_diagnostic.py",
    "backtest_engine.py",
    "backtest_accounting.py",
    "momentum_engine.py",
    "MoMo/backtest/MISSING_DATA_POLICY.md",
]
manifest = dict(
    schema=3,
    baseline_commit="bcdb6fb",
    supersedes=["frozen_comparison_plan.json", "frozen_comparison_plan_v2.json"],
    clarification="Original run_batch_csv.py inherits disabled quality defaults. The 7/25/18/45 filters belong to the prior diagnostic, not original batch defaults. Both exact configurations are frozen before new performance.",
    status="blocked before any new strategy performance",
    profiles=profiles,
    membership="complete candidate_v3 economic_ex_date NIFTY500; no survivor deletion",
    price_basis="nse-cash-split-research-v1 candidate, currently unaccepted; no raw/adjusted splice",
    warmup="252 supported observations plus complete 252-session metric windows; audit 750 default separately",
    execution="positive current source quotes only in repaired app; blocked sale retains quantity and explicit stale-mark diagnostics",
    MoMo_adapter="unimplemented: standing MISSING_DATA_POLICY assumes disclosed last-known-price sales; requires a common explicit policy before comparison",
    annualization="elapsed calendar days",
    tax_basis="same stated adjusted-unit research FIFO model, not nominal statutory tax basis",
    exclusions="No security deleted for unavailable history; no synthetic pre-launch Microcap or independent parent components",
    comparisons=[
        "old/repaired data effect on identical dates and profile if legitimate overlap",
        "MoMo/app only after identical supported data, strategy and execution-policy adapter",
    ],
    limitations="NIFTY500-only bounded study cannot repair original five-component batch or match headline performance",
    strategy_comparison_run=False,
    source_sha256={p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in sources},
)
name = sys.argv[1] if len(sys.argv) > 1 else "frozen_comparison_plan_v3.json"
if name != "frozen_comparison_plan_v3.json":
    manifest["schema"] = 4
    manifest["supersedes"].append("frozen_comparison_plan_v3.json")
    manifest["calculation_repair"] = (
        "Independent hand-window audit proved reversed lower-circuit inequalities in scalar/vector scoring; corrected before performance. Accounting engine unchanged."
    )
    for path in ["data.py", "app.py", "app_backtest.py", "backtest_provenance.py"]:
        manifest["source_sha256"][path] = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
with (OUT / name).open("x") as f:
    json.dump(manifest, f, indent=2, default=str)
