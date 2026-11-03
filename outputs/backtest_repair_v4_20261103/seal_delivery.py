"""Seal a local review bundle after behavioral and independent checks pass."""

from pathlib import Path
import hashlib
import json
import platform
import shutil
import subprocess
import sys

import numpy as np
import pandas as pd

OUT = Path(__file__).parent
ROOT = OUT.parents[1]
MODEL = OUT / "primary_model_v8"
AUDIT = OUT / "consumption_audit_v7"
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
read = lambda p: json.loads(Path(p).read_text())


def main():
    if (OUT / "delivery_manifest.json").exists():
        raise FileExistsError("Already sealed")
    summary = read(AUDIT / "summary.json")
    independent = read(AUDIT / "independent_consumption_reconciliation.json")
    arithmetic = read(OUT / "independent_primary_model_v8/independent_primary_arithmetic.json")
    lineage = read(OUT / "independent_primary_model_v8/independent_lineage_reconciliation.json")
    calendar = read(OUT / "independent_primary_model_v8/independent_warmup_calendar.json")
    membership = read(OUT / "independent_membership_reconciliation.json")
    accounting = read(OUT / "export_recheck.json")
    assert independent["passed"] and arithmetic["passed"] and lineage["passed"] and membership["passed"]
    assert not calendar["missing"] and not calendar["unexpected"]
    assert all(c["passed"] for c in accounting["reconciliation"].values())
    assert "485 passed" in (OUT / "full_tests_acceptance.log").read_text()
    assert not (OUT / "lint_acceptance.log").read_text().strip()
    assert not (OUT / "import_order_acceptance.log").read_text().strip()
    for r in read(OUT / "protected_baseline.json"):
        assert sha(ROOT / r["path"]) == r["sha256"]
    previous = ROOT / "outputs/backtest_repair_v3_20261103"
    prior = read(previous / "delivery_manifest.json")["sha256"]
    assert all(sha(previous / p) == digest for p, digest in prior.items())
    model = read(MODEL / "manifest.json")
    for field in ["source_hashes", "warmup_input_hashes"]:
        for path, digest in model[field].items():
            assert sha(ROOT / path) == digest, (field, path)
    assert pd.read_csv(MODEL / "unattributed_primary_action_records.csv").empty
    plan = read(OUT / "frozen_comparison_plan_v4.json")
    assert all(sha(ROOT / p) == digest for p, digest in plan["source_sha256"].items())
    assert not summary["accepted"] and not summary["strategy_comparison_run"]

    before = OUT / "source_before"
    before.mkdir(exist_ok=False)
    old = subprocess.check_output(["git", "show", "bcdb6fb:momentum_engine.py"], cwd=ROOT)
    (before / "momentum_engine.py").write_bytes(old)
    shutil.copyfile(
        OUT / "consumption_audit_v4/initial_consumption_warmup.csv", before / "initial_consumption_warmup.csv"
    )
    old_scores = pd.read_csv(before / "initial_consumption_warmup.csv")
    new_scores = pd.read_csv(AUDIT / "initial_consumption_warmup.csv")
    keys = ["frequency", "research_security_id", "rank_date"]
    comparison = old_scores.merge(new_scores, on=keys, suffixes=("_before", "_after"), validate="one_to_one")
    for column in [
        "Sharpe_3M",
        "Sharpe_6M",
        "Sharpe_9M",
        "Sharpe_1Y",
        "DMA200",
        "Vol_Median",
        "52w_High",
        "Pos_Days_12M",
    ]:
        assert np.allclose(comparison[column + "_before"], comparison[column + "_after"], equal_nan=True), column
    changed = ~np.isclose(comparison.Circuit_Count_before, comparison.Circuit_Count_after, equal_nan=True)
    comparison.loc[changed, keys + ["Circuit_Count_before", "Circuit_Count_after"]].to_csv(
        OUT / "circuit_count_repair_effect.csv", index=False
    )
    defect = dict(
        baseline_commit="bcdb6fb",
        baseline_git_blob_sha256=hashlib.sha256(old).hexdigest(),
        reproduction="SJVN / INE002L01015, 2017-05-23 return -4.915515%; weekly first rank 2017-07-06: lower-circuit count 0 before, 1 after",
        root_cause="Reversed negative-band inequalities were mutually exclusive in both scalar and vector paths",
        changed_initial_security_frequency_cases=int(changed.sum()),
        other_recorded_initial_signal_metrics_unchanged=True,
        portfolio_performance_run=False,
    )
    (OUT / "scoring_defect_evidence.json").write_text(json.dumps(defect, indent=2))

    restrictions = [
        r
        for r in read(previous / "candidate_v3/reviewed_lifecycle_decisions.json")
        if r["kind"] == "reconstruction_with_conditional_lock_in"
    ]
    (OUT / "tradability_restrictions.json").write_text(
        json.dumps(
            dict(
                status="nominal execution not supported",
                records=restrictions,
                policy="Identifier continuity only; no split multiplication. Conditional lock-in not erased. Adjusted research quantities are not nominal holdings eligible for the scheme threshold.",
            ),
            indent=2,
        )
    )
    residual = pd.read_csv(AUDIT / "scoped_unresolved_actions.csv")
    active = residual[residual.potentially_consumed]
    seams = pd.read_csv(AUDIT / "opposing_vendor_close_factor_seams.csv", parse_dates=["date"])
    warmup = new_scores.groupby("frequency")[["passes_original_count", "passes_engine_default_count"]].sum()
    marks = pd.read_csv(AUDIT / "potential_post_exit_missing_marks.csv")
    blockers = dict(
        scope=["2017-07-01", "2020-09-01"],
        model=model["model"],
        accepted=False,
        source_revisions_unchanged=accounting["source_revisions"],
        unresolved_consumption_action_records=len(active),
        affected_action_lineages=active.research_security_id.nunique(),
        opposing_primary_range_rows=summary["flags_primary_oppose"],
        opposing_vendor_factor_intervals_all_histories=len(seams),
        opposing_vendor_factor_intervals_after_earliest_warmup=int(seams.date.ge("2016-06-30").sum()),
        potential_unsold_exit_securities=["TECHNO", "CAPF"],
        scenario_missing_mark_rows=len(marks),
        unique_missing_security_mark_days=len(marks.drop_duplicates(["research_security_id", "date"])),
        publication="not performed; unaccepted price candidate is not consumed by app",
        app_changes="Shared circuit calculation repaired; screener and backtest cached calculations invalidated by calculation fingerprint",
        comparison="not run; price/action/source validation and common MoMo execution policy remain unresolved",
        next_required_work="Adjudicate retained action/source-seam worklists using security-specific primary terms or a verifiable adjusted-history provider. Model TECHNO/CAPF ownership conversion and delayed-sale exposure. Bind a common MoMo/app execution convention before a matched comparison.",
        supported_now=[
            "Complete reconciled membership evidence",
            "Dated primary observation layer and tested explicit research-price construction",
            "Independent signal/arithmetic checks",
            "Independent reconciliation of the retained 2024 accounting diagnostic",
        ],
        unsupported=[
            "Full-scope unbiased 2017 portfolio comparison",
            "Original five-component historical batch repair",
            "Realistic nominal execution and statutory tax certification",
            "Price-available survivor-subset performance claims",
        ],
    )
    (OUT / "publication_readiness.json").write_text(json.dumps(blockers, indent=2))

    source = OUT / "source_used"
    source.mkdir(exist_ok=False)
    paths = [
        "app.py",
        "app_backtest.py",
        "data.py",
        "momentum_engine.py",
        "backtest_provenance.py",
        "historical_price_model.py",
        "backtest_engine.py",
        "backtest_accounting.py",
        "equity_data.py",
        "momentum_ranking.py",
        "config.py",
        "tests/test_historical_price_model.py",
        "tests/test_momentum_circuit_oracle.py",
        "tests/test_calculation_cache_revision.py",
        "tests/test_screener_freshness.py",
        "MoMo/backtest/MISSING_DATA_POLICY.md",
    ]
    for path in paths:
        dest = source / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / path, dest)
    environment = dict(
        python=sys.version,
        platform=platform.platform(),
        pandas=pd.__version__,
        replay="Retained primary/provider bytes; fresh providers may restate history",
        runtime="Use a fresh app process to load changed calculation code; no running app, remote deployment or live checkpoint was advanced",
    )
    (OUT / "environment.json").write_text(json.dumps(environment, indent=2))
    report = f"""# Round 4: price completion and independent consumption validation

Continued from `bcdb6fb`. **The scoring defect is repaired and integrated into app calculation/cache paths. The historical price candidate is still unaccepted; publication and a fair new strategy comparison remain incomplete.** No accepted market data, membership source, workbook, accounting engine, Live Signal checkpoint or earlier sealed result was changed. No strategy performance matrix was run.

## Implemented accuracy repairs

`momentum_engine.py` had mutually exclusive lower-circuit inequalities in both scalar and vector scoring. A source-backed SJVN observation returning approximately −4.915515% on 2017-05-23 produced zero lower-circuit hits for the first weekly rank window. The independent hand-window oracle expected one. Both paths now count negative bands symmetrically with positive bands. Direct tests use known inside/outside changes around 5%, 10% and 20%, independently of either implementation.

The correction changes circuit counts in **{int(changed.sum())} initial security/frequency cases**; the other recorded initial signal metrics are unchanged. `circuit_count_repair_effect.csv` and `source_before/` retain the before/after evidence. This is a signal correction, not a measured CAGR effect.

The screener memory key and displayed momentum results now bind to the calculation fingerprint. The backtest UI rejects saved results whose engine/accounting/momentum source fingerprint is old, even with unchanged price revisions. Behavioral tests cover memory reuse/recalculation and actual Streamlit rejection of a stale backtest result. The accounting engine itself is byte-unchanged. A normal fresh app process is required to load local code changes; no app was deployed or restarted remotely.

`historical_price_model.py` adds explicit, offline primary cash-dividend, split and bonus research normalization. It preserves raw OHLC and nominal reported volume, deduplicates identical action terms, refuses ambiguous combined or complex entitlements, retains missing bars, and never credits split quantities or dividends again to an adjusted-unit portfolio. A known action may normalize earlier observations across a missing ex-date bar without creating that bar or making it executable. Debt series cannot join ordinary equity solely through an API ISIN. Factor-seam checks use a chronological union calendar and leave intervening missing dates visible.

## Coverage and evidence

| Item | Before | Round 4 staged result |
|---|---:|---:|
| Unavailable adjusted observations in canonical worklist | 33,754 | All 33,754 have a primary research candidate observation; **not accepted adjusted history** |
| Rows on missing 2019-02-13 / 2019-03-29 sessions | 1,002 | Both dates retained as real sessions with source quotes |
| Other missing rows / labels | 32,752 / 84 | All included, including 12 partial-gap labels outside failed-query list |
| Early AHLUCONT gaps | 45 | 45 candidate observations staged |
| OHLC flags | 246 | 149 primary-report agreements; 97 unresolved opposing primary ranges |
| Genuine no-positive-quote member days | 3 | Preserved for JKIL, 2017-08-08 through 2017-08-10; no fabricated fill |
| Complete row-level ledger | Incomplete query-only scope | 34,003 rows: 33,754 gaps + 246 flags + 3 genuine missing quotes |
| Newly acquired primary warmup calendar | None for acquired vendor histories | 862 independently reconciled sessions, 2014-01-01 through 2017-06-30 |
| Security-specific research candidate | Unvalidated gaps | {arithmetic['rows']:,} observations; 640 lineages; every row unaccepted |

The original NSE action API retains 13,130 records. Its ISIN can be obsolete or restated, and even bond records can carry an equity ISIN. The build excludes 360 non-equity-series records and preserves that exclusion register. Original dated action bulletins and source quote IDs support 546 action-attribution conflict reviews. There are 105 edge records representing 85 unique transitions: 23 retained lifecycle decisions and 62 independently checked additional price-history transitions. Five of the additional transitions require original bulletins because the API ISIN matches neither dated share identifier. No relevant API action record is left unattributed. Attribution does **not** imply validated entitlement terms: 126 decisions remain unresolved in the full acquired period, with **{len(active)} potentially consumed records across {active.research_security_id.nunique()} lineages**.

Both ordinary and DVR classes remain distinct. Economic and raw membership views retain their earlier evidence; all 39 monthly observations and both endpoints independently reconcile again. The warmup calendar includes the 2014 election closures and evidenced Saturday live sessions. The 2015 special session is corroborated by the NSE annual report's cash-underlying figure and primary CM archive; the retained currency circular is explicitly not treated as a cash-market circular.

On 2017-07-10, the daily CM and PR reports agree on Open/Close but differ on some intraday ranges. The SEBI technical-glitch inquiry is retained as common-date context; it does not establish which opposing range is correct. Among 231 affected worklist rows, 134 have agreeing primary OHLC and 97 have opposing primary ranges. All 15 flags on the other six dates have primary-report agreement. Opposing bytes and all row-level decisions remain retained; no accepted OHLC was overwritten.

## Explicit return and entitlement basis

The candidate is `nse-cash-split-research-v1`: pre-event OHLC is backward-normalized by `(P − cash)/(P × share_ratio)`, with independently recorded prior nominal close and terms. Volume stays nominal reported quantity. It is a stated research convention, not executable reinvestment or nominal statutory FIFO/tax basis. Unsupported rights, partly paid instruments, demergers, bonus debentures, ambiguous same-day distributions and missing cash terms remain named blockers; their value is not fitted to a close-price fall.

IDEA's 2019-03-29 fully paid rights are individually reviewed against the issuer release and NSE/FAOP/40530: 87 for 38 at total issue price ₹12.50. The theoretical research factor `(P + (87/38) × 12.5)/(P × (1 + 87/38))` matches the official indicative annex example of 0.591603. The annex was visually checked. This does not model a subscription cash payment or right-sale proceeds, and no such cash is credited again. It does not authorize the same formula for partly paid rights.

Yes Bank's 2020 change remains reconstruction with conditional lock-in, not a split. `tradability_restrictions.json` retains the NSE scheme evidence. The research series does not implement nominal restricted versus free holdings or the shareholder threshold. It cannot claim realistic nominal tradability.

Close-factor changes are checked against primary action factors, independently of OHLC shape. The complete vendor diagnostic covers the extended responses and original local dated joins. It retains **{len(seams):,} opposing intervals across all acquired histories**, of which {int(seams.date.ge('2016-06-30').sum()):,} are after the conservative first warmup boundary. These are source/convention discrepancies requiring adjudication, not proven portfolio losses or a blanket rejection based on price resemblance. Modern query keys remain proposals; a matching price path never certifies identity.

## Full consumption audit

The original batch uses **252 supported observations**, while the current engine default is **750**. Neither default nor requested start was changed. Weekly first execution is 2017-07-07, ranking on 2017-07-06, with its 252-session metric window starting 2016-06-30; a 750-observation grid starts 2014-06-20. Monthly first execution is 2017-07-31, ranking on 2017-07-28, with the metric window starting 2016-07-25 and the 750 grid starting 2014-07-14. Price history never grants earlier membership eligibility.

Of the 501 initial members, observed-count eligibility is {int(warmup.loc['weekly','passes_original_count'])}/501 weekly and {int(warmup.loc['monthly','passes_original_count'])}/501 monthly under 252; {int(warmup.loc['weekly','passes_engine_default_count'])}/501 and {int(warmup.loc['monthly','passes_engine_default_count'])}/501 under 750. Short or incomplete histories stay in membership and in the exception files. These counts do not establish that every short history is a legitimate IPO truncation, nor that every computable signal has correct entitlement adjustment.

All scheduled signal dates are audited: **102,705 security/frequency/date cases**, not only initial warmup. A separate oracle reconstructs every supported-history count and 9,018 initial scalar metrics without importing engine or metric functions. It discovered the circuit defect and now agrees with the correction. `scheduled_uncomputable_signals.csv` retains missing metric cases.

The potential post-index-exit audit covers 374 security/frequency exit cases. TECHNO and CAPF have no executable continuing old-security quote before scope end, creating four blocked scenarios over the two frequencies. Their 1,842 scenario mark rows correspond to 921 unique security/session days. Quantities cannot be assumed sold; merger/conversion ownership and marks need explicit treatment. These are **potential holding envelopes**, not invented actual portfolio holdings. Actual strategy-specific ownership and blocked-sale replay remain pending supported input acceptance.

## Independent checks and comparison status

The independent price arithmetic oracle reconstructs {arithmetic['independently_reconstructed_applied_factors']:,} applied factors and all {arithmetic['rows']:,} candidate rows; maximum absolute price difference is {arithmetic['maximum_price_absolute_error']:.3g}. Nominal volume is unchanged. Independent lineage, calendar, endpoint/monthly membership and consumption checks pass. Construction arithmetic does not certify missing entitlement evidence.

The full regression suite passes **485 tests**, including 22 additional cases over Round 3. There is one pandas forward-compatibility warning about default concat sorting; the factor audit explicitly sorts its output calendar and its missing-date behavior is tested. Lint, import order and formatting checks pass for changed production/test files.

The retained **2024-07-01 through 2026-10-27** accounting exports were independently reconciled against their original accepted quotes: 4,511 fills, 3,670 closed lots, and 576 daily rows per variant. All three variants pass cash, quantity, FIFO, NAV and stated-tax reconstruction, with maximum relative reconciliation differences below 3e-14. These are rechecks of historical exports, not a new 2017 strategy comparison and not a nominal statutory-tax certification.

`frozen_comparison_plan_v4.json` fixes the two frequencies, complete Nifty 500 scope, dates, 252 history requirement, M15/N30 classic band, all three rebalance variants, volume threshold 100,000, fees 0.1%, initial ₹1m and stated tax rates 20%/12.5%. It correctly distinguishes disabled original batch quality defaults from the separate 7/25/18/45 diagnostic sensitivity. Earlier imprecise draft manifests are retained as superseded. No new performance was inspected before this freeze. MoMo's standing missing-data policy assumes disclosed last-price exits; the repaired app retains units without an executable quote. A common explicit adapter/convention is required before comparison. No unsupported matrix was run.

## What remains to close the original objective

**No accepted price revision was published, and the app does not consume this staged candidate.** The app does consume the scoring/cache repair on a fresh local process. The existing locked, revision-checked publication mechanism is preserved; an unaccepted adjustment model is not a publishable transaction. No new publication approval is being requested for data that fails acceptance.

The concrete worklist is `publication_readiness.json` plus `scoped_unresolved_actions.csv`, `all_246_ohlc_flag_decisions.csv`, `opposing_vendor_close_factor_seams.csv`, and the post-exit consumption files. Closing it requires security-specific primary entitlement terms or a verifiable adjusted-history source, adjudication of opposing intraday/factor observations, explicit TECHNO/CAPF ownership continuation, and a common MoMo/app execution convention. Evidence was acquired but these issues were not fully resolved in this round; this delivery does not claim the original performance question is answered.

Currently supported outputs are the complete membership evidence, dated primary observation layer, independently checked construction/consumption diagnostics, and the retained accounting replay. None supports an unbiased full-scope new 2017 portfolio comparison yet. A price-available survivor subset, a later start date, synthetic Microcap history, or a combined-parent certificate presented as independent component history is not offered as a substitute. A bounded Nifty 500 result would still not repair the original five-component batch.

All 38 protected inputs and all 1,103 sealed Round 3 files are byte-unchanged. Unrelated work and superseded attempts remain on disk. The local commit includes only the accuracy/cache repairs, tests and this curated evidence bundle; no push, deployment or live-checkpoint advance is performed.

## Replay

Use the retained Python environment recorded in `environment.json`, with `.codex-runtime` and `outputs/momo_review_20261102/reader_dependencies` on `PYTHONPATH`. From the repository root, use fresh output names:

```powershell
python outputs/backtest_repair_v4_20261103/build_primary_model.py replay_primary
python outputs/backtest_repair_v4_20261103/audit_completion.py replay_primary replay_consumption
python outputs/backtest_repair_v4_20261103/independent_primary_oracle.py replay_primary
python outputs/backtest_repair_v4_20261103/independent_lineage_oracle.py replay_primary
python outputs/backtest_repair_v4_20261103/independent_consumption_oracle.py replay_primary replay_consumption
python outputs/backtest_repair_v4_20261103/independent_membership_recheck.py outputs/backtest_repair_v4_20261103/replay_membership.json
python outputs/backtest_repair_v4_20261103/independent_export_recheck.py outputs/backtest_repair_v4_20261103/replay_exports.json
python -m pytest -q -p no:cacheprovider
```

Canonical directories refuse reuse. Do not rerun `start_round4.py`, regenerate the baseline, or overwrite sealed targets. `primary_model_v8/` contains its actual build-used source snapshots. `source_used/` records the final app/calculation sources; the review bundle manifest binds all delivered files and primary/provider bytes. Acquisition receipts retain exact URLs, retrieval dates, status, hashes and failed response bytes. Replays use retained bytes; current providers may restate data.
"""
    (OUT / "implementation_report.md").write_text(report, encoding="utf-8")
    names = [
        ".gitattributes",
        "implementation_report.md",
        "baseline_counts.json",
        "protected_baseline.json",
        "protected_inputs_check.json",
        "prior_delivery_hashes_check.json",
        "price_completion_ledger_v1.csv",
        "vendor_query_identity_proposals.csv",
        "frozen_comparison_plan.json",
        "frozen_comparison_plan_v2.json",
        "frozen_comparison_plan_v3.json",
        "frozen_comparison_plan_v4.json",
        "publication_readiness.json",
        "tradability_restrictions.json",
        "scoring_defect_evidence.json",
        "circuit_count_repair_effect.csv",
        "environment.json",
        "full_tests_acceptance.log",
        "app_scoring_tests.log",
        "lint_acceptance.log",
        "import_order_acceptance.log",
        "format_acceptance.log",
        "independent_membership_reconciliation.json",
        "independent_membership_recheck.log",
        "export_recheck.json",
        "independent_export_recheck.log",
        "independent_primary_oracle_final.log",
        "independent_lineage_oracle.log",
        "independent_consumption_final.log",
        "consumption_audit_v7.log",
        "primary_model_v8.log",
        "IDEA_annex_visual.png",
        "acquire.py",
        "start_round4.py",
        "build_primary_model.py",
        "audit_completion.py",
        "freeze_comparison.py",
        "independent_primary_oracle.py",
        "independent_lineage_oracle.py",
        "independent_consumption_oracle.py",
        "independent_membership_recheck.py",
        "independent_export_recheck.py",
        "seal_delivery.py",
    ]
    names += [p.name for pattern in ["retrieval_*.json", "*_urls.json"] for p in OUT.glob(pattern)]
    delivered = [OUT / name for name in names]
    for folder in [
        "evidence",
        "primary_model_v8",
        "consumption_audit_v7",
        "independent_primary_model_v8",
        "source_before",
        "source_used",
    ]:
        delivered.extend(p for p in (OUT / folder).rglob("*") if p.is_file())
    hashes = {str(p.relative_to(OUT)).replace("\\", "/"): sha(p) for p in sorted(set(delivered))}
    manifest = dict(
        baseline_commit="bcdb6fb",
        canonical_primary_model="primary_model_v8",
        canonical_consumption_audit="consumption_audit_v7",
        comparison_plan="frozen_comparison_plan_v4.json",
        status="Scoring/cache accuracy repaired; historical adjusted-price acceptance and new matched comparison incomplete",
        tests="485 passed",
        publication_performed=False,
        strategy_comparison_run=False,
        protected_inputs_unchanged=38,
        prior_sealed_files_unchanged=1103,
        sha256=hashes,
    )
    with (OUT / "delivery_manifest.json").open("x") as f:
        json.dump(manifest, f, indent=2)
    selected = sorted(
        [
            str((OUT / "delivery_manifest.json").relative_to(ROOT)).replace("\\", "/"),
            *[str(p.relative_to(ROOT)).replace("\\", "/") for p in set(delivered)],
        ]
    )
    (OUT / "commit_paths.nul").write_bytes(("\0".join(selected) + "\0").encode())
    print(
        json.dumps(
            dict(files=len(hashes), bytes=sum(p.stat().st_size for p in set(delivered)), status=manifest["status"]),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
