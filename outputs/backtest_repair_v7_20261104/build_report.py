"""Create the report from reconciled provisional artifacts."""
import json
from pathlib import Path

import pandas as pd

OUT = Path(__file__).resolve().parent


def main():
    results = pd.read_csv(OUT / "results.csv")
    checks = json.loads((OUT / "reconciliation.json").read_text())
    independent = json.loads((OUT / "independent_reconciliation.json").read_text())
    ent = pd.read_csv(OUT / "entitlement_exposure.csv")
    local = pd.read_csv(OUT / "local_close_exposure.csv")
    short = pd.read_csv(OUT / "short_history_exposure.csv")
    eligible = pd.read_csv(OUT / "eligibility.csv")
    manifest = json.loads((OUT / "run_manifest.json").read_text())

    def get(freq, scoring, taxed, variant="Full Rebalance", engine=None):
        engine = engine or ("App ledger" if scoring == "App" else "MoMo FIFO adapter")
        rows = results.loc[
            results.frequency.eq(freq) & results.scoring.eq(scoring) & results.taxed.eq(taxed)
            & results.variant.eq(variant) & results.engine.eq(engine)
        ]
        assert len(rows) == 1
        return rows.iloc[0]

    def metric_row(freq, scoring, variant, label):
        pre, post = (get(freq, scoring, tax, variant) for tax in (False, True))
        return (f"| {freq.title()} | {label} | {pre.cagr_pct:.2f}% | {pre.max_drawdown_pct:.2f}% | "
                f"{post.cagr_pct:.2f}% | {post.max_drawdown_pct:.2f}% |")

    report = [
        "# Provisional matched Nifty 500 comparison",
        "",
        "| Frequency | Classic Full scoring | Pretax CAGR | Pretax max drawdown | Post-tax CAGR | Post-tax max drawdown |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for freq in ("weekly", "monthly"):
        for scoring in ("App", "MoMo"):
            report.append(metric_row(freq, scoring, "Full Rebalance", scoring))
    report += [
        "",
        "Conditional estimates for the full dated Nifty 500 candidate, 1 July 2017 through 1 September 2020. Both paths use the frozen 252-observation requirement and M15/N30 Classic bands, start with INR 1,000,000 and pay 0.1% per side. Both use the app's fiscal-year FIFO research tax rates and offsets. Drawdown uses daily net NAV. CAGR uses elapsed calendar days from the requested start. Tax reserves can change subsequent position sizes.",
        "",
        "## Reconciliation",
        "",
    ]
    max_gap = max(v["maxima"].get("daily", 0) for v in checks)
    engine_gap = max(
        abs(get(f, "App", t, v, "App ledger").cagr_pct - get(f, "App", t, v, "MoMo FIFO adapter").cagr_pct)
        for f in ("weekly", "monthly") for t in (False, True) for v in results.variant.unique()
    )
    report += [
        f"The two adapted paths reconciled in all {len(checks)} frequency, tax and allocation combinations. "
        f"The largest numeric gap across the daily cash, quantities, marks, tax and NAV fields was {max_gap:.3g} in normalized units. "
        f"The largest engine CAGR difference was {engine_gap:.3g} percentage points. "
        f"An independent reader replayed {len(independent)} exported paths and checked source fills, cash, holdings, FIFO, fiscal tax and NAV.",
        "",
        "The driver supplies the same selection, target solver, ownership, quote availability and fiscal-tax layer to both paths. The app uses its quantity ledger. The MoMo adapter runs the native FIFO sale routine with the common fees and gain classification. The scoring comparison uses MoMo's native 63/126-session scores. MoMo's native top-20 strategy, stops, intraday execution and calendar-year taxes remain separate.",
        "",
        f"The calendar has {manifest['observed_sessions']} sessions, {manifest['schedules']['weekly']} weekly and "
        f"{manifest['schedules']['monthly']} monthly rebalances. Signals use the prior observed session. "
        "The 27 October 2019 Muhurat decision uses a 19:15 IST research cutoff. Both paths retain TECHNO and CAPF as locked claims until listing, carrying original lot dates and basis. An ordinary missing sell uses the most recent eligible completed close and records its timestamp and age. Buys require a current executable close. Separate fixtures exercised both claims, an existing CAPF successor position and an assumed exit at the Muhurat cutoff.",
        "",
        "## Controlled attribution",
        "",
        "| Change from stated baseline | Pretax baseline CAGR | Pretax change | Post-tax baseline CAGR | Post-tax change | Limitation |",
        "|---|---:|---:|---:|---:|---|",
    ]
    comparisons = [
        ("Weekly App to weekly MoMo scoring", "weekly", "App", "weekly", "MoMo", "Common eligibility and allocation"),
        ("Monthly App to monthly MoMo scoring", "monthly", "App", "monthly", "MoMo", "Common eligibility and allocation"),
        ("Weekly to monthly, App scoring", "weekly", "App", "monthly", "App", "Changes signal and execution dates"),
        ("Weekly to monthly, MoMo scoring", "weekly", "MoMo", "monthly", "MoMo", "Changes signal and execution dates"),
    ]
    for label, f, s, f2, s2, limit in comparisons:
        p, t = (get(f, s, tax).cagr_pct for tax in (False, True))
        dp = get(f2, s2, False).cagr_pct - p
        dt = get(f2, s2, True).cagr_pct - t
        report.append(f"| {label} | {p:.2f}% | {dp:+.2f} pp | {t:.2f}% | {dt:+.2f} pp | {limit} |")
    report += [
        "",
        "Ordering and frequency can be measured within this shared Nifty 500 scope. Attribution to the original headline runs would require recreating their dated universes, strategy filters, execution sources and tax models. The 2017-2026 five-component batch requires separate coverage.",
        "",
        "| Frequency | App scoring allocation | Pretax CAGR | Pretax max drawdown | Post-tax CAGR | Post-tax max drawdown |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for freq in ("weekly", "monthly"):
        for variant in ("Marginal Rebalance", "Prop Rebalance"):
            report.append(metric_row(freq, "App", variant, variant))

    held = ent.loc[ent.actual_mark_or_entitlement_books.notna()]
    absent = ent.loc[ent.absent_from_all_exported_books & ent.potentially_score_sensitive.gt(0)]
    core = local.loc[local.decision.str.startswith("opposing local close return")]
    exposed = core.loc[core.actual_mark_or_entitlement_books.notna()].copy()
    exposed["magnitude"] = exposed.round6_residual_preview.abs()
    report += [
        "",
        "## Economic limits and certification",
        "",
        "The staged revision applies the reviewed Canara Bank, Karur Vysya Bank and PVR rights factors before the dated ex-sessions. It preserves the primary Close series for unresolved actions and opposing local seams. No unresolved claim, subscription cash or proceeds are imputed. Missing prices stay missing, and nominal Volume is unchanged. The 739 unverified vendor comparisons remain diagnostic. Disputed High/Low values are outside both tested scoring paths.",
        "",
        f"{len(held)} of the 33 unresolved entitlements touch an actual holding window in the exported Full books. "
        f"{len(absent)} concern families absent from all exported books but with potentially affected ranking windows. "
        f"{len(exposed)} of the 236 dated local Close discrepancies overlap a holding or its preceding inventory. "
        f"The replay traces {len(short)} unresolved short-history cases to the actual eligibility reason and history deficit. "
        f"The scheduled snapshots include {eligible.family.nunique()} dated families with complete candidate membership retained.",
        "",
        "| Unresolved event held at the seam | Evidence needed |",
        "|---|---|",
    ]
    for row in held.itertuples():
        report.append(f"| {row.symbol}, {row.date} | {row.needed.replace('|', '/')} |")
    if held.empty:
        report.append("| None in the exported Full books | Ranking-window effects still require evidence |")
    report += ["", "| Dated Close seam in a held security | Relative return residual | Evidence needed |",
               "|---|---:|---|"]
    for row in exposed.nlargest(5, "magnitude").itertuples():
        report.append(f"| {row.family}, {row.previous_date} to {row.date} | "
                      f"{100*row.round6_residual_preview:+.2f}% | Opposing dated nominal closes and adjustment basis |")
    if exposed.empty:
        report.append("| None in the exported Full books | | Continue ranking-window review |")
    report += ["", "Reliance Home Finance's scheme terms and listing dates are supported by its "
               "[FY2018 annual report, printed page 14](https://www.bseindia.com/bseplus/AnnualReport/540709/5407090318.pdf). "
               "The retained NSE issuer notices bind the record and listing dates. The later listing price cannot be used as a prelisting mark."]
    near = absent.sort_values("closest_app_rank").head(5)
    if not near.empty:
        report += ["", "Unheld candidates with affected ranking windows include " +
                   ", ".join(f"{r.symbol} ({r.date})" for r in near.itertuples()) +
                   ". Their observed ranks do not bound the ranks under an unknown entitlement value."]
    report += [
        "",
        "The residual seams and unsupported rights and demerger marks lack verified alternative series that could bound portfolio returns. The table therefore gives conditional point estimates. The exposure ledgers retain the dated security, observed rank, applicable price or ownership interval, and evidence required for certification, including candidates outside realized holdings.",
        "",
        "The historical acceptance gate remains in force. The runner created no certificate or production data revision. Certified figures and the Classic-only app consumption check remain blocked on the identified evidence and the worker's single-profile path.",
        "",
        "Replay in a copy of this output folder under the same outputs directory, from the repository root with the existing Python runtime and repository on PYTHONPATH. Run run_matched.py, verify_stage.py, verify_policy_fixture.py, verify_exports.py, audit_exposure.py, then build_report.py. The source_used folder preserves the exact MoMo routine and policy, and the manifest records their checkout paths and hashes.",
        "",
        "[Full results](results.csv) · [Ledger parity](reconciliation.json) · [Independent replay](independent_reconciliation.json) · "
        "[Input manifest](run_manifest.json) · [Entitlement exposure](entitlement_exposure.csv) · "
        "[Dated Close exposure](local_close_exposure.csv) · [Short histories](short_history_exposure.csv) · "
        "[Vendor diagnostics](unverified_vendor_diagnostic.csv)",
    ]
    (OUT / "round_7_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
