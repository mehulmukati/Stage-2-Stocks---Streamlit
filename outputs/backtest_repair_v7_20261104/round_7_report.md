# Provisional matched Nifty 500 comparison

| Frequency | Classic Full scoring | Pretax CAGR | Pretax max drawdown | Post-tax CAGR | Post-tax max drawdown |
|---|---|---:|---:|---:|---:|
| Weekly | App | 5.04% | -44.75% | 4.46% | -43.67% |
| Weekly | MoMo | 10.78% | -41.37% | 9.38% | -39.57% |
| Monthly | App | 3.33% | -45.98% | 3.19% | -45.13% |
| Monthly | MoMo | 5.96% | -40.75% | 5.55% | -39.64% |

Conditional estimates for the full dated Nifty 500 candidate, 1 July 2017 through 1 September 2020. Both paths use the frozen 252-observation requirement and M15/N30 Classic bands, start with INR 1,000,000 and pay 0.1% per side. Both use the app's fiscal-year FIFO research tax rates and offsets. Drawdown uses daily net NAV. CAGR uses elapsed calendar days from the requested start. Tax reserves can change subsequent position sizes.

## Reconciliation

The two adapted paths reconciled in all 12 frequency, tax and allocation combinations. The largest numeric gap across the daily cash, quantities, marks, tax and NAV fields was 8.53e-14 in normalized units. The largest engine CAGR difference was 2.22e-14 percentage points. An independent reader replayed 8 exported paths and checked source fills, cash, holdings, FIFO, fiscal tax and NAV.

The driver supplies the same selection, target solver, ownership, quote availability and fiscal-tax layer to both paths. The app uses its quantity ledger. The MoMo adapter runs the native FIFO sale routine with the common fees and gain classification. The scoring comparison uses MoMo's native 63/126-session scores. MoMo's native top-20 strategy, stops, intraday execution and calendar-year taxes remain separate.

The calendar has 783 sessions, 165 weekly and 38 monthly rebalances. Signals use the prior observed session. The 27 October 2019 Muhurat decision uses a 19:15 IST research cutoff. Both paths retain TECHNO and CAPF as locked claims until listing, carrying original lot dates and basis. An ordinary missing sell uses the most recent eligible completed close and records its timestamp and age. Buys require a current executable close. Separate fixtures exercised both claims, an existing CAPF successor position and an assumed exit at the Muhurat cutoff.

## Controlled attribution

| Change from stated baseline | Pretax baseline CAGR | Pretax change | Post-tax baseline CAGR | Post-tax change | Limitation |
|---|---:|---:|---:|---:|---|
| Weekly App to weekly MoMo scoring | 5.04% | +5.74 pp | 4.46% | +4.92 pp | Common eligibility and allocation |
| Monthly App to monthly MoMo scoring | 3.33% | +2.63 pp | 3.19% | +2.36 pp | Common eligibility and allocation |
| Weekly to monthly, App scoring | 5.04% | -1.71 pp | 4.46% | -1.27 pp | Changes signal and execution dates |
| Weekly to monthly, MoMo scoring | 10.78% | -4.82 pp | 9.38% | -3.83 pp | Changes signal and execution dates |

Ordering and frequency can be measured within this shared Nifty 500 scope. Attribution to the original headline runs would require recreating their dated universes, strategy filters, execution sources and tax models. The 2017-2026 five-component batch requires separate coverage.

| Frequency | App scoring allocation | Pretax CAGR | Pretax max drawdown | Post-tax CAGR | Post-tax max drawdown |
|---|---|---:|---:|---:|---:|
| Weekly | Marginal Rebalance | 6.13% | -47.29% | 5.92% | -46.01% |
| Weekly | Prop Rebalance | 6.11% | -45.41% | 5.64% | -44.39% |
| Monthly | Marginal Rebalance | 6.34% | -45.04% | 6.38% | -44.67% |
| Monthly | Prop Rebalance | 5.62% | -45.31% | 5.69% | -44.32% |

## Economic limits and certification

The staged revision applies the reviewed Canara Bank, Karur Vysya Bank and PVR rights factors before the dated ex-sessions. It preserves the primary Close series for unresolved actions and opposing local seams. No unresolved claim, subscription cash or proceeds are imputed. Missing prices stay missing, and nominal Volume is unchanged. The 739 unverified vendor comparisons remain diagnostic. Disputed High/Low values are outside both tested scoring paths.

1 of the 33 unresolved entitlements touch an actual holding window in the exported Full books. 10 concern families absent from all exported books but with potentially affected ranking windows. 20 of the 236 dated local Close discrepancies overlap a holding or its preceding inventory. The replay traces 571 unresolved short-history cases to the actual eligibility reason and history deficit. The scheduled snapshots include 640 dated families with complete candidate membership retained.

| Unresolved event held at the seam | Evidence needed |
|---|---|
| RELCAPITAL, 2017-09-05 | Issuer establishes 1:1 Reliance Home Finance shares, Sep 6 record, Sep 7 allotment, Sep 22 listing. Require contemporaneous prelisting claim value or documented adjusted research series, and a defensible parent/child basis allocation for tax replay. |

| Dated Close seam in a held security | Relative return residual | Evidence needed |
|---|---:|---|
| INE545A01016, 2018-07-12 to 2018-07-13 | +1.30% | Opposing dated nominal closes and adjustment basis |
| INE545A01016, 2018-11-09 to 2018-11-12 | +0.72% | Opposing dated nominal closes and adjustment basis |
| INE686F01025, 2017-12-07 to 2017-12-08 | +0.68% | Opposing dated nominal closes and adjustment basis |
| INE686F01025, 2017-12-06 to 2017-12-07 | -0.68% | Opposing dated nominal closes and adjustment basis |
| INE947J01015, 2019-03-18 to 2019-03-19 | +0.67% | Opposing dated nominal closes and adjustment basis |

Reliance Home Finance's scheme terms and listing dates are supported by its [FY2018 annual report, printed page 14](https://www.bseindia.com/bseplus/AnnualReport/540709/5407090318.pdf). The retained NSE issuer notices bind the record and listing dates. The later listing price cannot be used as a prelisting mark.

Unheld candidates with affected ranking windows include SOUTHBANK (2017-02-16), CESC (2018-10-30), IIFL (2017-10-17), HCL-INSYS (2017-10-31), GAYAPROJ (2018-01-30). Their observed ranks do not bound the ranks under an unknown entitlement value.

The residual seams and unsupported rights and demerger marks lack verified alternative series that could bound portfolio returns. The table therefore gives conditional point estimates. The exposure ledgers retain the dated security, observed rank, applicable price or ownership interval, and evidence required for certification, including candidates outside realized holdings.

The historical acceptance gate remains in force. The runner created no certificate or production data revision. Certified figures and the Classic-only app consumption check remain blocked on the identified evidence and the worker's single-profile path.

Replay in a copy of this output folder under the same outputs directory, from the repository root with the existing Python runtime and repository on PYTHONPATH. Run run_matched.py, verify_stage.py, verify_policy_fixture.py, verify_exports.py, audit_exposure.py, then build_report.py. The source_used folder preserves the exact MoMo routine and policy, and the manifest records their checkout paths and hashes.

[Full results](results.csv) · [Ledger parity](reconciliation.json) · [Independent replay](independent_reconciliation.json) · [Input manifest](run_manifest.json) · [Entitlement exposure](entitlement_exposure.csv) · [Dated Close exposure](local_close_exposure.csv) · [Short histories](short_history_exposure.csv) · [Vendor diagnostics](unverified_vendor_diagnostic.csv)
