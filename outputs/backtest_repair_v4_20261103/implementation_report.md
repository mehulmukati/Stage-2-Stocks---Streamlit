# Round 4: price completion and independent consumption validation

Continued from `bcdb6fb`. **The scoring defect is repaired and integrated into app calculation/cache paths. The historical price candidate is still unaccepted; publication and a fair new strategy comparison remain incomplete.** No accepted market data, membership source, workbook, accounting engine, Live Signal checkpoint or earlier sealed result was changed. No strategy performance matrix was run.

## Implemented accuracy repairs

`momentum_engine.py` had mutually exclusive lower-circuit inequalities in both scalar and vector scoring. A source-backed SJVN observation returning approximately −4.915515% on 2017-05-23 produced zero lower-circuit hits for the first weekly rank window. The independent hand-window oracle expected one. Both paths now count negative bands symmetrically with positive bands. Direct tests use known inside/outside changes around 5%, 10% and 20%, independently of either implementation.

The correction changes circuit counts in **340 initial security/frequency cases**; the other recorded initial signal metrics are unchanged. `circuit_count_repair_effect.csv` and `source_before/` retain the before/after evidence. This is a signal correction, not a measured CAGR effect.

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
| Security-specific research candidate | Unvalidated gaps | 939,941 observations; 640 lineages; every row unaccepted |

The original NSE action API retains 13,130 records. Its ISIN can be obsolete or restated, and even bond records can carry an equity ISIN. The build excludes 360 non-equity-series records and preserves that exclusion register. Original dated action bulletins and source quote IDs support 546 action-attribution conflict reviews. There are 105 edge records representing 85 unique transitions: 23 retained lifecycle decisions and 62 independently checked additional price-history transitions. Five of the additional transitions require original bulletins because the API ISIN matches neither dated share identifier. No relevant API action record is left unattributed. Attribution does **not** imply validated entitlement terms: 126 decisions remain unresolved in the full acquired period, with **67 potentially consumed records across 59 lineages**.

Both ordinary and DVR classes remain distinct. Economic and raw membership views retain their earlier evidence; all 39 monthly observations and both endpoints independently reconcile again. The warmup calendar includes the 2014 election closures and evidenced Saturday live sessions. The 2015 special session is corroborated by the NSE annual report's cash-underlying figure and primary CM archive; the retained currency circular is explicitly not treated as a cash-market circular.

On 2017-07-10, the daily CM and PR reports agree on Open/Close but differ on some intraday ranges. The SEBI technical-glitch inquiry is retained as common-date context; it does not establish which opposing range is correct. Among 231 affected worklist rows, 134 have agreeing primary OHLC and 97 have opposing primary ranges. All 15 flags on the other six dates have primary-report agreement. Opposing bytes and all row-level decisions remain retained; no accepted OHLC was overwritten.

## Explicit return and entitlement basis

The candidate is `nse-cash-split-research-v1`: pre-event OHLC is backward-normalized by `(P − cash)/(P × share_ratio)`, with independently recorded prior nominal close and terms. Volume stays nominal reported quantity. It is a stated research convention, not executable reinvestment or nominal statutory FIFO/tax basis. Unsupported rights, partly paid instruments, demergers, bonus debentures, ambiguous same-day distributions and missing cash terms remain named blockers; their value is not fitted to a close-price fall.

IDEA's 2019-03-29 fully paid rights are individually reviewed against the issuer release and NSE/FAOP/40530: 87 for 38 at total issue price ₹12.50. The theoretical research factor `(P + (87/38) × 12.5)/(P × (1 + 87/38))` matches the official indicative annex example of 0.591603. The annex was visually checked. This does not model a subscription cash payment or right-sale proceeds, and no such cash is credited again. It does not authorize the same formula for partly paid rights.

Yes Bank's 2020 change remains reconstruction with conditional lock-in, not a split. `tradability_restrictions.json` retains the NSE scheme evidence. The research series does not implement nominal restricted versus free holdings or the shareholder threshold. It cannot claim realistic nominal tradability.

Close-factor changes are checked against primary action factors, independently of OHLC shape. The complete vendor diagnostic covers the extended responses and original local dated joins. It retains **2,927 opposing intervals across all acquired histories**, of which 1,460 are after the conservative first warmup boundary. These are source/convention discrepancies requiring adjudication, not proven portfolio losses or a blanket rejection based on price resemblance. Modern query keys remain proposals; a matching price path never certifies identity.

## Full consumption audit

The original batch uses **252 supported observations**, while the current engine default is **750**. Neither default nor requested start was changed. Weekly first execution is 2017-07-07, ranking on 2017-07-06, with its 252-session metric window starting 2016-06-30; a 750-observation grid starts 2014-06-20. Monthly first execution is 2017-07-31, ranking on 2017-07-28, with the metric window starting 2016-07-25 and the 750 grid starting 2014-07-14. Price history never grants earlier membership eligibility.

Of the 501 initial members, observed-count eligibility is 486/501 weekly and 490/501 monthly under 252; 458/501 and 459/501 under 750. Short or incomplete histories stay in membership and in the exception files. These counts do not establish that every short history is a legitimate IPO truncation, nor that every computable signal has correct entitlement adjustment.

All scheduled signal dates are audited: **102,705 security/frequency/date cases**, not only initial warmup. A separate oracle reconstructs every supported-history count and 9,018 initial scalar metrics without importing engine or metric functions. It discovered the circuit defect and now agrees with the correction. `scheduled_uncomputable_signals.csv` retains missing metric cases.

The potential post-index-exit audit covers 374 security/frequency exit cases. TECHNO and CAPF have no executable continuing old-security quote before scope end, creating four blocked scenarios over the two frequencies. Their 1,842 scenario mark rows correspond to 921 unique security/session days. Quantities cannot be assumed sold; merger/conversion ownership and marks need explicit treatment. These are **potential holding envelopes**, not invented actual portfolio holdings. Actual strategy-specific ownership and blocked-sale replay remain pending supported input acceptance.

## Independent checks and comparison status

The independent price arithmetic oracle reconstructs 4,288 applied factors and all 939,941 candidate rows; maximum absolute price difference is 1.36e-12. Nominal volume is unchanged. Independent lineage, calendar, endpoint/monthly membership and consumption checks pass. Construction arithmetic does not certify missing entitlement evidence.

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
