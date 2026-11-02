# Accuracy repair implementation report — 2 November 2026

Implemented accounting repairs and conservative reconstruction/publication gates. Historical security identity and complete universe coverage remain unresolved, so this is **not a completed certified-universe repair**. The weekly/monthly/filter comparison matrix was not run. No strategy defaults were changed.

## Validation evidence

- Frozen baseline: commit `32c1d08`, complete hashes in `baseline_manifest.json`, original engine copy in `backtest_engine_baseline.py`.
- Reproduced workbook 15/30 classic weekly 3/6/9/12, no Stage 2 drop/cap: full/marginal/prop NAV 887.36 / 902.83 / 908.47 and original 252-session CAGR 27.01 / 27.25 / 27.34%. See `baseline_replay.log`.
- Full suite: **425 passed** (`full_tests.log`). Recovery restoration additionally tested (`recovery_tests.log`). New fixtures plus affected suites passed; final static checks in `lint_verified.log` and `git diff --check`.
- Independently reconstructed all three final diagnostic portfolios from original accepted quotes and exported orders. Cash, quantity, FIFO gains/basis and daily simplified FY tax liabilities reconcile. Detailed errors and counts: `accounting_diagnostic_v2/independent_reconciliation.json`. Maximum NAV error is below 5e-13 across 2,302 sessions per variant.
- `protected_inputs_check_final.json` verifies unchanged accepted market sources, original constituent/mapping inputs and historical workbooks. Source snapshots in `source_snapshot` preserve the implementation used. No shared publication, deployment, push, checkpoint advancement or live instruction occurred.

## Deterministic defect repairs

| Oracle | Reviewed behavior | Repaired expected/observed behavior |
|---|---|---|
| Missing buy quote before resumption | Buy recorded, NAV 200 | No fill, NAV 100 cash |
| Missing sell quote | Holding removed, NAV 100 | Units retained; uncertain carried mark; NAV 200 after genuine resumption |
| 50/50 gap/resumption, no trades | NAV 266.6667 | NAV 300; longer gaps also tested |
| Full FIFO exit at 200, post-fee NAV 199.8 | 0.001 orphan shares | Sell full actual lot; zero shares |
| All-cash exit, 0.1% per-side convention | NAV 199.8 | NAV 199.6002 |
| Two holdings, 20% cap | 50% / 50% | 20% / 20%, 60% cash |
| Partial-period schedule | Terminal date can move prior events | Completed exchange periods; prefix-invariant NAV/events |
| First available exclusion | Silently discarded as inception | Retained by default; optional explicit legacy rule and review register |
| Fuzzy or stripped name | Becomes historical ticker identity | Proposal only; dated primary evidence required |

The ledger also implements fee-inclusive FIFO basis, daily simplified tax accrual, cash reservation and actual tax-funding sales, with payable liabilities retained when no sale can fill. Corporate-action transfer preserves units, basis and acquisition dates under the reviewed registry. Cash earns zero. Missing fills are retried only at a new scheduled decision. See `doc/backtest_accuracy_repair.md` for the explicit price/volume/tax/cap limits and compatibility adapters.

## Single legacy-universe accounting diagnostic

These are the same frozen legacy inputs/settings, **not certified repaired-universe strategy comparisons**. Both CAGR columns below use elapsed calendar days / 365.25; the workbook's original session-based CAGR is stated separately above. Ledger, fill, tax, cost, cash, concentration, manifest and coverage exports are in `accounting_diagnostic_v2`.

| Variant | Before NAV | After NAV | Before calendar CAGR | After calendar CAGR |
|---|---:|---:|---:|---:|
| Full Rebalance | 887.3619 | 876.1478 | 26.4041% | 26.2317% |
| Marginal Rebalance | 902.8315 | 894.4541 | 26.6388% | 26.5122% |
| Prop Rebalance | 908.4689 | 897.1658 | 26.7235% | 26.5533% |

These aggregate changes combine interacting repairs: permitted fills, quantity/mark consistency, actual-order costs, fee-inclusive basis, tax reservation/accrual/funding, and the removal of implicit terminal rebalancing. Identity/coverage corrections have not been published and contribute **no accepted universe change** to this diagnostic. The independent oracles isolate each defect; no additive full-period CAGR attribution or improvement claim is made. The legacy Tax Drag column now explicitly reports total paid/accrued tax relative to initial NAV and is not comparable to the old sum of deduction ratios; actual units are in `accounting_totals.json`.

## Identity, coverage and remaining blockers

`candidate_universe_v5` retains all **9,179** nonblank source events with original row IDs, raw/effective dates, mapping decisions and inception-assumption reviews. Nine empty formatting rows are recorded in parser rejections. All **1,449** name heuristics (132 stripped-exact, 489 fuzzy-high, 723 fuzzy-medium, 105 unresolved) are quarantined, including every high-tier suggestion. The evidence-only master demonstrates a reviewed point identity without projecting its ticker back to old events. All 9,179 old events still lack accepted date-scoped name/security evidence. They remain inspectable as synthetic unresolved identities in rejected partial candidates, not silently omitted or remapped to survivors.

The official Free Float 100 renames now join their corresponding 100-index lineages, with source labels retained. They do not become Midcap 150 or Smallcap 250. The first logged date is not verified inception; the legacy exclusion policy is explicit and separately exported. LargeMidcap 250 launched 30 November 2017 despite a 2005 base date; pre-launch relationship applicability requires evidence. Official Microcap launch/factsheet searches supplied no pre-May-2021 historical membership lists. Later components are never copied backward.

`universe_audit_v2` contains an unresolved-impact register, all-historical price coverage, per-index/date coverage and explicit parent/component member discrepancies. It audits **1,184** known historical symbols plus **1,449** unresolved source names independently of current survivors. Fifty-nine known historical symbols have no local price series, including old symbols and merged/delisted issuers; these require reviewed aliases and/or actual historical prices, not deletion. The current accepted selected-index/parent change-date grid has **17 disagreements on 90 dates** after September 2020. This grid is deliberately pinned to current accepted sources and relevant index changes, not the earlier assessment's differently sampled 134-date grid; the sets/dates are exported for inspection. Observed component snapshots take precedence, and stale parent histories cannot automatically replace them.

No complete historical-universe interval is certified. Quantitative return impact of unresolved identities/missing delisted prices remains unknown. Candidate publication is rejected; accepted legacy history remains labelled partial/uncertified. A reviewed certificate must bind stable identities, observed states, source hashes/revisions and explicit coverage scopes before an interval repair can publish. Backups, atomic/conflict checks, cache invalidation and checked restoration are implemented/tested. Startup and readers make no upstream requests or reconstruction writes.

## Next gate

Complete the dated identity review, obtain missing historical price/member evidence, and validate the parent/component intervals and discrepancies. Then stage and review an accepted repair, re-run independent source ledgers, and only then run the requested small frozen weekly/monthly/portfolio/filter matrix. No performance-tuned defaults or optional MoMo variant were introduced. Inspected history remains retrospective; use future paper trading for genuinely unseen validation.

The initial `accounting_diagnostic` directory/log is a superseded exploratory attempt begun before the final input-manifest capture/performance cleanup. Use **only `accounting_diagnostic_v2`** and its matching source snapshots as the final reconciled evidence.
