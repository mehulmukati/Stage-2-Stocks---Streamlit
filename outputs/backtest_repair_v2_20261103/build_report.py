from pathlib import Path
import json,hashlib,pandas as pd
root=Path.cwd();out=root/'outputs/backtest_repair_v2_20261103'
before=json.loads((out/'before_review_checks.json').read_text());after=json.loads((out/'after_review_checks.json').read_text())
checks={k:v for k,v in after.items() if isinstance(v,dict) and 'passed' in v}
assert all(v['passed'] for v in checks.values())
protected=json.loads((out/'protected_inputs_check.json').read_text(encoding='utf-8-sig'))
assert all(x['unchanged'] for x in protected)
recon=json.loads((out/'accounting_diagnostic/independent_reconciliation.json').read_text())
assert all(x['passed'] for x in recon.values())
for key,digest in json.loads((out/'accounting_diagnostic/manifest.json').read_text())['code_and_registry_sha256'].items():
 assert hashlib.sha256((out/'diagnostic_source_used'/key).read_bytes()).hexdigest()==digest,key
candidate=json.loads((out/'bounded_candidate_v3/manifest.json').read_text())
for key,digest in candidate['code_hashes'].items():
 relative=Path(key)
 assert hashlib.sha256((out/'bounded_candidate_v3/source_used'/relative).read_bytes()).hexdigest()==digest,key
ident=json.loads((out/'identity_resolution_recheck.json').read_text())
assert ident['events']==376 and ident['names']==291 and ident['passed']
assert '451 passed' in (out/'full_tests_release.log').read_text()
rows=[]
for name in ['average_holdings_after_failed_buy','blocked_exit_position_count','ordinary_taxable_cap','engine_ordinary_taxable_cap','independent_variant_states','publication_interval_isolation']:
 rows.append(dict(check=name,before_passed=before[name]['passed'],after_passed=after[name]['passed'],
                  before=json.dumps(before[name],sort_keys=True),after=json.dumps(after[name],sort_keys=True)))
pd.DataFrame(rows).to_csv(out/'boundary_before_after.csv',index=False)
report='''# Follow-up accuracy implementation — 3 November 2026

Continued from `ef6139f`. The five remaining engine/reporting boundaries and effective-interval publication defect are fixed. The bounded historical work now resolves every relevant event identity and the full official anchor, with primary artifacts and a concrete lifecycle/price worklist. **The daily historical universe is still unaccepted; no candidate has been published and no strategy matrix was run.**

## Fixed and verified

- **451 tests passed** in the final full suite (`full_tests_release.log`); the final affected subset also passed. Formatting/import checks, strict lint and `git diff --check` pass.
- The supplied reassessment probes were reproduced before edits; new acceptance fixtures failed before the fixes (`failing_oracles.log`: 12 failures) and now pass. Every behavioral check in the original reassessment probe passes after the repairs. The unchanged round-one examples continue to pass.
- Independent exported-source reconstruction passes for all three books over **576 sessions and 4,511 fills**, including prices, quantities, cash, FIFO basis/gains and simplified FY taxes. Maximum NAV disagreement is under **3e-14 NAV units** (`accounting_diagnostic/independent_reconciliation.json`).
- Accepted source revisions remain unchanged. `protected_inputs_check.json` verifies all protected original data/mapping/calendar/workbook bytes; prior results and Live Signal checkpoints were not overwritten or advanced. No push/deployment or real trade instruction occurred.

| Boundary | Before | After |
|---|---|---|
| M=2, blocked A exit and executable B exit | A/C/D: three actual stocks | A/C: two actual stocks, in each book |
| Repeated blocked exits / multiple unfilled names | Planned vacancies could admit excess stocks | Locked holdings reserve actual slots; permitted candidates admitted in rank order |
| Public engine, executable taxable 50% cap | 50.8463% / 49.1537% | 50% / 50%, no cap violation |
| Direct ledger, taxable 50% cap | Largest position 52.6288% | Both positions at 50% of final net NAV |
| Divergent full=A, marginal=B, M=1 | Full incorrectly acquired B | Each retains its own qualified incumbent |
| Missing buy, no stock ever held | Avg Holdings 1.0 | Avg Holdings 0.0 |
| Repair approved 5–10 January | 13 January changed OLD to NEW | 13 January remains OLD |

`boundary_before_after.csv` includes machine-readable before/after probe details. Repeated blocked signals, partial cash, skipped candidate fills, reachable book divergence from different sale affordability, all variants, no-gap/no-fee/no-tax cases and independently solved final quantities/cash/NAV are tested. Publication tests use the actual publisher with temporary parquets and compare every outside calendar date, including no subsequent snapshot, adjacent/spaced scopes and collisions with original boundary snapshots. Rejected updates and backup restoration are tested.

## Strategy/accounting semantics

Only immutable ranks and metrics are shared. Each portfolio makes incumbent, slot, fill and target decisions independently. Classic retains its existing buffered mechanics; displacement keeps its M-stock limit. A blocked or uneconomic exit occupies a slot, while an unfilled buy consumes none. Another eligible candidate in the existing pool can fill a real vacancy. No filters, defaults, ranking scores or frequencies were tuned.

Caps use final **cash + marked stocks − payable/accrued tax**, after fees and newly realized FIFO tax. The target solver uses isolated trial accounting state and does not export trial fills. Ordinary taxable overshoots are repaired rather than classified as locked-position exceptions. Genuine blocked positions and subsequent drift remain explicit limitations. Cash/tax funding and original adjusted-unit fee/tax conventions are preserved.

Avg Holdings is sampled from each book's actual end-of-session positions, including all-cash sessions; planned counts remain separate. Common union fields remain compatibility/reporting views. The app labels these union views, exports actual per-book counts, and Live Signal replay projects the selected book before stale-position checks or display. Actual-position checkpoint events keep their existing interface and confirmation flow. The replay result-cache version changes; confirmed checkpoints do not.

Publication scopes are inclusive, timezone-free calendar dates. The original effective state resumes on end+1 calendar day; adjacent approved scopes and existing authoritative boundary snapshots take precedence. Source revision/locking/atomic-write protections remain. Candidate, observed snapshots and identity bytes are rechecked under the lock and preserved in backup evidence.

## Bounded historical evidence delivered

The candidate is **`bounded_candidate_v3`**; earlier attempt directories are retained as diagnostics, not accepted data.

- Worklist rechecked: **376 Nifty 500 source records / 291 names**, 2017-07-01–2020-09-01. Two records duplicate dated identity/direction transitions; all source row IDs are retained, and the walker applies sets of unique transitions.
- **376/376 event identities resolved**, covering **291/291 names**, through exact dated primary company-name/symbol rows plus NSE ISIN observations. No fuzzy or modern survivor assignment is promoted. The 295 reusable alias decisions include explicit verified-date whitelists; interstitial aliases are not inferred merely from their minimum/maximum dates. `identity_resolution_recheck.json` independently loads the staged registry through the production resolver and rechecks every source row.
- **501/501 official anchor securities resolved** at the 31 August 2020 close. The extra ordinary/DVR security rows are retained. The supplied September 1 source has 500 rows and modern ticker names; `anchor_comparison.json` records the exact differences, rather than silently accepting those restated labels as historical exchange symbols.
- Evidence includes **40 official monthly constituent archives**, **58 official dated trading archives**, three official event-change notices and an IRCON issuer filing. URLs, retrieval dates and hashes are preserved in `evidence`. Parser caches, raw-source hashes and the exact parser/resolver source versions are recorded. No upstream fetch occurs in page/startup code or the offline candidate builder.
- Three event notices corroborate date/direction for **56 source records (54 unique transitions)**. The other 320 records still need comparable independent timing/completeness checks. An amendment notice is not treated as the full main review. The effective session is distinguished from the preceding session close.

## Precisely what remains unresolved

Identity resolution alone does not prove a complete daily security universe. The stable-security-ID reverse walk still has **three conflict dates / four missing inclusion IDs**, fails its forward round trip and differs from **38 monthly anchor comparisons**. These discrepancies include unresolved share-security conversions; they are not automatically evidence of incorrect company membership events. Neither a nominal size target nor a latest-date rule is used to delete/insert stocks.

`security_lifecycle_worklist.csv` and `lifecycle_transition_worklist.csv` show **24 observed ID transitions across 23 tickers** over the monthly evidence window. Twenty-two are observed before the August anchor; two observation bounds extend into September and require scope-applicability review. Seven relevant source records are associated with these ticker families. The worklist records last-old/first-new dates and affected row IDs, without guessing exact conversion dates or ratios. The IRCON filing establishes a CIN, new ISIN and 1:5 split ratio; its exact exchange-effective date remains unsupported by that document, so it does not authorize an applied conversion. No unrelated later action is made a prerequisite to a bounded repair.

`all_eligible_price_coverage.csv` audits **652 historical ticker identities**, including merged/delisted/retired names and names outside today's survivors. **144** lack a usable local adjusted series in the scoped dates under their historical ticker. Many have preserved raw NSE observations; missing local adjusted history is not proof that all historical prices are unavailable. Vendor aliases, issuer identifiers not independently established and cross-security lineage are explicitly unresolved; no modern ticker history is substituted. Raw and adjusted series are not mixed.

The staged candidate and monthly observations are inspectable. Its confidence is unverified_chain and its acceptance is false. No certificate has been fabricated and no active membership file changed. The existing app therefore remains honestly in legacy/uncertified research mode; scope support is not claimed by changing a global string. A combined Nifty 500 scope does not establish separate Midcap 150/Smallcap 250 histories or early Microcap membership. No pre-launch LargeMidcap relationship is used.

## Result interpretation and next gate

The source-ledger diagnostic is a fresh **2024-07-01–2026-10-27** run with fixed existing 15/30 classic weekly settings, not a paired continuation of the previous 2017-start workbook or a certified strategy comparison. It is used to check arithmetic and source consistency only. Input manifests and complete ledgers/stats are preserved; no return improvement or alpha is attributed to these repairs. The original adjusted research-unit, simplified tax, volume, stale-mark and price-only benchmark limits remain explicit.

Next work is bounded and reviewable: establish applicable effective-date security lineage, complete event timing/completeness checks, reconcile the stable-ID round trip to the authoritative monthly observations, and resolve the historical adjusted/vendor price worklist. Only a supported scope can be published through the repaired publisher and connected to certificate-driven app coverage. The matched strategy matrix stays gated until then.

See `doc/backtest_accuracy_followup.md` for policy/publication details. `diagnostic_source_used` binds the exact executed accounting source bytes; `bounded_candidate_v3/source_used` binds the exact evidence-builder/parser bytes. Final source differs only in subsequent unused-variable/formatting cleanup and UI/selected-book presentation compatibility. The accepted-source and archive hashes are independently rechecked before delivery. This follow-up is delivered in a local commit; no remote action was taken.
'''
(out/'implementation_report.md').write_text(report,encoding='utf-8')
print('Report written; protected inputs:',len(protected),'all unchanged; independent checks verified.')
