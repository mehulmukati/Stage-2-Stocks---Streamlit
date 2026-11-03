# Round 3: bounded historical data repair

Continued from `6981092`, for Nifty 500, 2017-07-01 through 2020-09-01. **Membership now reconciles; the complete backtest data candidate remains unaccepted because price and entitlement validation is unfinished.** No accepted market data, strategy defaults, historical workbook, or Live Signal checkpoint was changed. No strategy matrix was run.

## Completed and independently checked

| Check | Round 2 | Round 3 |
|---|---:|---:|
| Verified event identities | 376 records / 291 names | Preserved |
| Independently corroborated event transitions | 54 unique / 56 records | **374 unique / all 376 records, on all 24 dates** |
| Dated lifecycle decisions | 24 transitions pending | **24 evidenced; 23 applicable, 1 outside scope** |
| Reverse-walk conflict dates | 3 | **0** |
| Forward/backward observed endpoints | Failed | **Both pass** |
| Monthly observation discrepancies | 38 | **0 across 39 comparisons** |
| Missing-label vendor histories acquired and identified | 0 | **72 of 144 staged** |
| Primary daily trading sessions retained | Sparse observations | **783 / 783 independently checked calendar sessions** |
| Full regression suite | 451 passed | **463 passed** |

The canonical candidate is **`candidate_v3`**. Its two membership views explicitly distinguish economic ex-date security lineage from identifiers reported in raw exchange files. Earlier candidate and price-audit attempts are retained locally as superseded diagnostics.

## Lifecycle and timing decisions

Primary MSEI corporate-action tables, NSE listing/clearing circulars, and adjacent daily NSE identifier rows establish all 24 worklist decisions. The four original conflict families are resolved: HERITGFOOD, IEX, SIS and IRCON. Each decision preserves old/new ISINs, economic ex-date, last daily old identifier, first daily new identifier, ratio, source-row references, source hash, retrieval/verification date, and applicability. No issuer-prefix merging or fuzzy alias promotion is used.

The distinction between dates matters. IEX trades ex-split on 2018-10-19 while the new identifier first appears in the retained daily exchange file on 2018-10-22. IRCON's corresponding dates are 2020-04-03 and 2020-04-08. The raw ISIN observation date is not asserted to be a depository settlement or legal conversion date. `reviewed_lifecycle_decisions.json` states this explicitly. The official NSE IEX and SIS circular pages were also visually inspected.

`provisional_membership.parquet` reconciles unchanged raw identifier observations. `economic_provisional_membership.parquet` applies supported economic ex-dates. Its comparison keeps raw observations intact and explicitly projects the known identifier lag: the 2017-08-31 INFIBEAM observation is the sole monthly projection. Both views pass forward/backward endpoint checks and all 39 relevant monthly comparisons. No nominal 500-security size is imposed; the official 501-security anchor and ordinary/DVR distinction remain intact.

Laurus Labs' split is effective 2020-09-29 and is excluded from this bounded repair. Yes Bank's second identifier change is **inside** scope, effective for trading from 2020-03-16, despite its original monthly bounds extending into September. The NSE reconstruction circular establishes conditional holding restrictions: this is not classified as a routine split. Avanti Feeds' combined split/bonus is distinguished from a simple split. Membership conversion is supported; these decisions do not authorize nominal portfolio transformations. Existing adjusted research quantities, FIFO lots and cash are unchanged; splits/bonuses/distributions are not credited a second time. Nominal tax-basis and entitlement treatment remains outside the accepted adjusted-unit model.

Twenty-eight full review/amendment notices corroborate all inherited event sets. Announcement date, effective session, preceding close, source rows and verification date are separate fields in `reviewed_notice_date_contexts.json`. Duplicate source records remain provenance; the walk applies unique transitions. `primary_review_transitions.csv` explicitly retains superseded rows. `review_supersession_decisions.json` records the September 2017 and March 2018 accelerations and the 2020 deferrals/cancellations. The May 13, 2020 notice declares the unexecuted March 27 proposals null and void; the June review is treated as a fresh review, not the old list shifted forward. The already executed March 19 Yes Bank replacement is preserved.

The independent audit extracts the 28 notice tables with **pdfplumber**, independently re-reads monthly exchange ISINs and daily lifecycle references, and reconstructs both membership views without importing the production walker. See `independent_membership_reconciliation_v2.json`.

## Price coverage and precise rejection

The evidence archive now includes daily primary files for **783 sessions**, reconciled against the four annual NSE calendars, election-holiday amendments, and Muhurat sessions. There are no unexplained missing or extra primary sessions. The protected local price history has **no rows on 2019-02-13 or 2019-03-29**, both actual trading sessions. Their original NSE quotes are retained separately; no adjustment factor is invented to insert them into accepted adjusted prices.

The daily audit covers **652 historical exchange labels / 663 security IDs** and retains **477,660 relevant raw security quotes**, including periods outside each security's index eligibility. It evaluates **392,280 actual eligible quote rows**. Three additional eligible security/session pairs have no positive allowed-series quote: JKIL on 2017-08-08, 09 and 10. Membership is retained; no quote or fill is invented.

All 144 missing-label vendor queries were attempted and recorded. **72 histories, containing 55,756 staged rows, were acquired and linked to dated security evidence.** Exact ISIN bridges identify most; Eros Media and Jet Airways additionally match exact historical legal names/symbols and their own dated primary price rows. These are retrospective vendor-series links, not a claim that modern exchange names were valid in the past. Per-observation links are preserved in `dated_vendor_security_links.parquet`; no continuous unobserved exchange-alias interval is inferred.

The remaining **72 labels** returned unavailable histories under the reviewed query keys (70 HTTP 404 responses and two HTTP 400 responses). This does not prove that their historical prices cannot be obtained elsewhere. `remaining_adjusted_history_acquisition.csv` lists each historical identity, attempted vendor key/URL/status, primary raw coverage and dates. A successor's earlier history is not substituted. Modern securities with changed ISINs are not joined by an issuer prefix.

| Eligible quote-row availability | Rows |
|---|---:|
| Protected local series under the dated exchange label | 320,918 |
| Newly acquired, identity-supported staged vendor series | 37,608 |
| No available adjusted observation under the reviewed mapping | **33,754** |

Availability does not establish a correct adjustment model. **246 rows** show an open/high/low shape disagreement exceeding 1% after removing the close-price normalization scale; 231 are concentrated on 2017-07-10. This diagnostic does not decide which feed is correct. Every flagged row and source reference is retained. Raw exchange prices, vendor split-normalized close, derived adjusted OHLC, reported volume and distributions remain separately inspectable. No raw/adjusted splice is published. Dividend/bonus/demerger entitlements, adjustment seams, volume conventions, and the Yes Bank reconstruction restrictions still require model validation.

`price_audit_v4` contains the final daily coverage, flags, acquisition worklist and source hashes. An independent reader rechecks **all 55,756 vendor rows** directly against retained provider payloads and **300 sampled raw exchange rows**, without the production price parser. Those checks pass; they certify extraction arithmetic and provenance, not investment-model completeness. **Zero newly acquired histories are promoted to accepted backtest inputs.**

## Source hardening and validation

- The builder now checks cached PDF member/digests against raw archives before use. Corrupted/replaced sources fail closed; all 40 original cache bindings pass.
- Legacy `CONFIDENCE`, `MATCH_SCORE` and `RECONSTRUCTION_ALLOWED` fields are namespaced. Verified identity, independently corroborated event timing, chain reconciliation and daily-data acceptance are separate statuses.
- Primary trading timestamps support explicit four- or two-digit years and must match their archive date. Calendar parsing uses an explicit format, including dates with no space after the comma.
- Twelve additional tests cover primary cache binding, lifecycle/date policies, independent endpoint failures, nonmember conversion, distinct share-class collisions, a real complete review/amendment fixture, timestamp/calendar formats, vendor identity/schema rejection, missing bars and no double adjustment. **463 tests pass**, preserving all 451 existing cases. Strict lint/import/format checks pass.
- Repeated independent source-price/cash/quantity/FIFO/tax reconciliation still passes for all three delivered diagnostic books: **576 sessions each / 4,511 fills**, maximum NAV discrepancy under **3e-14**. This rechecks the existing diagnostic; it is not a new matched strategy comparison.

The delivery checks verify protected original inputs and the prior sealed Round 2 artifacts. Exact source snapshots, acquisition receipts, raw evidence, manifests and independent checks are retained. Offline builders do not fetch, publish, or run from app startup/page reads.

## Publication and next supported work

No acceptance certificate or publication was created. App coverage remains honestly legacy/uncertified. Combined Nifty 500 membership does not establish separate Midcap/Smallcap partitions or pre-launch Microcap history. Existing publication locking, backups, revision checks and outside-scope isolation are untouched.

Next work is the specific remaining price acquisition and adjustment/entitlement worklist, including the two absent market sessions and source disagreements. Only a price-supported scope can proceed through the repaired review/publication mechanism and certificate-driven app coverage. The fixed matched matrix remains unrun. Adjusted research-unit tax, nominal execution/capacity, stale-mark and price-only benchmark limitations remain explicit; no alpha or untouched holdout is claimed.

This delivery is committed locally without pushing or deploying. Unrelated files, accepted sources, historical results and live checkpoints are preserved.

See [policy and reproduction notes](../../doc/backtest_accuracy_round3.md), [candidate manifest](candidate_v3/manifest.json), [price manifest](price_audit_v4/manifest.json), [calendar reconciliation](calendar_reconciliation.json), and [delivery manifest](delivery_manifest.json).
