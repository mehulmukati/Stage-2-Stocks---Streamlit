# Backtest accuracy follow-up — 3 November 2026

See [Round 3 historical data policy](backtest_accuracy_round3.md) and the versioned `outputs/backtest_repair_v3_20261103/implementation_report.md`. Membership now reconciles; price/model validation remains unaccepted. The Round 2 accounting and publication repairs below remain in force.

The second repair retains the quantity/cash/FIFO foundation and fixes the remaining execution boundaries. These changes do not tune ranking, quality filters, rebalance frequencies or defaults. The adjusted-unit, simplified-tax, volume, stale-mark and benchmark limitations in [the original policy](backtest_accuracy_repair.md) still apply.

## Each portfolio makes its own decision

Full, marginal and proportional books share rankings and indicator history. Each selects from its own actual incumbents, available funds and occupied slots. A stock in another book cannot become an incumbent or admission candidate merely through the compatibility union. The common `holdings`, `entries` and `exits` fields remain reporting views; `selection_by_variant`, `holdings_by_variant` and each execution ledger describe the individual books.

Displacement retains a slot for an unfilled exit, including a position whose proceeds cannot cover the sale fee. Replacement candidates are considered in valid rank order from the existing candidate pool. Missing quotes do not consume slots; the next permitted candidate can fill an actual vacancy. No candidate outside the declared ranking/Stage 2 pool is introduced. A rejected order itself is retried only on a later scheduled signal. Classic retains its intended ability to hold more than M; displacement retains its M-stock maximum. No holdings are deleted to satisfy that count.

## Caps use attainable final net NAV

At a rebalance, the cap denominator is **cash plus marked stock value less payable and accrued tax, after all executed fees and newly realized tax**. The target solver includes actual FIFO sales, fee allocation, current FY offsets and the tax reserve before committing trades. It solves final weights rather than targeting a pre-tax balance and labeling the resulting overshoot an exception.

A normal executable taxable rebalance must meet a feasible cap. Insufficient names retain cash. A genuinely locked position can prevent attainment, and later market drift can exceed a rebalance-only cap; those remain explicit exceptions. There are no hypothetical fills, external funds or trades hidden outside the ledger. Trials used to solve final NAV never mutate actual lots, cash or exported fills.

For one share at cost 100, mark 200, no fees, 20% ST rate and two 50% targets, the independent solution sells 10/19 share, retains 9/19 share of A, buys 18/19 share of B at 100, reserves tax/cash 200/19 and ends at net NAV 3600/19. Both stocks are exactly 50% of final net NAV.

`Avg Holdings` keeps its export column name. It now averages each book's actual number of holdings at every session close, including initial/all-cash sessions. A live replay reset discards warm-up ledger rows before this average. Planned counts are stored separately in each event's `selection_by_variant`.

## Publication scopes preserve effective state

Repair scopes are inclusive, timezone-free calendar dates. A repair on 5–10 January takes effect on those dates only. At 11 January, the publisher restores the original effective state, even if it is a weekend and the next old snapshot is 20 January or does not exist. Adjacent approved scopes take precedence over a restoration on their shared boundary. An existing original snapshot on the restoration date is preserved. Overlapping scopes and intraday/timezone boundaries are rejected.

The original source revisions and atomic publisher lock remain in use. Candidate, observed-snapshot and identity hashes are checked again under the lock. Backup evidence now preserves candidate bytes, observed evidence and the identity registry alongside the certificate, prior accepted file and recovery manifest. The tests call the actual publisher against temporary files, compare every outside calendar date, exercise adjacent/spaced scopes and boundary collisions, and verify backup restoration. No test publishes into the active shared directory.

## Bounded historical evidence delivered

`outputs/backtest_repair_v2_20261103/bounded_candidate_v3` is an inspectable, **unaccepted** candidate for Nifty 500, 1 July 2017–1 September 2020. This is a bounded repair work package, not a requirement to resolve all unrelated 9,179 old events first.

The evidence collector preserved 40 official NSE Indices monthly archives and 58 dated NSE trading archives, with URLs, retrieval dates and hashes. The original source register contains 376 relevant records, 291 names and 374 unique dated identity/direction transitions; two source records are duplicates and remain inspectable. All 376 records now have exact historical name/symbol/ISIN evidence. The reusable decision register contains 295 security/name/symbol decisions, with explicit verified-date lists. A span between sparse observations does not authorize intervening aliases. Fuzzy matching and modern survivor substitution are not used.

The official 31 August 2020 anchor has 501 security rows, all joined to dated NSE ISINs. Both Tata Motors ordinary and DVR securities are preserved. The supplied 1 September snapshot has 500 rows and contains modern ticker labels such as TMPV and JSWDULUX. The exported anchor comparison exposes this difference; it does not silently replace historical names with today's labels. Security IDs remain distinct from issuer and vendor identities. Issuer identifiers that have not been established are null, and unverified vendor aliases remain null.

The candidate is rejected because security lifecycle timing and price/vendor coverage remain incomplete. The stable-ID backward walk exposes three conflict dates involving four security IDs; its forward round trip fails, and 38 monthly comparisons disagree. These discrepancies include unresolved ISIN conversions and are not proof that all corresponding company membership events are missing. The full monthly evidence worklist contains 24 observed ID transitions across 23 tickers, with last-old/first-new observations and affected source rows. Twenty-two transitions are observed by the August anchor; two observation bounds extend into the September cross-check and need scope-applicability review. Unrelated later actions are not prerequisites to this bounded repair. No intermediate effective date is invented. An IRCON issuer filing supports its CIN, new ISIN and 1:5 split ratio, but does not establish an exchange-effective conversion date; that date remains unresolved and no portfolio conversion is authorized.

Prices are audited for all 652 historically observed/reconstructed ticker identities in the scope. Of those, 144 lack a usable local adjusted series during the interval under the historical ticker. This mixes retired tickers, mergers, delistings and unverified vendor aliases; it does not mean all historical raw exchange prices are unavailable. Every such identity remains in the worklist, with dated primary ISIN observations. Raw NSE closes are not spliced into Yahoo-adjusted histories.

Three official event notices independently corroborate date/direction for 56 source records, including the September 2017 rescheduling and June 2020 review. Remaining records still need comparable effective-date/completeness checks; a notice that amends a main review does not by itself certify all review events. Primary dates distinguish the effective session from the preceding session close.

No complete daily interval has been accepted or published. Consequently the app's legacy/uncertified mode remains truthful, and a supported-scope strategy matrix has not been run. A combined Nifty 500 candidate does not establish individual Midcap 150/Smallcap 250 partitions or missing early microcaps. Future acceptance must use the actual reviewed certificate and scope, not a global warning switch.

## Evidence and next gate

See the versioned implementation report, before/after probes, identity recheck, complete test logs and independent accounting reconciliation in the same output directory. The source-ledger audit covers a fresh 2024–2026 legacy-data diagnostic, not a repaired-universe performance study. Historical results and source inputs remain unchanged. No Live Signal checkpoint was advanced, and nothing was pushed or deployed.

Next work is specific: determine which transition bounds apply to the scope and obtain their effective-date lineage evidence, complete constituent-event timing/completeness checks, validate the stable-ID round trip against the monthly anchors, and resolve the historical/vendor adjusted-price worklist. Only then can the bounded scope be certified, published and used for matched strategy comparisons.
