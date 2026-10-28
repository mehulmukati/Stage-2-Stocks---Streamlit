# Live Signal portfolio continuation

The default **Continue actual portfolio** mode applies one scheduled strategy
decision to the positions actually owned. It does not replay a hypothetical
portfolio from inception. The shared ranking, Classic/Displacement selection,
Stage 2 conditions and weight variants remain the strategy rules.

## Start once

1. Choose **Initialize or reset starting checkpoint (no trades)**.
2. Upload actual broker positions, enter available cash and the cash reserve,
   and select the date those positions represent.
3. Set the strategy and **Fixed schedule anchor**. Weekly uses this date's
   weekday; biweekly also preserves its fortnight. Keep the anchor fixed.
   Monthly, quarterly and half-yearly use completed calendar-period closes.
4. Create and save the starting checkpoint. This records the current holdings,
   current-policy Stage 2 baseline, settings, source revisions and policy versions.
   It does not recommend or execute any trades.

Initialization is an explicit new baseline. It does not reconstruct an unknown
past score or silently reinterpret trades performed under previous calculations.

## Subsequent decisions

1. Choose **Continue from confirmed checkpoint** and upload that checkpoint.
   Its settings are restored once; subsequent changes are checked against it.
2. Upload the positions actually owned immediately before the decision, and
   update available cash and the reserve. Checkpoint quantities are evidence of
   the prior decision, not a substitute for the actual broker snapshot.
3. Choose the scheduled signal date. On a holiday, the decision uses the last
   exchange session before it. Holiday and weekend period ends are resolved by
   the shared exchange calendar, including at the selected endpoint.
4. Generate and review the proposed orders. Save the trade CSV and decision
   audit JSON. Neither generation nor download advances the checkpoint.
5. After executing all orders, confirm that fact and save the **confirmed
   checkpoint for next signal**. If no orders are needed, confirm the review and
   save the checkpoint so the score baseline advances to this decision.

For partial fills or external changes that make the previous score baseline
unusable, initialize a new checkpoint explicitly from the actual portfolio.
Corporate-action conversions must be reflected in actual broker quantities.
Missing scores or unresolved old security identities block orders rather than
guessing successor quantities or cross-security score changes.

Checkpoint JSON files are portable user-owned records. Keep each confirmed file
and its decision audit; they are not stored on a shared application server. Each
contains an integrity hash and the preceding checkpoint ID. The hash detects
accidental edits; it is not a digital signature or proof of broker execution.

## Guarantees and stopping conditions

- The signal cutoff cannot change the schedule or its historical anchor.
- Actual quantities determine held stocks and current relative weights.
  Classic holds an eligible incumbent through rank N; an absent stock must
  qualify for entry through rank M or an enabled Stage 2 entry rule.
- Saved prior Stage 2 scores are immutable even after a price-file refresh.
  Current scores use the current accepted source at the decision close.
- A changed calculation policy or strategy requires an explicit new baseline.
  Ordinary source revisions do not rebuild prior portfolio decisions.
- Before the next scheduled decision is due, no orders are generated. Reusing
  the same confirmed checkpoint produces the same proposal on the same data.
- Multiple missed decisions are not replayed as hypothetical trades against
  current quantities. A new baseline must be established explicitly.
- A later valuation date cannot silently substitute for an older pending
  decision. Select its decision date or start a new baseline.
- Missing whole-market prices, unpriced/stale actual holdings, missing Stage 2
  baselines and infeasible position caps block orders.
- Editing inputs hides the previous orders and execution confirmation until a
  new result is generated. A new source revision also invalidates the result.
- Marginal positions that have no genuine weight change retain their quantities;
  floating-point rounding cannot create a one-share trim.

Quality filters still apply to incumbents before ranking. For example, a stock
can exit after falling more than the permitted distance from its 52-week high,
even if its Sharpe rank would otherwise be acceptable. Such exits are identified
as eligibility exclusions, rather than incorrectly labelled index removals.

The displayed **Gross turnover (buys + sells)** counts both sides. The CSV's
Signal Date is the actual decision close; Requested Cutoff records the selected
date when it differs because of a holiday.

## Research and model alignment

**Replay from start date (research)** remains available for historical model
reconstruction. **Align actual portfolio to replayed model** deliberately trades
an actual portfolio toward that reconstruction. Its warning explains that this
can reverse earlier trades and resize retained positions. Both use a fixed
schedule anchor and do not present an old rebalance as a new off-schedule signal.

The existing Excel replay audit remains available for those research modes.
Continuation uses a decision JSON audit carrying the actual inputs, previous
checkpoint ID, ranking, eligibility exclusions, pre-trade/target weights, prices,
source/policy provenance and the unconfirmed next checkpoint. This keeps actual
continuation distinct from independent historical replay verification.

When changing the continuation decision semantics, bump
`live_signal_state.DECISION_POLICY_VERSION`. When changing the checkpoint schema,
bump `STATE_VERSION`; older records must be migrated explicitly. The equity data
policy version is also checked before continuing.
