# Missing data at rebalance: standing user instruction

Recorded: 31 October 2026.

User instruction:

> For all backtests, if there is data missing for any stock during a rebalance,
> please exit that stock in that rebalance at the last available price. Replace
> it with another stock based on the ranking.

Apply the following in all future project backtests:

1. For a held stock, first use any valid fallback candle permitted by that run's
   execution rules. A missing preferred hourly candle alone does not trigger a
   stale-price sale if an allowed fallback candle is available.
2. If no usable execution quote exists in the permitted rebalance window, exit
   the holding at that rebalance using its most recent available price known
   by the execution cutoff. Never use a price from a future date or a later
   candle that had not completed by that cutoff.
3. Apply the run's normal sell-side fees and slippage to that reference price.
   Record the actual source-price timestamp, its age, and the reason for using
   a stale price. This is an assumed fill at a historical price, not evidence
   that the stock was actually tradable at that price.
4. Use sale proceeds to fund the highest-ranked eligible unheld replacement
   with valid execution data. Preserve the existing allocation, cash-scaling,
   tax-reserve, no-borrowing and no-same-rebalance-reentry rules. If no eligible
   replacement exists, retain the proceeds as cash.
5. Do not retain a holding across successive rebalances solely because an
   executable candle is missing. Missing required signal data also makes a
   holding ineligible under the existing rules; use a valid rebalance quote
   when available, otherwise the historical-price exit convention above.
6. Preserve the existing tolerance for gaps inside historical lookbacks. This
   instruction does not reintroduce a percentage-completeness screen.

Implementation status, updated 31 October 2026: implemented as the default in
`available_observations/run_backtest.py`. A missing-price sale is booked at the
primary execution candle's close (11:15-labelled hourly candle, or daily close).
The newest valid completed hourly candle or daily close known by that time is
used, preferring the run's execution source when completion times tie. Its source
timestamp, availability time and price age are exported with the trade.

Weekly and monthly midday, no-regime reruns passed 20 engine/schedule tests and
independent source-price, timing, latest-price-selection, cash/NAV and tax
reconciliation. Revised results are in
`available_observations/results_hourly_midday_weekly_lastprice` and
`available_observations/results_hourly_midday_monthly_lastprice`; reports are in
`Report/Last_Price_Exit_Weekly_vs_Monthly` relative to the project root.

Earlier saved results and source snapshots retain the previous convention.
Clearly label new runs as using this revised convention. Do not overwrite
earlier reports or imply that their results already use this policy.
