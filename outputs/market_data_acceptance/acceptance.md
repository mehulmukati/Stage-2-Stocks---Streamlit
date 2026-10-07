# Shared market data implementation acceptance

Implemented locally on 7 October 2026. All app sections read one pricing file (`data/screener_ohlcv.parquet`) and one dated constituent file (`data/constituents.parquet`). Legacy stores remain recovery inputs and are no longer active data sources.

## Original discrepancy

The supplied exports used identical Sharpe scores but different membership snapshots: the Screener used September 30 constituents while Live Signal used July 31 constituents. Excluding SHREEJISPG and UNIMECH moved LOTUSDEV from rank 17 to 15. Live Signal also enforced a 100,000 median-volume floor missing from the Screener. CPPLUS was already held; the Entries table listed newly entered positions, not the entire portfolio. See `outputs/rank_forensic_20261006_report.md` for the original export reconciliation.

## Acceptance result

For October 6, 2026, the five selected core indices contain 750 symbols. With matching supplied quality filters, 252 history rows, median volume >=100,000 and at most three stale sessions, all **146 eligible symbols** have exactly matching prices, scores and ranks between the Screener and scalar/precomputed portfolio ranking paths.

| Symbol | Aligned result |
|---|---|
| LOTUSDEV | Rank 16 |
| CPPLUS | Rank 13 |
| UNIMECH | Excluded: median volume 74,190.5 is below 100,000 |
| SHREEJISPG | Rank 4 |

A held position can remain outside the top 15 under the configured exit band of 30; rank parity does not mean a marginal-rebalance portfolio always contains the current top 15.

Price revision: `9f51df58606b415b95cb47432f8815fb`. Constituent revision: `06afd4354bd9473f9f88d67d732e2d12`. A final metadata correction narrowed coverage reporting to the selected five indices; an exact comparison of all 2,225,108 price rows confirmed prices were unchanged. The machine-readable audit records this correction and both revisions.

## Validation and publication

- Full test suite: 313 passed.
- Subsequent focused writer, shared-source and breadth checks: 33 passed.
- Four Streamlit page smoke checks passed without exceptions.
- Shared ranking regression tests cover eligibility, exact score agreement, deterministic ties, same-date corrections, revision invalidation and disabled volume filters.

The existing GitHub Actions schedule remains 19:45 IST. Its updated workflow verifies constituents, refreshes the shared pricing file and commits both sources. A background refresh in the app uses the same publishers. Source revisions are exposed in relevant results and exports.

## Remaining upstream coverage

Nineteen incomplete historical rebuilds retain prior accepted histories, remain explicitly flagged and will be retried by the shared publisher. BAGMANE, BIRET and EMBASSY lack target-session prices. The configured SMALL250BES.NS ETF download is unavailable upstream. These gaps are reported; no replacement quotes or candles are fabricated.

The code, source migration and workflow changes are local. No commit, push or hosted-app deployment has been performed; the updated remote schedule becomes active after these changes are published.
