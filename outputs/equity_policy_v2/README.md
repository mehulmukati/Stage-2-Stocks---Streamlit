# Equity observation policy v2 — implementation and audit

Implemented directly on `main` on 2026-10-26. The comparison baseline is the
merged PR #12 policy, using the same source snapshot through 2026-10-23.
Validation was completed locally before commit and push; no hosted deployment
was performed during validation.

## Published data changes

The canonical `data/screener_ohlcv.parquet` was cleaned and republished with
observation quality flags. It contains 2,236,118 rows, down from 2,242,028.

| Change | Rows |
| --- | ---: |
| Removed zero/unknown-volume equity placeholders on verified full closures | 5,910 |
| Restored erroneous zero volume using official NSE evidence | 13 |
| Retained other zero-volume equity observations, flagged as unverified | 7,839 |

Removed rows by date: January 15 (908), May 1 (918), May 28 (1,110), June 26
(1,115), September 14 (1,022), and October 2 (837), all in 2026. Different counts
from the initial investigation reflect the latest snapshot used for this cleanup.
Positive-volume observations on a verified closure are retained as calendar
conflicts for review. Other series types and retained adjusted OHLC prices are
unchanged. Only the 13 verified volume fields were repaired.

The exact original is archived in
`archive/screener_ohlcv.b888e488d13b45b2b8f5dce2ad875eb4.parquet.before`.
It matches the original SHA-256 and is excluded from Git. All ten other parquet
files match their pre-implementation hashes. See [data_integrity.json](data_integrity.json).

## Calculation behavior

- Calculations align to the shared exchange calendar. A 1Y window uses 252
  session positions and 251 daily-return intervals; it cannot extend backward
  to collect enough trading rows.
- Zero-volume observations remain stored. A verified no-trade closing mark can
  support price calculations, while execution still requires evidence of a trade.
  Flat prices alone do not establish a verified mark. All 7,839 remaining zero
  rows currently lack sufficient verification and remain explicit price gaps.
- Unknown prices are not filled or skipped when calculating daily returns,
  Sharpe, positive-day percentages or moving averages. A return can use trusted
  endpoints, subject to freshness checks; Sharpe requires complete consecutive
  return intervals. Composite scores require every selected period.
- Eligibility separately checks supported history, recent trading and at least
  95% trading coverage. Charts and candle strategies preserve missing positions;
  missing weeks do not manufacture candles or entry/exit signals.
- Screener, Stage 2, breadth, momentum, strategy and portfolio paths use the
  revised policy. Source and policy versions invalidate derived results. The
  publisher repeats the cleanup on future refreshes without vendor calls on page reads.

## Before and after examples

The following results use the 2026-10-23 cutoff. “Unavailable” means the fixed
recent window lacks the required trusted prices, not that the stored rows were deleted.

| Symbol | Previous 1Y window start | New 1Y window start | Previous 1Y return / Sharpe | New 1Y return / Sharpe |
| --- | --- | --- | --- | --- |
| SKYGOLD | 2025-10-15 | 2025-10-15 | 222.06% / 2.685 | 222.06% / 2.685 |
| CUPID | 2025-10-15 | 2025-10-15 | 512.34% / 3.388 | 512.34% / 3.388 |
| GOODYEAR | 2023-04-28 | 2025-10-15 | -37.27% / -0.805 | Unavailable |
| FRETAIL | 2022-09-07 | 2025-10-15 | -34.86% / -0.410 | Unavailable |

GOODYEAR has 123 missing trusted prices in the fixed 252-position window;
FRETAIL has 252. Both were already ineligible under the previous coverage or
staleness gates; the change also prevents misleading historical metrics.
Healthy names retain their existing results. In the audited 751-member universe,
89 names are eligible under both policies, with the same top 15.

See [before_after_examples.csv](before_after_examples.csv),
[all-symbol comparison](before_after_all_symbols.csv) and
[comparison settings](before_after_summary.json).

## Verification and scope

All 367 tests passed. The real-data alignment check confirms identical eligible
names, order, scores and prices across the screener and scalar/precomputed
backtest rankings. Formatting, import sorting, lint and diff whitespace checks
passed. Reapplying the publisher transformation to the published snapshot
produces zero further removals or repairs and leaves retained raw price fields
unchanged. See [validation.json](validation.json) and
[rank alignment](alignment_20261023/rank_alignment.json).

The calendar is independently verified for 2026 from the
[annual NSE CM circular](https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf)
and [January 15 supplement](https://nsearchives.nseindia.com/content/circulars/CMTR72260.pdf).
It includes the November 8 Muhurat session. Earlier years use market-wide
observed trading dates as an explicit fallback; no historical row is removed
based on an unverified closure. Additional years need verified calendar updates.

The 13 volume corrections use the
[official NSE bhavcopy for May 24, 2023](https://nsearchives.nseindia.com/content/historical/EQUITIES/2023/MAY/cm24MAY2023bhav.csv.zip).
All four OHLC ratios must agree on a common adjustment basis before volume is
repaired. The source archive hash and matches are saved in `source_verification/`;
the repeatable repair entries are in `data/equity_observation_verifications.json`.
These corrections do not establish which upstream step originally introduced
the bad volume values or holiday placeholders.

Detailed publication evidence is in [cleanup_applied.json](cleanup_applied.json),
[removed rows](removed_holiday_rows.csv),
[retained uncertain rows](retained_uncertain_observations.csv) and
[verified repairs](verified_volume_repairs.csv).
