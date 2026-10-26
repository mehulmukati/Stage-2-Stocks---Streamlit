# Equity observation filtering: before and after

Snapshot date: **2026-10-19**. The same raw source data is used in both calculations.

The previous calculation counted every stored equity row. The new calculation requires finite, positive Close and Volume before indicators, lookbacks, and history counts. Indices, ETFs and cash proxies retain their separate policies. Raw Parquet files are preserved.

## Examples

| Symbol | Previous 1Y return | New 1Y return | Previous window starts | New window starts | Recent coverage | New eligibility |
|---|---:|---:|---|---|---:|---|
| SKYGOLD | 191.04% | 200.04% | 2025-10-20 | 2025-10-10 | 100.00% | eligible |
| CUPID | 567.29% | 582.81% | 2025-10-20 | 2025-10-10 | 100.00% | eligible |
| RELIANCE | -17.67% | -12.60% | 2025-10-20 | 2025-10-10 | 100.00% | eligible |
| CONCOR | -15.18% | -17.05% | 2025-10-17 | 2025-10-10 | 100.00% | eligible |
| AUTOAXLES | -2.70% | -4.27% | 2025-10-15 | 2025-10-10 | 100.00% | low_volume |
| GOODYEAR | -39.41% | -32.35% | 2023-06-14 | 2023-04-25 | 50.00% | incomplete_recent_coverage |
| FRETAIL | 0.00% | -34.86% | 2025-10-20 | 2022-09-07 | 0.00% | stale_price |

For SKYGOLD and CUPID, six zero-volume rows previously occupied positions in the 252-row window. The new window contains 252 positive-volume observations. GOODYEAR has sufficient older observations but only 50% coverage of the latest 252 market sessions, so it fails the new 95% coverage gate. FRETAIL remains excluded for staleness; its last positive-volume observation is 2024-08-01. Its older filtered return above is diagnostic and does not make it eligible.

## Ranking comparison

Using the settings recorded in audit.json (252 accepted observations, median volume 100,000, maximum staleness three sessions and the listed quality filters), eligible current members change from **123 to 141**. Filtering can move stocks above or below quality thresholds; it is not expected to reduce eligibility in every case.

The first five ranks remain CUPID, LAURUSLABS, SHREEJISPG, SKYGOLD and WELCORP. HFCL moves from rank 7 to 6; CPPLUS from 13 to 10; LOTUSDEV enters the top 15. See rank_changes.csv for the complete comparison.

## Source retention and scope

13,758 equity rows are excluded in memory from 2,200,941 raw equity observations. These include 13 non-flat zero-volume candles retained for source review. The filter does not delete, replace or rewrite these rows.

Coverage uses the union of accepted equity session dates across the available market, preserving special weekend sessions. Expected sessions after the last market observation are counted using weekdays excluding known CM holidays. A session absent from every source instrument cannot be inferred from the historical observed calendar.

Score, chart, breadth, strategy and portfolio caches include the equity policy version so old results are recomputed without a new source-file revision.

- before_after_examples.csv: numerical examples and eligibility reasons.
- rank_changes.csv: previous and new eligible ranks.
- audit.json: settings, revisions and full numerical results.
- alignment_20261019/: screener versus scalar and precomputed replay verification.
- data_integrity.json: before/after hashes of the data Parquet files.

## Validation

The full regression run passed all 355 tests. After the final calendar propagation correction and additional boundary tests, all 144 affected checks passed. The real-data alignment audit passed for October 19: screener, scalar ranking and precomputed ranking agree on 141 eligible symbols, exact scores, prices and ordering. Formatting and lint checks passed. SHA-256 checks confirm that all 11 data Parquet files remain byte-for-byte unchanged. Validation details are in validation.json.

The same market-wide calendar is passed into Momentum replay, Live Signal and Quant Portfolio before selecting a universe. Missing sessions cannot disappear from coverage merely because the selected stocks all have gaps on those dates.
