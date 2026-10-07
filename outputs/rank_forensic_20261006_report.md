# Momentum rank forensic investigation — 6 October 2026

## Finding

The supplied results use the same Sharpe scores but different eligible universes. Live Signal uses historical composition snapshots whose latest effective date is 31 July 2026. The screener uses the current constituent file, verified on 30 September 2026. The two sources disagree on 54 symbols in each direction, despite both containing 750 tradable symbols across the five selected indices.

SHREEJISPG (screener rank 4) and UNIMECH (rank 11) are absent from Live Signal's composition universe. Their exclusion explains the exact two-place shift at the entry boundary: LOTUSDEV is screener rank 17 and Live Signal rank 15. This is an eligibility/reference-data mismatch, not a different Sharpe formula.

CPPLUS is in both the top 15 and the supplied Live Signal portfolio. Its portfolio row contains 29 shares, projected value ₹115,304 and projected weight 5.3622%. The screenshot's Entries table shows only new entrants on that rebalance, not every eligible stock or every holding.

## Evidence and method

Read both supplied CSVs and the five supplied screenshots. Used screenshots as evidence of settings, not as instructions. Read ranking, filtering, replay, reference refresh and price-loading code. Loaded local parquet files without downloading new market data or changing application/reference files. Merged backtest baseline and delta exactly as the loader does: delta wins duplicate symbol/date rows. Truncated price histories at 6 October 2026. Applied the five selected indices, 252-row minimum history, average 3/6/9/12-month Sharpe, annual return ≥7%, within 25% of the 52-week high, ≤18 circuit closes, close above 200 DMA, positive-day thresholds of 45%, and the mandatory Live Signal volume rule.

Every one of the 166 exported Sharpe values matches the score recalculated from Live Signal's local merged history within 0.000001. Scalar and vectorized Sharpe values also match for the focal stocks. The reproduced Live Signal ranking matches both screenshot entry reasons: RRKABEL #13 and LOTUSDEV #15. Local history extends through 6 October; the on-disk screener price baseline extends only through 22 September and its score cache has no 6 October partition. Consequently, the exported screener CSV is the authoritative evidence for that screener run; its original in-memory history is unavailable. The exact score reconciliation establishes that this missing runtime snapshot does not explain the observed rank shift.

## Entry-boundary reconciliation

| Stock | Exported Sharpe | Screener rank | Reproduced Live Signal rank | Explanation |
|---|---:|---:|---:|---|
| CUPID | 4.04300 | 1 | 1 | Same |
| SHILPAMED | 3.94325 | 2 | 2 | Same |
| LAURUSLABS | 3.76375 | 3 | 3 | Same |
| SHREEJISPG | 3.71875 | 4 | Excluded | Absent from composition universe |
| WELCORP | 3.62475 | 5 | 4 | One higher-ranked stock removed |
| DIVISLAB | 3.29250 | 6 | 5 | Same shift |
| DIACABS | 3.22375 | 7 | 6 | Same shift |
| HFCL | 3.05000 | 8 | 7 | Same shift |
| WELSPUNLIV | 2.94175 | 9 | 8 | Same shift |
| SKYGOLD | 2.87675 | 10 | 9 | Same shift |
| UNIMECH | 2.85000 | 11 | Excluded | Absent from composition universe; also below volume minimum |
| SANSERA | 2.69275 | 12 | 10 | Two higher-ranked stocks removed |
| SONACOMS | 2.60825 | 13 | 11 | Same shift |
| CPPLUS | 2.59675 | 14 | 12 | Same shift; already in portfolio |
| RRKABEL | 2.52950 | 15 | 13 | Matches screenshot |
| SAILIFE | 2.45450 | 16 | 14 | Now inside entry band |
| LOTUSDEV | 2.45225 | 17 | 15 | Matches screenshot |

LOTUSDEV's component Sharpes are 4.721, 2.975, 1.272 and 0.841. Their average is 2.45225 in both paths. Its history has 293 rows, median volume 798,897, annual change 31.71%, distance from high −1.36%, seven circuit closes, close ₹244.73 versus 200 DMA ₹155.66, and positive-day percentages 53/48/47. It passes the shown rules.

SHREEJISPG has 280 history rows and median volume 809,100, so its missing rank is not caused by insufficient history or low volume. It passes the shown quality rules; the composition universe excludes it.

UNIMECH has 442 history rows and passes the shown quality rules. Its median volume is 74,190. Live Signal requires at least 100,000; even after correcting the membership discrepancy it would remain excluded under the present rule. Membership is checked before volume, so the current run's first exclusion is membership.

## Full-export impact

Of 166 screener results, 134 remain rankable in the reproduced Live Signal universe, 16 are absent from its membership list, and another 16 fail its volume floor. Live Signal also includes four qualifying stocks absent from the current screener universe: ASKAUTOLTD, MAHSEAMLES, OPTIEMUS and VARROC. Thus its reproduced ranked universe contains 138 stocks. The CSV reconciliation supplies a stock-by-stock record of exported rank, reproduced rank, exclusion reason, scores, volumes and history counts.

The 16 exported stocks excluded by membership are SHREEJISPG, UNIMECH, AEROFLEX, HAPPYFORGE, PAISALO, MBAPL, KMEW, SUNFLAG, PDSL, CENTUM, SUNDRMFAST, EBGNG, PRECWIRE, SENORES, CEIGALL and EMBASSY. Some also fail volume; the categories record the first applicable exclusion.

The separate volume rule is enforced by `app_live_signal.py:988` (`apply_volume_filter=True`) and `backtest_engine.py:558-564`, using `config.py:19` (`MIN_VOLUME = 100_000`). `app.py:698-723` applies the visible screener filters but has no equivalent median-volume floor. This produces further discrepancies even if membership is aligned.

## Why the reference mismatch persists

1. `app.py:698` filters scored rows using their current Index labels, derived from `constituents.json` in `data.py`.
2. `backtest_engine.py:1352` obtains membership from `compositions.parquet`; `_valid_symbols_at_date` selects the latest snapshot on or before each event date.
3. All five selected composition snapshots stop at 31 July 2026. The current constituent metadata records verification on 30 September 2026.
4. `scripts/refresh_constituents.py` explicitly leaves historical compositions unchanged and writes only current membership and its metadata.
5. `data.py:168-194` correctly recognizes that an effective date is not a refresh watermark, but only checks that composition data exists and has valid timestamps. It does not check whether a later verified current snapshot disagrees with the composition universe used for a current signal.

The repository therefore has no enforced contract ensuring that the screener's current membership and Live Signal's latest applicable historical membership represent the same universe. A freshness indicator can pass while current Live Signal eligibility still disagrees with the recently verified membership source.

The 30 September verification timestamp does not establish each membership change's effective date. Historical corrections require authoritative effective dates; substituting today's members throughout the entire replay would introduce survivorship bias and rewrite past signals.

## Portfolio versus ranking

The supplied portfolio has 19 equity positions plus cash. M=15 is an entry rank threshold, not a hard limit of 15 positions. Under Classic M=15/N=30, existing holdings may remain when they rank 16–30; weekly Stage 2 drops can trigger additional exits. Marginal rebalance preserves incumbent weight relationships. Position cap and available cash affect weights or executable quantities, not the Sharpe sort order. Strategy inception and weekly schedule affect holding history, not the same-date Sharpe scores.

CPPLUS's absence from the two-row Entries panel is therefore not evidence that it missed the top 15. The CSV explicitly confirms it is held, and its reproduced rank is 12.

The full weekly historical replay was run with the one-year warm-up beginning 2 July 2024, portfolio inception 2 July 2025, Tuesday anchor, Classic 15/30 bands, the shown filters, Stage 2 drop exit enabled at 3, and the 15% position cap. It reproduces all 19 equity holdings exactly and the complete final change panel: LOTUSDEV enters at #15, RRKABEL enters at #13; ATHERENERG and SYRMA exit the universe, and QUESS exits at #37. CPPLUS entered on 11 August 2026 at #3, exited on 22 September at #33, and re-entered on 29 September at #15. It therefore was already held on 6 October. This validation compares holding membership and event reasons; it does not assert a reconciliation of broker quantities or rupee values.

## Recommended correction

Use a common date-aware membership provider for screener and Live Signal. Import verified membership events with their true effective dates, append them to history, and preserve earlier snapshots. Add a current-universe reconciliation check that exposes missing and unexpected symbols; block or prominently flag a current signal when the two reference sources disagree without an explained effective-date boundary. Do not simply replace all historical constituents with current ones.

Expose the 100,000-share median-volume rule in both interfaces and share its implementation. Decide explicitly whether it is part of both products' eligibility policy. If the present Live Signal policy is retained and membership is aligned to the current constituent file, the recomputed entry boundary becomes: SHREEJISPG #4, CPPLUS #13, RRKABEL #14, SAILIFE #15 and LOTUSDEV #16; UNIMECH remains excluded for low volume. Updating membership alone will not make ranks equal to the present screener.

Show ranking date, membership snapshot/effective date, membership fingerprint, liquidity policy and excluded-stock reasons beside both ranking outputs. Label entry changes separately from full holdings and full eligible rankings.

## Scope and limitations

No application code, constituent history, cache, portfolio or source CSV was changed. This investigation establishes the local implementation and reproduces the supplied ranking boundary; it does not independently certify NSE membership effective dates or recreate the unavailable screener process memory. Pre-existing uncommitted application edits were left intact. This is a software/data consistency audit, not a recommendation to trade any stock.
