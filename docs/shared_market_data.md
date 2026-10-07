# Shared market data

The application has two authoritative market-data files:

| File | Contents | Owner |
|---|---|---|
| `data/screener_ohlcv.parquet` | Full retained equity OHLCV, adjusted ETF quotes, price indices, imported TRI and labeled cash proxy | `scripts/refresh_market_data.py` |
| `data/constituents.parquet` | Complete dated membership snapshots, including historical membership | `scripts/refresh_constituents.py`; explicit historical reconstruction |

`market_data.load_snapshot(as_of)` pins both file revisions for a calculation. Screeners, charts, Momentum replay, Live Signal, breadth and Quant Portfolio use these readers. Dual Momentum reads its explicitly typed series from the same price file. Page reads never fetch vendor data or fall back to another price/membership file.

## Date and revision contract

A date describes the calculation cutoff. A revision identifies the exact accepted file contents, including same-date corrections. In-process and Streamlit caches include source revisions. Reports and Momentum/Stage 2 CSV exports identify both revisions. A historical calculation selects the latest complete membership snapshot at or before its date and excludes later price observations.

Membership effective dates and verification times are separate. An unchanged CSV verification updates file metadata without adding fake historical events. A changed current CSV without an official effective-date source is recorded as `observed_not_effective` on the observation date. It is never backdated into an earlier replay. Official effective-date imports require a source document/URL. Earlier membership snapshots remain stored.

## Ranking contract

Momentum Screener and portfolio ranking use `momentum_engine.precompute_metrics`, the common quality-filter function and the same composite-score calculation. History length, median volume, stale-session limit and quality settings are visible inputs. Ties sort by symbol ascending. Close prices come directly from the shared observation; presentation formatting is separate. Composite summation is normalized across numeric row types.

The acceptance audit compares the eligible symbol list, exact scores, prices, ordering and both revisions between Screener, scalar ranking and the replay ranking path:

```sh
python scripts/verify_market_alignment.py --date 2026-10-06
```

Portfolio holdings can differ from a fresh top-M list because of entry/hold bands, previous holdings, allocation caps and Stage 2 exits. These strategy rules do not alter the underlying eligible ranking. Quant candle strategies and Stage 2 classifications retain their own strategy definitions while sharing source prices and dated membership.

Stale candidates are excluded consistently. Live Signal separately blocks a stale broker/model holding that needs a reliable trade price. Complete-candle strategies explicitly exclude and report missing OHLCV rows; the pricing store preserves valid historical closes without manufacturing candles.

## Publication and scheduling

The existing GitHub Actions workflow remains at **19:45 IST daily**, with manual dispatch available. It first verifies current NSE constituents, then runs the shared pricing publisher, then commits the two files together. GitHub concurrency and a local publisher lock serialize writers. Each file replacement validates data, checks the expected prior revision and publishes atomically. A failed download never erases accepted history. Partial failures and actual coverage are embedded in the file metadata and shown through the shared readers.

```sh
python scripts/refresh_constituents.py
python scripts/refresh_market_data.py
# Explicit official effective-date membership update:
python scripts/refresh_constituents.py --effective-date YYYY-MM-DD --effective-source URL
# Targeted shared series or full history rebuild:
python scripts/refresh_market_data.py --series-type etf_adjusted
python scripts/refresh_market_data.py --full
```

Pricing refreshes use each instrument's own coverage and overlapping sessions. A changed adjusted close requires full stored-history coverage for that instrument. An incomplete correction response retains the prior history and records the failure; it cannot introduce an adjustment seam. Legacy adjustment conflicts stay in `history_rebuild_required` until the complete upstream history is accepted, so later scheduled runs retry them.

TRI CSVs are source ingestion inputs, imported by `scripts/build_dual_momentum_tri.py` into the shared price file. ETF prices, TRI and price indices are distinct series types. RBI repo-rate inputs are auxiliary rate assumptions for the labeled cash proxy, whose generated price history lives in the same price file. Holiday and corporate-action calendars are auxiliary reference data, not competing market-price or membership stores.

The workflow changes are local until pushed/deployed. No remote workflow run or hosted-app deployment is implied by local tests.

## Migration and recovery

The offline migration uses the legacy replay baseline plus its delta as overlap priority, with screener rows filling absent keys and compatible missing candle fields. It preserves the historical union, logs every conflicting close and flags adjustment histories requiring a vendor rebuild. The original files are copied to `outputs/market_data_migration/*.before` before publication. The original legacy files remain migration/recovery inputs and have no active reader path.

The migration is guarded against rerunning over an already accepted shared store:

```sh
python scripts/migrate_market_data.py --effective-date 2026-09-30 --effective-source https://www.niftyindices.com/Press_Release/ind_prs10082026.pdf
```

The September 30 date is sourced from the NSE index review announcement; the existing historical snapshots are preserved. A historical reconstruction writes missing dated snapshots into the shared constituent file and preserves accepted current snapshots.

If a publication fails, the last accepted file remains active. Inspect its metadata and rerun the same publisher. Do not create a page-specific delta or fallback store. A stale lock is never silently stolen: inspect the recorded PID and remove the lock only after confirming that its publisher has stopped. For recovery, stop publishers and restore the explicitly saved source pair, then verify both revisions before reopening readers.
