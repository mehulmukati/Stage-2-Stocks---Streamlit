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

Stale candidates are excluded consistently. Live Signal separately blocks a stale broker/model holding that needs a reliable trade price. Complete-candle strategies report missing OHLCV and preserve their session positions without allowing execution; the pricing store preserves valid historical closes without manufacturing candles.

## Equity observation policy (version 2)

The canonical price Parquet is selectively cleaned. Only equity placeholders on
verified complete NSE CM closures are removed. All other zero-volume or
unknown-volume observations remain stored with quality flags. Indices, ETFs and
the cash proxy retain their separate policies.

Stored rows carry `observation_status`, `price_status`, `volume_status`,
`is_tradable`, `verification_source` and `verified_at`. A positive-volume,
positive-close observation establishes trading. A zero-volume close is usable
as a verified no-trade mark only when its verification source and timestamp
are supplied explicitly; flatness alone is not proof. Uncertain quotes remain
in the dataset and are masked in calculation views. Positive-volume quotes on
a verified closure are retained as calendar conflicts for review.

`data/nse_session_calendar.json` records the verified 2026 calendar, official
NSE circulars, complete closures and the Muhurat session. Before verified
years, market-wide observed trading dates provide an explicitly documented
fallback, including observed special weekend sessions. No historical row is
deleted on an unverified date. Expected sessions through the requested cutoff
still count when the whole market snapshot is stale.

Calculations use a common exchange-session grid before selecting a universe.
A 1Y window contains 252 fixed session positions and 251 daily-return intervals;
it never extends backward to collect 252 trades. Return endpoints require a
trusted close and trading evidence within three sessions of each boundary.
Sharpe, positive-day percentages and moving averages do not silently skip
unknown prices or bridge gaps. Composite Sharpe scores require all requested
periods. The 52-week high uses actual trading highs in its fixed window and is
unavailable when required data are missing. Verified no-trade marks can support
Close-based indicators but do not manufacture OHLC candles or execution prices.

History length counts supported price sessions, including verified no-trade
marks. Eligibility separately requires recent actual trading (three-session
default) and at least 95% actual trading coverage of the latest 252 market
sessions since the first observation. A history with 240 recent trades and 12
ancient trades can no longer report a years-long 1Y return. Historical
calculations cut observations and calendar positions at their requested date.

Charts and candle strategies preserve missing session positions. Weekly candles
aggregate real trading candles; wholly missing weeks remain empty positions.
Portfolio valuation may carry an existing holding's last known value, while
execution requires trading evidence and an available execution price.

`data/equity_observation_verifications.json` records narrowly scoped repairs
with source hashes and verification times. The initial cleanup restores volume
for 13 May 24, 2023 candles using NSE's official bhavcopy, after confirming that
the raw and adjusted candle shapes agree. Adjusted OHLC prices are unchanged.
Every subsequent publisher repeats the same idempotent cleanup and preserves
matching verified marks, preventing rejected placeholders from accumulating.

The cleanup is read-only by default, writes an audit of all removals and
repairs, and archives the exact original before atomic publication:

```sh
python scripts/clean_equity_observations.py
python scripts/clean_equity_observations.py --apply
```

Recovery copies in `outputs/equity_policy_v2/archive/` are not application
fallbacks and are excluded from Git. The manifest records the original revision
and SHA-256. Stop publishers and restore that archived source if rollback is
needed. Other market files are not changed by cleanup. Source revisions and the
independent equity policy version invalidate derived caches and portfolio
results after publication or policy changes. Page reads never request vendor
verification data.

## Publication and scheduling

The existing GitHub Actions workflow remains scheduled for **19:45 IST daily**, with manual dispatch available; GitHub may start scheduled runs later. It first verifies current NSE constituents, then runs the shared pricing publisher, then commits the two files together. After committing accepted data, a read-only coverage check fails the workflow if no current constituents cover the target session or any of the three Live Signal benchmarks lack a target-session price. Useful partial observations remain published, while a green run cannot hide a universe-wide lag. GitHub concurrency and a local publisher lock serialize writers. Each file replacement validates data, checks the expected prior revision and publishes atomically. A failed download never erases accepted history. Partial failures and actual coverage are embedded in the file metadata and shown through the shared readers.

```sh
python scripts/refresh_constituents.py
python scripts/refresh_market_data.py
python scripts/refresh_market_data.py --check-coverage
# Explicit official effective-date membership update:
python scripts/refresh_constituents.py --effective-date YYYY-MM-DD --effective-source URL
# Targeted shared series or full history rebuild:
python scripts/refresh_market_data.py --series-type etf_adjusted
python scripts/refresh_market_data.py --full
```

Pricing refreshes use each instrument's own coverage and overlapping sessions. A changed adjusted close requires full stored-history coverage for that instrument, except known zero-volume non-trading placeholders that Yahoo omits. An omitted weekend trading quote is retained only when overlapping observations on both sides (within seven calendar days) confirm the same adjustment basis. A changed basis, missing boundary, or omitted ordinary trading observation still rejects the rebuild. Legacy closes with unknown volume require the same coverage checks. Only verified complete-closure equity placeholders are removed. Other uncertain observations remain stored with quality flags and do not establish tradable coverage. An incomplete correction response retains the prior history and records the failure; it cannot introduce an adjustment seam between accepted trading observations. Legacy adjustment conflicts stay in `history_rebuild_required` until the complete upstream history is accepted, so later scheduled runs retry them.

Generate Signal reads the accepted shared revision. It does not fetch prices. To repair stale prices, run the shared publisher (locally) or the Refresh Shared Market Data workflow (for the hosted app), then generate the signal again.

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
