# Data & Methodology

## Data source

All price data is sourced from **Yahoo Finance** via the `yfinance` library. NSE stocks are fetched with the `.NS` suffix (e.g. `RELIANCE.NS`). Benchmark indices use their Yahoo Finance tickers (`^NSEI` for Nifty 50, `^CRSLDX` for Nifty 500).

Yahoo Finance provides **adjusted close prices** (adjusted for splits and dividends). All return and MA calculations use adjusted prices, which means historical charts show what an investor would have actually experienced, not raw price levels.

---

## Cache architecture

### Screener — 3-tier cache

Data flows through three tiers to minimise redundant fetches:

```
Request
   │
   ▼
Tier 1 — In-memory dict
   │  (same process, keyed by last trading date)
   │  HIT → return immediately
   ▼
Tier 2 — screener_ohlcv.parquet on disk
   │  (persists across restarts; checked if memory cache is stale)
   │  HIT → load from file, populate memory cache
   ▼
Tier 3 — yfinance (internet)
      (only when parquet is stale — fetches incrementally from last parquet date)
      → merge into screener_ohlcv.parquet → populate memory cache
```

The screener parquet is updated in-place on each delta fetch.

### Backtester — 4-tier cache

The backtester keeps the committed baseline parquet read-only (so git stays clean) and
maintains a separate gitignored delta cache for tail rows:

```
Request
   │
   ▼
Tier 1 — In-memory dict
   │  (keyed by target trading date; survives for the container lifetime)
   │  HIT → return immediately
   ▼
Tier 1b — In-memory baseline DataFrame
   │  (materialised once from Tier 2 + 2.5 combined; amortises read_parquet)
   │  HIT → compute gap, skip to Tier 3 if needed
   ▼
Tier 2 — backtest_history.parquet (committed to repo, never written at runtime)
   +
Tier 2.5 — backtest_delta.parquet (gitignored, grows with each fetch)
   │  Merged on first load; last_date = max(Tier 2, Tier 2.5)
   ▼
Tier 3 — yfinance (internet)
      Only dates after max(Tier 2, Tier 2.5) last date
      → new rows appended to backtest_delta.parquet → populate memory cache
```

This means on a typical day after the first run, the gap is 0 or 1 day and yfinance
is either skipped entirely or fetches just that day's data. The committed baseline
never changes between explicit rebuilds.

---

## Parquet file layout

### Screener

| File | Committed | Contents |
|---|---|---|
| `data/screener_ohlcv.parquet` | ✅ | Long-form `{symbol, date, Open, High, Low, Close, Volume}` for ~750 NSE symbols, ~2 years |
| `data/stage2_cache.parquet` | ❌ Gitignored | Most-recent Stage 2 scores with a `cache_date` column |
| `data/momentum_cache.parquet` | ❌ Gitignored | Most-recent Momentum scores with a `cache_date` column |

### Backtester

| File | Committed | Contents |
|---|---|---|
| `data/backtest_history.parquet` | ✅ | Long-form `{symbol, date, Close, High, Volume}` for ~750 NSE symbols, ~10 years |
| `data/benchmarks.parquet` | ✅ | Nifty 50 & Nifty 500 daily close history |
| `data/compositions.parquet` | ✅ | Historical index constituent snapshots |
| `data/backtest_delta.parquet` | ❌ Gitignored | Accumulated yfinance tail rows (OHLCV) since last baseline rebuild |
| `data/benchmarks_delta.parquet` | ❌ Gitignored | Accumulated yfinance tail rows for benchmarks |

Score caches store only the most recent scored date. On read, the `cache_date` column is compared to the target trading date; a mismatch triggers a re-score and overwrites the file atomically.

Concurrent writes are protected by a `threading.Lock` (same-process serialisation) plus an atomic `tempfile` + `os.replace()` rename so that readers always see either the complete old file or the complete new file.

### Seeding the screener baseline

Run once after cloning:

```bash
python scripts/refresh_screener_parquet.py
```

This downloads ~2 years of OHLCV for all symbols in `constituents.json` and writes `data/screener_ohlcv.parquet`. After the initial seed the app performs incremental delta fetches automatically at startup.

---

## NSE trading calendar

The app resolves the "last valid trading date" by walking backwards from the current date, skipping:
- Weekends (Saturday, Sunday)
- NSE market holidays from `nse_holidays.json`

The holiday file covers all NSE segment holidays (equity, F&O, currency). The back-walk extends up to **10 days** to handle extended closure windows such as Diwali, Budget Day, and consecutive public holidays.

After-market cutoff is set at **7:00 pm IST**. Before 7 pm, the app uses the previous trading day as the cache key (today's data may not yet be available on yfinance). After 7 pm, it targets today's date.

---

## Incremental sync

When the parquet baseline is stale, data is fetched from 5 days before the last parquet date (small overlap to avoid missing the most-recent partial day). This keeps incremental syncs fast (typically a few seconds).

A full rebuild is triggered only when: the parquet file does not exist, it is empty, or the latest date is more than ~2 years old. Force a full rebuild at any time with:

```bash
python scripts/refresh_screener_parquet.py --full
```

---

## Constituent universe

The app screens the stocks present in **`constituents.json`**, which maps each NSE index to its current list of symbols. This file is generated by the `data/collate_constituents.py` pipeline and should be refreshed whenever index compositions change (typically quarterly at NSE rebalances).

---

## Moving average periods

The app uses **simple moving averages (SMA)** for all MA calculations:

| MA | Period | Role |
|---|---|---|
| MA50 | 50 days | Short-term trend (~10 weeks) |
| MA150 | 150 days | Intermediate trend (~30 weeks) |
| MA200 | 200 days | Long-term structural trend (~40 weeks) |

Weinstein's original work uses weekly charts with 30-week and 10-week MAs. The app works on **daily data**, so the MA50 (≈10 weeks of daily bars) and MA150 (≈30 weeks) are the direct equivalents.

---

## RSI

The app uses **Wilder's RSI** with a 14-period exponential smoothing (alpha = 1/14):

```
avg_gain = EWM(gains, alpha=1/14)
avg_loss = EWM(losses, alpha=1/14)
RSI = 100 − 100 / (1 + avg_gain/avg_loss)
```

When `avg_loss = 0` (a period of all-positive closes), RSI is defined as 100 rather than undefined.

---

## Sharpe ratio

The Sharpe ratio used throughout is the **standard annualised Sharpe with no risk-free rate**:

```
Sharpe = mean(daily_returns) / std(daily_returns) × √252
```

This is appropriate for relative ranking — since all stocks face the same risk-free rate, omitting it does not change the ranking order. The metric favours stocks with a high and consistent daily return relative to their daily volatility.
