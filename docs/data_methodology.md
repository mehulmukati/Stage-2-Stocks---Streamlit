# Data & Methodology

## Shared market sources

Every section reads the same two accepted market files: **`data/screener_ohlcv.parquet`** for pricing and **`data/constituents.parquet`** for dated membership. The pricing file retains full available history, including equities, adjusted ETFs, benchmark price indices, imported NSE TRI and a labeled cash proxy. The membership file retains historical snapshots as well as current membership.

Equity/ETF vendor observations use Yahoo Finance with adjusted OHLC prices. Imported TRI is sourced from NSE Indices. Synthetic cash history is clearly distinguished from actual ETF prices. Legacy data with incomplete candles or unresolved adjustment conflicts is retained and reported while complete replacements are sought; no missing candle values are invented.

## Date, universe and revisions

For the same date, selected indices and eligibility settings, Momentum Screener and portfolio ranking share the same eligible symbols, prices, scores and ranks. Results identify the **price revision** and **constituent revision**. Changing either source, including a correction on the same date, invalidates cached results.

Choose **As of date** in Momentum Screener and match it to Live Signal. Match minimum history, median volume, maximum stale sessions and the quality filters. The default universe is the five core indices. Other historical index snapshots remain stored without expanding that selection.

Historical runs use the latest membership snapshot effective on or before each date. A current CSV observation with no verified effective date is recorded on its observation date, with an explicit date basis; it is never silently backdated. Verification time describes freshness and is separate from membership effective dates.

## Refresh behavior

Opening a page reads accepted local data. It does not fetch its own vendor prices or create a private delta. The scheduled GitHub Actions publisher runs at **7:45 PM IST**, verifies constituents and updates the two shared files. **Refresh shared market data** runs the same publishers explicitly in the background.

The publisher preserves full history, uses each instrument's coverage and replaces a source atomically only after validation. Partial failures retain accepted observations and record actual coverage. A changed adjusted price requires a complete historical replacement; an incomplete correction cannot create a seam in stored history.

In-process caches follow the calculation date and both source revisions. The legacy replay baselines, benchmark files, JSON constituent snapshots and page-specific caches are migration/recovery inputs rather than active data sources. Operational details are documented in `docs/shared_market_data.md`.

## NSE trading calendar and eligibility

The completed-session cutoff is **7:00 PM IST**. Before that time, the app targets the previous NSE session; after it, the app can target the current session. Weekends and the NSE Capital Market holiday calendar are excluded.

Momentum defaults to 252 rows of history, median daily volume of 100,000 shares, and a three-session maximum price lag. These are visible settings across the Momentum sections. Stale nonholdings are excluded from ranking. Live Signal blocks a stale actual/model holding when it needs a reliable trade price. Stage 2 needs its indicator warm-up; candle strategies explicitly report excluded incomplete OHLCV observations.

Temporary `DUMMY*` placeholders remain in the stored membership record but are excluded from the tradable universe and vendor requests. Portfolio holdings can differ from a fresh top-15 list because entry/hold bands, Stage 2 exits and allocation rules act on portfolio history; the underlying ranking still agrees under matched inputs.

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
