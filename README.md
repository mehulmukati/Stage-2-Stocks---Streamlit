# NSE Stage 2 Screener & Momentum Backtester

A pair of Streamlit apps for systematic stock analysis on the NSE (National Stock Exchange of India). The **screener** applies Stan Weinstein's Stage 2 methodology and momentum ranking across ~750 stocks in real time; the **backtester** simulates momentum portfolio strategies on 10+ years of historical data with realistic costs and survivorship-bias controls.

---

## Features

| [Screener](app.py) — `app.py` | [Backtester](app_backtest.py) — `app_backtest.py` |
|---|---|
| Stage 2 Breakout screener (Weinstein 8-point score) | 4 simultaneous portfolio strategies |
| Momentum screener (Sharpe ratio ranking) | Entry/exit band parameters (M / N) |
| Phase Chart — rolling Stage 2 score for any ticker | Weekly / biweekly / monthly / quarterly / half-yearly rebalance |
| Fuzzy ticker search (typo-tolerant) | Anti-survivorship-bias via historical constituents |
| CSV export | Transaction-cost drag modelling |
| Live auto-refresh during background data sync | NAV chart, rolling CAGR, and drawdown metrics |

---

## Architecture

```
app.py                  ← Screener  (Parquet-backed, no external DB required)
app_backtest.py         ← Backtester (Parquet-backed, no external DB required)

Shared modules:
  stage2_engine.py      Weinstein 8-point scoring, RSI, consolidation detection
  momentum_engine.py    Sharpe ratio computation across multiple lookback periods
  backtest_engine.py    Portfolio rebalancing logic, NAV tracking
  charts.py             Plotly chart builders
  data.py               Screener data layer (Parquet + yfinance delta)
  data_backtest.py      Backtester data layer (Parquet + yfinance delta)
  config.py             Shared constants
```

Both apps are fully self-contained — no external database or credentials required.

---

## Prerequisites

- Python 3.9 or later
- pip

---

## Installation

```bash
git clone https://github.com/<your-username>/Stage-2-Stocks---Streamlit.git
cd Stage-2-Stocks---Streamlit
pip install -r requirements.txt
```

---

## Data Setup

Both apps use local Parquet files under `data/` — no external database or credentials needed.

### Seed the screener baseline (first run only)

```bash
python scripts/refresh_screener_parquet.py
```

This downloads ~2 years of OHLCV for all ~750 NSE symbols and writes `data/screener_ohlcv.parquet` (~10 MB). Run it once after cloning. After that the app performs incremental delta fetches at startup automatically.

To force a full rebuild later:

```bash
python scripts/refresh_screener_parquet.py --full
```

The backtester baseline (`data/backtest_history.parquet`) is committed to the repo — no seed step required.

---

## Running the Apps

### Screener

```bash
streamlit run app.py
```

Opens at `http://localhost:8501`. Use the sidebar to select the universe (Nifty 50 / 100 / 250 / 500 / Smallcap 250) and switch between the Stage 2 Screener, Momentum Screener, and Phase Chart views.

### Backtester

```bash
streamlit run app_backtest.py
```

Opens at `http://localhost:8501` (or `8502` if the screener is already running). Configure M/N thresholds, rebalance frequency, date range, and universe in the sidebar, then click **Run Backtest**.

---

## App Details

### Screener (`app.py`)

**Stage 2 Breakout Screener** scores each stock 0–8 on Weinstein criteria (30-week MA slope, price vs MA, volume trend, RS, consolidation, RSI). Results are colour-coded by stage strength and sortable; CSV export is available.

**Momentum Screener** ranks the universe by annualised Sharpe ratio averaged across 3 / 6 / 9 / 12-month lookback periods. Filters include minimum return, proximity to 52-week high, circuit-breaker frequency, and DMA crossover status.

**Phase Chart** plots a stock's daily rolling Stage 2 score as a colour-coded background band over its full price history. Supports log / linear Y-axis and fuzzy ticker lookup.

Data flows: `data/screener_ohlcv.parquet` → score cache (`data/stage2_cache.parquet` / `data/momentum_cache.parquet`) → in-memory cache → yfinance delta fetch.

### Backtester (`app_backtest.py`)

Runs four portfolio variants in a single pass:

| Variant | Band rule | Weight method |
|---|---|---|
| Classic · Full | Standard entry (≤ M) / exit (> N) | Equal-weight reset each rebalance |
| Classic · Marginal | Standard bands | Incumbents keep price-drifted weights |
| Displacement · Full | Incumbents in M+1…N band displaceable | Equal-weight reset |
| Displacement · Marginal | Displacement rule | Price-drifted weights |

Outputs: NAV equity curve vs Nifty 50 & Nifty 500 benchmarks, rolling CAGR chart, performance table (CAGR, Sharpe, Calmar, Sortino, max drawdown, turnover, cost drag), and per-strategy rebalance logs.

Data flows: `data/backtest_history.parquet` (10-year baseline) → yfinance delta (last parquet date → today) → in-memory cache.

---

## Data Sources

| Source | Used by | What it provides |
|---|---|---|
| `data/screener_ohlcv.parquet` | Screener | ~2 years of EOD OHLCV for all ~750 NSE symbols |
| `data/stage2_cache.parquet` | Screener | Most-recent Stage 2 scores (avoids re-scoring on page load) |
| `data/momentum_cache.parquet` | Screener | Most-recent Momentum scores |
| `data/backtest_history.parquet` | Backtester | 10-year bundled OHLCV baseline |
| `data/benchmarks.parquet` | Backtester | Nifty 50 & Nifty 500 benchmark history |
| Yahoo Finance (yfinance) | Both (delta) | Incremental OHLCV updates since last parquet date |
| `constituents.json` | Both | Index membership (Nifty 50 / 100 / 250 / 500 / Smallcap 250) |
| `nse_holidays.json` | Screener | NSE trading calendar |
