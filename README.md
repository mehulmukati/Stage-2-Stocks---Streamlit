# NSE Stage 2 Screener & Momentum Backtester

A pair of Streamlit apps for systematic stock analysis on the NSE (National Stock Exchange of India). The **screener** applies Stan Weinstein's Stage 2 methodology and momentum ranking across ~750 stocks in real time; the **backtester** simulates momentum portfolio strategies on 10+ years of historical data with realistic costs and survivorship-bias controls.

---

## Features

| [Screener](app.py) — `app.py` | [Backtester](app_backtest.py) — `app_backtest.py` |
|---|---|
| Stage 2 Breakout screener (Weinstein 8-point score) | 4 simultaneous portfolio strategies |
| Momentum screener (Sharpe ratio ranking) | Entry/exit band parameters (M / N) |
| Phase Chart — rolling Stage 2 score for any ticker | Weekly / biweekly / monthly rebalance |
| Fuzzy ticker search (typo-tolerant) | Anti-survivorship-bias via historical constituents |
| CSV export | Transaction-cost drag modelling |
| Live auto-refresh during background data sync | NAV chart, rolling CAGR, and drawdown metrics |

---

## Architecture

```
app.py                  ← Screener  (DB-backed, real-time)
app_backtest.py         ← Backtester (Parquet-backed, no DB required)

Shared modules:
  stage2_engine.py      Weinstein 8-point scoring, RSI, consolidation detection
  momentum_engine.py    Sharpe ratio computation across multiple lookback periods
  backtest_engine.py    Portfolio rebalancing logic, NAV tracking
  charts.py             Plotly chart builders
  data.py               Screener data layer (PostgreSQL → yfinance fallback)
  data_backtest.py      Backtester data layer (Parquet + yfinance delta)
  config.py             Shared constants
```

The two apps are fully independent — the backtester runs without a database connection.

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

## Environment Setup

The screener reads live prices from a PostgreSQL database (Supabase / Neon). Create a `.env` file in the project root:

```
DATABASE_URL=postgresql://user:password@host:port/dbname
```

The backtester does **not** require a database — it uses bundled Parquet files under `data/` and fetches only the recent delta from Yahoo Finance.

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

Data flows: PostgreSQL (primary, updated once per trading day after 7 pm IST) → in-memory cache → yfinance fallback.

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
| PostgreSQL (Supabase / Neon) | Screener | Cached EOD OHLCV + derived metrics, ~10 years |
| Yahoo Finance (yfinance) | Screener (fallback) · Backtester (delta) | Live and historical OHLCV |
| `data/backtest_history.parquet` | Backtester | 10-year bundled OHLCV baseline |
| `data/benchmarks.parquet` | Backtester | Nifty 50 & Nifty 500 benchmark history |
| `constituents.json` | Both | Index membership (Nifty 50 / 100 / 250 / 500 / Smallcap 250) |
| `nse_holidays.json` | Screener | NSE trading calendar |
