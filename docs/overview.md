# Overview

This app is a **Weinstein Stage 2 stock screener and momentum backtester** for the NSE (National Stock Exchange of India) universe — covering the Nifty 50, Nifty Next 50, Nifty Midcap 150, and Nifty Smallcap 250 indices (~750 stocks in total).

It implements Stan Weinstein's stage analysis methodology from *Secrets for Profiting in Bull and Bear Markets*, augmented with quantitative momentum ranking and a full portfolio backtester.

---

## Four tools in one

| Tool | What it does |
|---|---|
| **Stage 2 Screener** | Scores each stock 0–8 on Weinstein criteria and surfaces those in a Stage 2 uptrend |
| **Momentum Screener** | Ranks the universe by risk-adjusted return (Sharpe ratio) across multiple time periods |
| **Phase Chart** | Plots rolling Stage 2 score and MA bands for any single stock over its full history |
| **Backtest** | Simulates a momentum portfolio (top-M entry, top-N exit band) with realistic costs and survivorship-bias mitigations |

---

## Weinstein Stage Analysis — quick primer

Stan Weinstein divided a stock's price cycle into four stages:

- **Stage 1 — Basing**: price moves sideways after a decline; the 30-week MA flattens
- **Stage 2 — Advancing**: price breaks out above the 30-week MA on expanding volume; MAs fan upward
- **Stage 3 — Topping**: price stalls near highs; the MA flattens again
- **Stage 4 — Declining**: price breaks below the MA and trends down

The ideal buy point is the **Stage 2 breakout** — when the stock clears a consolidation on strong volume and begins a sustained advance. This app automates the detection of that condition across the entire NSE 750 universe.

---

## Data source and freshness

- Prices are sourced from **Yahoo Finance** via yfinance and stored in a **PostgreSQL database** (Neon).
- Data is synced incrementally once per session, with a 3-tier cache (memory → database → internet).
- The database covers approximately **10 years** of daily OHLCV history.
- The screener cache key is the last valid NSE trading date (weekday, non-holiday). Results update once per trading day after 7 pm IST.

---

## Navigation

Use the **Screener selector** in the sidebar to switch between tools. Each tool has its own filters and controls that appear below the selector.
