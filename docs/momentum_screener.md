# Momentum Screener

The Momentum Screener ranks the universe by **risk-adjusted return** (annualised Sharpe ratio) across multiple time horizons, and applies a set of quality filters to surface stocks with consistent, smooth uptrends.

---

## Ranking metric — Sharpe ratio

The screener uses the **standard annualised Sharpe ratio** (no risk-free rate deduction):

```
Sharpe = mean(daily_returns) / std(daily_returns) × √252
```

A higher Sharpe indicates the stock has delivered more return per unit of daily volatility. This favours stocks with **smooth, consistent advances** over stocks that spike and crash.

### Available time periods

| Label | Trading days | Approximate period |
|---|---|---|
| 3M | 63 | 3 months |
| 6M | 126 | 6 months |
| 9M | 189 | 9 months |
| 1Y | 252 | 1 year |

### Sort methods

| Method | How it works |
|---|---|
| **Average of 3/6/9/12 months** | Mean of all four Sharpe values (recommended — balanced across horizons) |
| **Average of 3/6 months** | Mean of short-term Sharpes only — favours recent momentum |
| **1 year / 9 months / 6 months / 3 months** | Single-period rank |

> **Recommendation**: "Average of 3/6/9/12 months" is the most robust — it avoids over-fitting to any single horizon and naturally rewards stocks with persistent momentum across timeframes.

---

## Filters

### Min Annual Return (%)
Excludes stocks whose 1-year price change is below this threshold. Default is 7% (roughly inflation-level minimum). Raising this focuses the list on strong performers but may exclude quality stocks early in their move.

### Within % of 52-week High
Keeps only stocks trading within N% of their 52-week high. For example, 25% means the stock is within 25% of its 52-week high (i.e. `(52w_high - close) / 52w_high ≤ 0.25`). Lower values focus on stocks near new highs — the core of momentum investing.

### Max Circuits (1yr)
Circuit breakers are days when the stock moved ±5%, ±10%, or ±20% exactly at a circuit limit. A high count suggests erratic, event-driven price action rather than smooth momentum. The count covers the **last 252 trading days** (1 year).

### Close above 100/200 DMA
Checkboxes to require the close to be above the 100-day and/or 200-day moving average. Useful for ensuring the stock is in a longer-term uptrend.

---

## Metrics in the results table

| Column | Description |
|---|---|
| **Close** | Last closing price |
| **Sharpe** | Averaged annualised Sharpe across the selected time periods |
| **Volatility (%)** | Annualised standard deviation of daily returns over full history |
| **52w High** | 52-week high (rolling 252-day high of the High column) |
| **Median Vol** | 252-day median daily volume |
| **1Y Change** | Price return over the last 252 trading days |
| **% from 52wH** | How far below the 52-week high the stock currently trades |
| **Circuit Close** | Number of circuit-limit closes in the last 252 days |

> DMA100, DMA200, individual period Sharpes, and positive-day counts are used for filtering but are not shown in the table.

---

## How to use

1. Select indices in the sidebar, set your filters, and click **Run**.
2. Results are pre-sorted by Sharpe (descending). Re-sort other columns as needed.
3. Cross-reference with the Stage 2 Screener — stocks that rank highly on both momentum and Weinstein criteria are the strongest candidates.
4. Switch to the **Phase Chart** tab and type a symbol to review its full price history.

> **Tip**: Combining a high Sharpe rank with a Stage 2 score ≥ 5 is a powerful filter. The Sharpe rank ensures smooth upward movement; the Stage 2 score confirms the structural MA alignment that sustains it.
