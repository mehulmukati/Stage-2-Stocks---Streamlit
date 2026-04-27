# Backtest

The Backtest simulates a systematic momentum portfolio over a chosen historical period. It tracks two portfolio variants simultaneously and compares them against the Nifty 50 and Nifty 500 benchmarks.

---

## How the strategy works

### Entry and exit — the band rule

The strategy maintains a portfolio of stocks ranked by Sharpe momentum:

- A stock **enters** the portfolio if it ranks in the **top M** of the universe on a rebalance date.
- A stock **exits** only if it falls outside the **top N** (where N > M).

The gap between M and N creates a **buffer band** that reduces unnecessary turnover — a stock that was in the top 20 (M=20) but slips to 22 does not immediately exit if N=30. This is important for reducing transaction costs on volatile rankings.

### Rebalancing

Ranking is performed on the **previous trading day's close** (T-1), and trades execute at the **current day's close** (T). This ensures the strategy cannot use information it would not have had at the time of decision.

Available frequencies:

| Frequency | Description |
|---|---|
| **Monthly** | Rebalances on the last trading day of each calendar month |
| **Biweekly** | Rebalances on the last trading day of every other calendar week |
| **Weekly** | Rebalances on the last trading day of each calendar week |
| **Quarterly** | Rebalances on the last trading day of each calendar quarter |
| **Half-yearly** | Rebalances on the last trading day of each half-year (Jan–Jun, Jul–Dec) |

### Two portfolio variants

| Variant | Weight rule |
|---|---|
| **Full Rebalance** | On every rebalance, all holdings are reset to equal weight (1/M) |
| **Marginal Rebalance** | Only the weight freed by exiting stocks is redistributed to entrants; incumbents keep their price-drifted weights |

The full rebalance is simpler and easier to execute in practice. The marginal rebalance approximates how a real portfolio might drift between rebalances.

---

## Parameters

### Portfolio size (M and N)

| Parameter | Description | Typical range |
|---|---|---|
| **M** | Number of stocks to hold (entry threshold) | 10–30 |
| **N** | Exit threshold — stock leaves if ranked > N | M + 5 to M + 20 |

Smaller M concentrates the portfolio in fewer, higher-conviction names (higher volatility, potentially higher return). Larger M diversifies more broadly.

### Rank by Sharpe
Same sort methods as the Momentum Screener. "Average of 3/6/9/12 months" is recommended for balanced ranking.

### Universe
Select which indices to draw from. Only stocks that were constituents of the selected indices **at the time of each rebalance** are eligible (see Survivorship Bias below).

### Min history (trading days)
A stock must have at least this many days of history before it can be ranked. Default is 252 (1 year). Raising this to 500–750 days excludes recently-listed stocks with thin history — more conservative but reduces the available universe, especially in early years.

### Transaction cost per trade (%)
One-way cost applied to each stock that enters or exits the portfolio at rebalance. Default is 0.1% (10 basis points). This covers brokerage and slippage. The cost drag accumulates over the full simulation and is reported in the summary.

---

## Survivorship bias mitigation

When **"Use historical constituents"** is enabled, the eligible universe on each rebalance date is restricted to stocks that were **actually in the selected index at that point in time**, using historical composition data from `compositions.parquet`.

This prevents the backtest from picking stocks that:
- Were added to the index *after* the rebalance date (hindsight selection)
- Were delisted or merged before the present day (survivorship bias)

If composition data is unavailable for a given date, the filter falls back to the current constituents — which slightly overstates quality for older periods.

---

## Understanding the results

### NAV chart (base = 100)
All series start at 100 and compound from there. A final NAV of 350 means the portfolio returned 250% over the period.

### Performance summary table

| Metric | Definition |
|---|---|
| **CAGR (%)** | Compound Annual Growth Rate — annualised total return |
| **Sharpe** | Annualised Sharpe ratio of the portfolio's daily NAV returns |
| **Max Drawdown (%)** | Largest peak-to-trough decline over the full period |
| **Final NAV** | Ending value on a base-100 investment |

### Rolling returns chart
Shows the rolling annualised CAGR (%) at each point in time — what a buy-and-hold investor over the selected window would have earned, annualised. Useful for seeing consistency: a portfolio with persistently positive rolling CAGR (rarely dipping below zero) indicates smoother compounding.

### Avg Turnover / Rebalance
The average fraction of the portfolio traded at each rebalance (exits + entries as a share of holdings). Lower turnover = lower cost drag. The band rule (M vs N gap) is the primary lever for reducing this.

### Total Cost Drag (%)
Cumulative cost deducted from the NAV over the full simulation. This compounds — a 0.1% cost per trade over 10 years of monthly rebalancing can reduce terminal wealth meaningfully.

---

## Known limitations

- **No slippage model for large orders**: the cost parameter is applied uniformly. In practice, larger positions in illiquid stocks incur higher impact costs.
- **Daily OHLCV only**: the strategy assumes execution at the closing price on rebalance day. Intraday execution at open or VWAP is not modelled.
- **Volume filter uses median volume**: stocks near the liquidity threshold may pass the filter in some periods and fail in others, creating inconsistent inclusion.
- **Historical compositions coverage**: composition data may be incomplete for older dates or less common indices, partially limiting the survivorship-bias mitigation.
