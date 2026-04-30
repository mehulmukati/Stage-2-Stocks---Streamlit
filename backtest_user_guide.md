## Overview

#### What this app does
Runs a momentum backtest on NSE stocks. At each rebalance, every stock in your chosen
universe is scored by its Sharpe ratio, ranked best → worst, and a band rule decides
which stocks enter or exit the portfolio. NAV is tracked daily and compared against
Nifty 50 and Nifty 500 benchmarks.

#### Six strategies, always compared
The backtest simultaneously runs **6 portfolio variants** so you can compare them
side-by-side without re-running:

| Strategy | Band Rule | Weight Method |
|---|---|---|
| Classic · Full | Classic | Equal-weight reset each rebalance |
| Classic · Marginal | Classic | Slot-fill: freed exit weight split equally among entrants |
| Classic · Prop | Classic | Prop-fill: entrants seeded at 1/n; surplus freed weight flows to all survivors |
| Displacement · Full | Displacement | Equal-weight reset each rebalance |
| Displacement · Marginal | Displacement | Slot-fill: freed exit weight split equally among entrants |
| Displacement · Prop | Displacement | Prop-fill: entrants seeded at 1/n; surplus freed weight flows to all survivors |

#### How to use it
1. Set **M**, **N**, frequency, and ranking method in the sidebar.
2. Choose your index universe and date range.
3. Click **▶ Run Backtest**.
4. Read the Performance Summary table — higher CAGR, Sharpe, and Calmar is better.

## Entry & Exit Band (M / N)

#### M — Entry threshold
A stock **enters** the portfolio only if its momentum rank is ≤ M (i.e., it's in the
top-M). Smaller M = more selective, higher bar to entry.

#### N — Worst Rank Held (WRH)
No stock with rank > N may be held in **either** rule — it exits unconditionally.
Must be greater than M.

#### The hysteresis band: M … N
The gap between M and N acts as a **buffer zone**. A stock in ranks M+1 … N cannot
enter (rank > M) and does not automatically exit (rank ≤ N) — so it stays held.
This prevents constant churning of stocks hovering near the entry boundary.

In **Classic**, M+1..N stocks simply sit in the buffer until they either recover into
top-M or fall past N. In **Displacement**, they can additionally be pushed out by a
top-M entrant when the portfolio is already at capacity — see the Classic vs
Displacement tab for details.

**Example with M = 20, N = 30:**
- Rank 1–20 → eligible to enter
- Rank 21–30 → hold if already in, do not enter if not
- Rank 31+ → exit unconditionally (both rules)

> **Tip:** A wider band (e.g., M=20, N=40) lowers turnover but lets underperformers
> linger longer. A narrow band (M=20, N=22) reacts faster but trades more.

## Classic vs Displacement

#### Classic band rule
- **Enter** if rank ≤ M
- **Exit** if rank > N (unconditional)

In Classic, N is a pure exit threshold. Any stock that falls past rank N leaves
immediately at the next rebalance, whether or not a replacement is available.
The portfolio can temporarily hold more than M stocks when multiple stocks enter
in the same rebalance and fewer exit.

#### Displacement band rule — N is the Worst Rank Held (WRH)
N still acts as a hard cap: **no stock with rank > N may be held** — those exit
unconditionally, same as Classic.

The difference is in the **M+1 … N band**. In Displacement, a stock whose rank
has slipped past M but is still ≤ N *stays in the portfolio* — unless a new
top-M stock needs the slot. When the portfolio is at M capacity and a stock ranked
≤ M wants to enter, it displaces the **worst-ranked incumbent** in the M+1..N band.
If there is no such incumbent (everyone is already ≤ M), no displacement happens.

**Three-step rebalance logic:**
1. Exit all stocks with rank > N unconditionally (WRH).
2. Fill newly freed slots with top-M stocks (best rank first).
3. If still at M capacity: each remaining top-M entrant swaps out the worst-ranked
   M+1..N incumbent, one-for-one.

#### The analogy
> **Classic** = "Remove anyone who falls below the cut line, no matter what."
> **Displacement** = "The cut line (N) is still enforced; but between M and N,
>  you only lose your seat when someone ranked higher shows up to claim it."

#### Which to prefer?

| | Classic | Displacement |
|---|---|---|
| N role | Exit threshold | WRH hard cap |
| M+1..N stocks | Hold (buffer zone — no entry, no exit) | Hold, but displaceable by top-M entrant |
| Turnover | Higher | Lower |
| Momentum tilt | Moderate | Stronger (lets winners run) |
| Portfolio size | Can exceed M temporarily | Always ≤ M |

## Full vs Marginal Rebalance

#### Full rebalance
At every rebalance date, **all holdings are reset to equal weight** (1 / portfolio size).

- Simple and deterministic.
- Implicitly mean-reverting: overweight winners are trimmed, underweight laggards are topped up.
- Higher turnover cost (all weights are touched every period, even unchanged holdings).

#### Marginal rebalance — two variants

Only **entering and exiting stocks** trigger explicit weight changes. Incumbents keep
whatever weight they have drifted to since the last rebalance — recent winners carry
more portfolio weight (momentum factor embedded in the weights).

The two variants differ in how they assign weight to **new entrants**:

**Slot-fill (Classic · Marginal / Displacement · Marginal)**

- When exits are present: the freed exit weight is split equally among entrants.
  Incumbents are untouched. A large exit gives the new entrant a large starting weight.
- When there are no exits (entry-only, classic rule): entrants are seeded at
  `1 / portfolio_size` and all weights are normalised to 1 — incumbents are diluted
  proportionally to make room.

**Prop-fill (Classic · Prop / Displacement · Prop)**

- Entrants are **always** seeded at `1 / portfolio_size` regardless of how much weight
  was freed by exits. After normalisation:
  - If freed capital > entrant allocation: surplus flows proportionally to all survivors
    (incumbents gain weight — they absorb the extra freed capital).
  - If freed capital < entrant allocation (or no exits): incumbents are diluted, same as
    the slot-fill no-exit case.
- Net effect: entrants always start at a fair equal-weight position; the size of the exit
  doesn't inflate the entrant's opening stake.

> **Example (slot-fill vs prop-fill with a large exit):**
> Portfolio of 20 stocks; one 8% position exits; one new stock enters.
> - Slot-fill: entrant gets 8% (the full freed weight). Large boost.
> - Prop-fill: entrant gets 1/20 = 5%; remaining 3% freed weight distributed to all 19
>   survivors (each gains ~0.16%).

#### When each works better

| | Full | Marginal (slot-fill) | Marginal (prop-fill) |
|---|---|---|---|
| Style | Mean-reverting | Trend-following | Trend-following |
| Entrant weight | 1/size always | freed ÷ n_entries | 1/size always |
| Turnover | Higher | Lower | Lower |
| Winner concentration | Avoided (reset each time) | Allowed (drift + slot boost) | Allowed (drift, fair entry) |

> **Tip:** In strong trending markets, Displacement + Marginal often wins. In choppy
> markets, Classic + Full may be more robust. Prop-fill is a useful middle ground —
> entrants aren't disadvantaged when the exit that created the vacancy was a large position.

## Ranking & Scoring

#### Sharpe ratio score
Each stock is ranked by its **annualised Sharpe ratio** — mean daily return divided by
daily return standard deviation, multiplied by √252. No risk-free rate is deducted.
Higher Sharpe = more consistent upward momentum relative to its own volatility.

#### Rank by Sharpe options

| Option | What it uses | Best for |
|---|---|---|
| **Average of 3/6/9/12 months** *(default)* | Average across all four lookbacks | Robust, balanced view |
| **Average of 3/6 months** | Short-term only | Recent momentum emphasis |
| **3 months** | Last ~63 days | Very recent, more noise |
| **6 months** | Last ~126 days | Medium-term |
| **9 months** | Last ~189 days | Medium-long |
| **1 year** | Last ~252 days | Longer trend confirmation |

> **Tip:** The default "Average of 3/6/9/12" smooths out single-lookback noise and
> tends to be the most consistent performer across market regimes.

#### Universe filters applied before ranking
A stock is **excluded from ranking** (and therefore can't enter the portfolio) if any
of these fail:

- **Min history days** (default 252): too little price data → excluded.
- **Volume filter**: median daily volume < 100,000 shares → excluded (illiquid stocks).
- **Historical constituents** toggle: stock wasn't in the index on that date → excluded.

## Realism Settings

#### Transaction cost per trade (%)
One-way cost (brokerage + slippage) applied to each stock **traded** at rebalance.
The traded fraction of the portfolio (entries + exits ÷ portfolio size) is multiplied
by this rate and deducted from NAV immediately. Default 0.1%.

Higher frequencies + narrower bands → more trades → cost drag compounds quickly.

#### Min history (trading days)
A stock must have at least this many trading days of price data before it is eligible
for ranking. Default 252 ≈ 1 year. Use higher values (e.g., 504) to exclude newer
listings entirely; lower values (e.g., 126) let younger stocks in sooner.

#### Use historical constituents — anti-survivorship bias

<!-- warning -->

- **ON (recommended):** At each rebalance date, only stocks that were *actually members
  of the chosen index on that date* are eligible. This prevents the backtest from
  retroactively including stocks that joined the index later — a form of look-ahead bias
  called **survivorship bias**.
- **OFF:** All stocks in the data file are eligible at all times. Backtested returns will
  look significantly better, but the result is misleading — you would have been unable
  to know which stocks to buy at the time.

#### Rebalance frequency

| Frequency | Reaction speed | Annual trades | Cost sensitivity |
|---|---|---|---|
| Weekly | Fast | High | High |
| Biweekly | Moderate | Medium | Medium |
| Monthly | Slow | Low | Low |
| Quarterly | Very slow | Very low | Very low |
| Half-yearly | Minimal | Minimal | Minimal |

Monthly rebalancing is usually the best trade-off between responsiveness and cost
unless the strategy has very fast momentum signals. Quarterly and half-yearly are
suited to low-turnover strategies or tax-conscious portfolios where fewer rebalances
reduce realised gains.

## Reading the Results

#### Portfolio NAV chart
All six strategies plus both benchmarks start at 100 and compound daily. A final NAV of
350 means 250% total return over the period. Hover for unified tooltips across all series.

#### Rolling CAGR chart
Shows the annualised return an investor would have earned over a rolling window ending at
each date. Persistent positivity indicates consistent compounding; dips below 0 show
periods where the strategy was underwater on that rolling horizon.

#### Portfolio Churn per Rebalance
Displayed between the Rolling CAGR chart and the Performance Summary. Two subplots (one
per band rule) each show:
- **Bars (left axis):** stocks entering above the zero line (positive), stocks exiting
  below (negative) — immediate visual of how active each rebalance was
- **Lines (right axis):** Full, Marginal (slot-fill), and Prop (prop-fill) turnover % for that event

High-churn spikes correlate directly with cost drag. Wide M/N bands reduce bar heights.

#### Performance Summary table
| Metric | What it means |
|---|---|
| **CAGR (%)** | Compound Annual Growth Rate over the full period |
| **Sharpe** | Annualised risk-adjusted return (no risk-free rate deducted) |
| **Max Drawdown (%)** | Largest peak-to-trough decline |
| **Calmar** | CAGR ÷ Max Drawdown — reward per unit of drawdown risk |
| **Sortino** | Like Sharpe but only penalises downside volatility |
| **Avg Turnover (%)** | Mean fraction of the portfolio traded per rebalance |
| **Cost Drag (%)** | Cumulative NAV drag from transaction costs over the full period |

#### Download Full Rebalance Log
The **📥 Download Full Rebalance Log** button exports a CSV with every rebalance event.
Each row covers one rebalance date for one band rule and includes:
- Entry and exit ticker lists
- `Holdings (Full Weights %)`, `Holdings (Marg Weights %)`, and `Holdings (Prop Weights %)` —
  exact allocation for every stock in `TICKER:X.XXXX%` format
- Per-rebalance turnover % for all three variants
- Valid universe size at that date

Useful for auditing specific rebalance decisions or analysing weight drift over time.

## Data Files

The backtester is fully self-contained — no internet connection is required after the
initial seed. Committed Parquet files provide the historical baseline; a gitignored
local delta cache accumulates yfinance tail rows so restarts skip re-fetching known dates.

| File | Committed | Contents |
|---|---|---|
| `data/backtest_history.parquet` | ✅ Yes | Long-form `{symbol, date, Close, High, Volume}` for ~750 NSE symbols, ~10 years |
| `data/benchmarks.parquet` | ✅ Yes | Nifty 50 & Nifty 500 daily close history |
| `data/compositions.parquet` | ✅ Yes | Historical index constituent snapshots for anti-survivorship-bias filtering |
| `data/backtest_delta.parquet` | ❌ Gitignored | Local delta cache — yfinance tail rows accumulated across restarts |
| `data/benchmarks_delta.parquet` | ❌ Gitignored | Same for benchmark data |

**Data load order at startup:**
1. Committed baseline parquet
2. Local delta cache (if present) — extends baseline without re-fetching
3. yfinance — only dates not yet covered by either parquet
4. New rows saved back to the delta cache for future restarts

The delta cache is transparent — if missing or corrupted, the app falls back to
fetching from yfinance as before. After a baseline rebuild
(`scripts/refresh_backtest_parquet.py`) the delta becomes redundant; overlapping
rows are silently deduped on the next load.

#### Seeding or rebuilding the backtest baseline

Run once after cloning, or to force a full refresh:

```bash
python scripts/refresh_backtest_parquet.py
```

This downloads ~10 years of OHLCV for all symbols in `constituents.json` plus both
benchmark indices and writes the three Parquet files above. A full rebuild takes
several minutes; subsequent startups are fast.
