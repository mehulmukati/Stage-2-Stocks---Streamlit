## Overview

#### What this app does
Runs a momentum backtest on NSE stocks. At each rebalance, every stock in your chosen
universe is scored by its Sharpe ratio, ranked best → worst, and a band rule decides
which stocks enter or exit the portfolio. NAV is tracked daily and compared against
Nifty 50 and Nifty 500 benchmarks.

#### Four strategies, always compared
The backtest simultaneously runs **4 portfolio variants** so you can compare them
side-by-side without re-running:

| Strategy | Band Rule | Weight Method |
|---|---|---|
| Classic · Full | Classic | Equal-weight reset each rebalance |
| Classic · Marginal | Classic | Incumbents keep price-drifted weight |
| Displacement · Full | Displacement | Equal-weight reset each rebalance |
| Displacement · Marginal | Displacement | Incumbents keep price-drifted weight |

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

#### Marginal rebalance
Only **entering and exiting stocks** trigger weight changes. Incumbents keep whatever
weight they have drifted to since the last rebalance.

- Price-drifted weights: a stock up 30% since the last rebalance carries 30% more weight.
- Lower turnover (unchanged incumbents are not touched).
- Implicit momentum factor in the weights — recent winners carry more portfolio weight.

**Weight assignment for entrants depends on whether any stocks exit that period:**

*When exits are present:* the combined weight freed by exiting stocks is split equally
among entrants. Incumbents are not touched beyond their natural price drift.

*When there are only entries (no exits):* there is no freed weight to redistribute.
Each entrant is seeded at `1 / portfolio_size` (equal-weight share of the new total),
and all weights are then normalised to 1. The practical effect is that incumbents are
**diluted proportionally** to make room for entrants. With *k* entrants joining a
portfolio of *size* stocks, entrants each receive `1 / (size + k)` and every incumbent
weight is scaled by `size / (size + k)`.

> **Example:** 5 incumbents (equal 20% each), 2 entrants, no exits → portfolio size = 7.
> After normalisation: each incumbent = 15.6%, each entrant = 11.1%.
> Incumbents yield ~22% of the portfolio collectively to the two new arrivals.

#### When each works better

| | Full | Marginal |
|---|---|---|
| Style | Mean-reverting | Trend-following |
| Turnover | Higher | Lower |
| Transaction cost | Higher | Lower |
| Winner concentration | Avoided (reset each time) | Allowed (drift compounds) |

> **Tip:** In strong trending markets, Displacement + Marginal often wins. In choppy
> markets, Classic + Full may be more robust.

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

Monthly rebalancing is usually the best trade-off between responsiveness and cost
unless the strategy has very fast momentum signals.
