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

**Two-step rebalance logic:**
1. Exit all stocks with rank > N unconditionally (WRH).
2. Fill newly freed slots with top-M stocks (best rank first), up to the hard cap of M.
   Stocks in the M+1..N buffer zone are never actively displaced — they only lose their
   seat when a WRH exit (or a Stage 2 drop exit, if that signal is enabled) frees a slot.

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

#### Band rule selector (display only)
The sidebar shows a **Band rule** dropdown that always reads "Classic / Displacement". It is **disabled** — both rules are always evaluated simultaneously in every backtest run and presented side-by-side. No selection is needed.

#### Universe filters applied before ranking
A stock is **excluded from ranking** (and therefore can't enter the portfolio) if any
of these fail:

- **Min history days** (default 252): too little price data → excluded.
- **Volume filter**: median daily volume < 100,000 shares → excluded (illiquid stocks).
- **Historical constituents** toggle: stock wasn't in the index on that date → excluded.
- **Quality pre-filters** (see Quality Filters section below): any of the 8 optional eligibility gates → excluded.

## Realism Settings

#### Max position size (%)
Maximum weight any single stock may hold **after** each rebalance. When a stock's
assigned weight would exceed this cap, the excess is redistributed proportionally
to all smaller positions (iteratively, if redistribution would push another stock
over the cap). Default 0 = no cap.

This guard is most important for **Marginal** and **Prop** variants:
- In those variants, incumbents drift with prices between rebalances.
- A mass-exit event (many stocks exiting simultaneously) can funnel their combined
  freed weight into just one or two new entrants.
- With M=20, a single stock can reach 50%+ of the portfolio in a single rebalance
  if several large positions exit at once.

**Recommended setting: 15%** for Marginal or Prop variants. For Full Rebalance,
equal-weight reset prevents concentration anyway, so the cap has no practical effect.

#### Transaction cost per trade (%)
One-way cost (brokerage + slippage) applied to each stock **traded** at rebalance.
The traded fraction of the portfolio (entries + exits ÷ portfolio size) is multiplied
by this rate and deducted from NAV immediately. Default 0.1%.

Higher frequencies + narrower bands → more trades → cost drag compounds quickly.

#### Start date and the warmup window

The **Start date** is when the data window opens, not when the first trade executes.
Before any stock can be ranked it must accumulate at least **Min history** trading days
of price data — 252 by default (≈ 1 year). This creates a **warmup period**:

> **First portfolio formed ≈ Start date + Min history days**

For example, with Start date = 1 Jan 2021 and Min history = 252, the first rebalance
where stocks are ranked and a portfolio is built occurs around **Jan 2022**. NAV
tracking begins from that first rebalance.

**Practical rule:** set Start date *earlier* than the period you want to measure by at
least the Min history window. To study performance from Jan 2022, set Start date to
Jan 2021 with the default 252-day min history.

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

#### Brokerage per sale (Rs)
Flat charge deducted on each **exit** (sells only). Models fixed-cost brokerages such as Rs 15–20 per order. Needs the "Initial capital" field to convert the flat amount to a proportion of NAV. Default 0 = no flat charge.

#### Tax — LTCG / STCG
Available inside the "Realism" expander. Models Indian capital gains tax on each realised exit using FIFO lot accounting:

| Setting | Default | Trigger |
|---|---|---|
| **STCG rate** | 20% | Holdings sold within 12 months of purchase |
| **LTCG rate** | 12.5% | Holdings sold after 12 months of purchase |

Tax is computed per financial year and deducted from NAV as each exit is processed. Set both to 0% to run pre-tax. The impact is most visible with monthly or weekly rebalancing (many short-term lots) — quarterly or half-yearly rebalancing shifts most exits into LTCG territory.

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

## Quality Filters

Eight optional eligibility gates — identical to the Momentum Screener's quality pre-filters — applied **before ranking** at each rebalance. A stock that fails any enabled filter is excluded from the ranking step entirely and cannot enter the portfolio on that date.

All filters default to **off** (or permissive thresholds) so existing backtests produce identical results unless the filters are explicitly tightened.

| Filter | Default (backtest) | Screener default | Notes |
|---|---|---|---|
| **Min Annual Return (%)** | 0 (disabled) | 7% | Requires 1-year price change ≥ threshold |
| **Within % of 52w High** | 100 (disabled) | 25% | Excludes stocks more than N% below their 52-week high |
| **Max Circuits (1yr)** | 999 (disabled) | 18 | Excludes stocks with more than N upper/lower circuit-breaker hits |
| **Close > 100 DMA** | Off | Off | Only stocks trading above their 100-day moving average |
| **Close > 200 DMA** | Off | On | Only stocks trading above their 200-day moving average |
| **Pos Days 3M (%)** | 0 (disabled) | 45% | Min fraction of positive-return days in the last 3 months |
| **Pos Days 6M (%)** | 0 (disabled) | 45% | Min fraction of positive-return days in the last 6 months |
| **Pos Days 12M (%)** | 0 (disabled) | 45% | Min fraction of positive-return days in the last 12 months |

> **Tip:** To replicate the Momentum Screener's filter set in the backtest, set all values to their screener defaults (7%, 25, 18, unchecked/checked DMA, 45/45/45). Stocks that pass the screener on any given day will then be the same pool that the backtest considers eligible on that rebalance date.

When a stock is excluded by a quality filter, the Debug tab shows the exclusion reason as **quality_filter**.

## Weekly Stage 2 Signals

Available only when **Rebalance frequency = Weekly**. Both signals apply to Classic and Displacement band rules.

#### Entry filter — Enter on Stage 2 score jump
Allows a stock outside the top-M to enter the portfolio if its Weinstein Stage 2 score
(0–8) rises by **≥ threshold** points since the previous week's rebalance.

- **Classic:** the stock enters alongside normal top-M entrants (portfolio may briefly
  exceed M if many signals fire simultaneously).
- **Displacement:** Stage 2 jump candidates join the slot-fill candidate pool alongside
  top-M entrants, ordered by momentum rank. The hard cap of M is always preserved — a
  Stage 2 jumper only enters if a slot was freed by a WRH exit or Stage 2 drop exit in
  the same week.

Entry reason in the rebalance log: `S2 +N` (e.g. `S2 +3` means score rose 3 points).

#### Exit signal — Exit on Stage 2 score drop
Forces a held stock to exit if its Stage 2 score falls by **≥ threshold** points since
the previous rebalance, regardless of its momentum rank. Useful for cutting structurally
deteriorating positions before the momentum rank catches up.

In **Displacement** mode, slots freed by Stage 2 drop exits are filled in the same
rebalance — capital is not left idle until the following week.

Exit reason in the rebalance log: `S2 -N` (e.g. `S2 -2` means score fell 2 points).

#### Score jump / drop threshold
Both signals share the same configurable threshold (1–4 points). The default of **2**
means a single-point drift won't trigger; only a meaningful multi-point shift will.

> **Tip:** Stage 2 signals add a structural/trend filter on top of the momentum rank.
> They are most useful when momentum alone is generating too many false entries (stocks
> with high Sharpe but deteriorating MA structure). Combining a Stage 2 drop exit with
> Displacement mode is particularly effective: a structural breakdown exits the position
> early, and the freed slot is immediately refilled from the top-M candidate pool.

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

## 🔍 Debug Tab — Portfolio Debugger

After running a backtest, switch to the **Debug tab** to ask "Why wasn't stock X in the portfolio on date Y?"

#### How to use it
1. Select the band rule (Classic or Displacement).
2. Pick a rebalance date from the dropdown (most-recent first).
3. Type a stock ticker (e.g. `RELIANCE`).

#### What you'll see

| Outcome | Badge | Meaning |
|---|---|---|
| Stock was in the portfolio | ✅ Held | Shows rank, weight in all three variants (Full / Marginal / Prop) |
| Stock passed filters but wasn't held | 🟡 Ranked but not held | Shows exact rank vs M/N band — buffer zone or above N |
| Stock never reached the ranking step | 🔴 Excluded before ranking | Failed history, volume, or index-composition filter; shows universe size |

A **top-10 ranked stocks** table is always shown for context — useful for seeing where the queried stock sits relative to the top of the universe.

#### Common reasons a stock shows 🔴 Excluded
- **Insufficient history**: fewer trading days than the Min History setting on that date
- **Low volume**: median daily volume below 100,000 shares on that date
- **Not an index constituent**: historical composition filter excluded it (if enabled)
- **No price data**: stock had no OHLCV rows in the backtest parquet for that period
- **quality_filter**: one or more Quality Filter gates were enabled and the stock failed at least one (min return, 52w high proximity, circuit count, DMA crossover, or positive-day ratio)

---

## 📐 Walk-Forward Tab — Overfit Detection

The **Walk-Forward tab** splits the backtest into two windows — an in-sample (calibration) period and an out-of-sample (forward) period — so you can see whether a strategy's good numbers come from the tuning window or from genuinely unseen data.

#### How to use it
1. Use the **split date picker** (defaults to the midpoint of your backtest range).
2. Read the two side-by-side stats tables — in-sample on the left, out-of-sample on the right.
3. Look at the NAV chart with the vertical split line.

#### Interpreting results

| Signal | What it suggests |
|---|---|
| OOS CAGR close to IS CAGR | Parameters generalise well — no obvious overfit |
| OOS CAGR materially lower than IS | Parameters may be overfit to the calibration window |
| OOS Sharpe > IS Sharpe | Strategy may have gotten better (regime change working in its favour) |
| OOS Max Drawdown >> IS | More tail risk in the forward period — consider wider M/N band |

> **Tip:** Move the split date earlier (e.g., 30% in-sample, 70% out-of-sample) for a more conservative OOS test. A strategy that holds up over a 7-year OOS window with parameters tuned on only 3 years is much more credible than the reverse.

---

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
