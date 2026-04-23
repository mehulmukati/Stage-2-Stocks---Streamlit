# Stage 2 Screener

The Stage 2 Screener scores every stock in the selected indices on **eight Weinstein criteria**, classifies each into a phase, and surfaces candidates in or near a Stage 2 uptrend.

---

## The 8-point scoring system

Each criterion contributes **1 point** (0 or 1). The maximum score is 8.

| # | Criterion | Condition | What it measures |
|---|---|---|---|
| 1 | **Volume spike** | Today's volume ≥ 2× the 10-day average volume | Institutional participation — breakouts on thin volume rarely sustain |
| 2 | **Higher high** | Close ≥ the 50-day closing high from the prior bar | Price is at or above recent resistance — bullish breakout momentum |
| 3 | **Higher low** | 20-day low (yesterday) > 50-day low (50 bars ago) | Recent pullbacks are shallower than older ones — uptrend structure is intact |
| 4 | **Price above rising MA50** | Close > MA50 **and** MA50 is higher than it was 50 bars ago | Short-term trend is up and the MA itself is rising |
| 5 | **Price above rising MA200** | Close > MA200 **and** MA200 is higher than it was 50 bars ago | Long-term trend is up and the base is solid |
| 6 | **Price above MA150** | Close > MA150 | Intermediate trend confirmation |
| 7 | **MA stack** | MA50 > MA150 > MA200 | The three MAs are fanned out in bull-market order |
| 8 | **Consolidation base** | 20-day close range < 15% of the period low | Stock has been basing/consolidating — distinguishes genuine base breakouts from momentum continuation moves |

> **Note on the volume criterion**: the 10-day rolling average is deliberately short. On most ordinary trading days stocks will *not* fire this criterion (volume is ≈ average), so a score of 6/8 without the volume spike is still a solid Stage 2 signal. An 8/8 score indicates a base breakout on heavy volume — the highest-quality Weinstein setup.

> **Note on the consolidation criterion**: a stock trending steadily upward without pausing will *not* earn this point (its 20-day range exceeds 15%). This intentionally filters out momentum-continuation moves that score high on criteria 2–7 but lack the basing pattern Weinstein considered essential.

---

## Phase classification

| Score | Phase | Interpretation |
|---|---|---|
| 0–1 | ⚪ Not Stage 2 | Stage 1 basing, Stage 3 topping, or Stage 4 decline |
| 2–3 | 🟠 Early / Weak Stage 2 | Early uptrend structure forming; confirm with chart |
| 4–5 | 🟡 Likely Stage 2 | Most Weinstein criteria satisfied; reasonable entry candidate |
| 6–8 | 🟢 Strong Stage 2 | Full bull configuration; best-quality Stage 2 setups |

---

## Filters

### RSI between 50–70
When toggled on, only shows stocks with a 14-period Wilder RSI between 50 and 70. This excludes:
- **RSI < 50**: stock is not yet showing positive momentum
- **RSI > 70**: stock may be near-term overbought and prone to consolidation

This filter is optional and best applied to reduce candidates when the universe is large.

### Show Illiquid
By default, stocks with a 252-day median volume below **100,000 shares/day** are hidden. Toggle this on to include them — useful for researching smaller-cap setups, but exercise caution on execution (wide spreads, impact cost).

---

## Retest indicator

Stocks marked **Retest ✓** have recently:
1. Made a 50-day closing high on at least 2× average volume (the breakout)
2. Pulled back to within ±2% of that breakout level on declining volume (the retest)
3. Bounced back ≥2% above the breakout level (confirmation)

A confirmed retest is considered a lower-risk entry than chasing the initial breakout. The lookback window for detecting the original breakout is **20 trading days**.

---

## How to use

1. Select one or more indices in the sidebar.
2. Click **Run**.
3. Sort the results table by **Score** (descending) to see the strongest setups first.
4. Click any symbol to open the **Phase Chart** and review the full price history and MA structure.
5. Apply RSI and volume filters to narrow to actionable candidates.

> **Tip**: A score of 6 or 7 without the volume criterion (criterion 1) often indicates a stock that has already broken out and is now in a clean Stage 2 advance — these can be excellent swing or position trade entries on pullbacks to the MA50. A score of **8/8** (all criteria including consolidation + volume spike) is the highest-confidence Weinstein base breakout signal.
