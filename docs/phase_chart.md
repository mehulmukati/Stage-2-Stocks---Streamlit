# Phase Chart

The Phase Chart displays the complete price history of a single stock overlaid with its moving averages and colour-coded Stage 2 phase bands — giving a visual picture of where the stock is in its Weinstein cycle at any point in time.

---

## Reading the chart

### Price and moving averages
Three MAs are plotted alongside the daily close:

| Line | Period | Weinstein equivalent |
|---|---|---|
| **MA50** | 50-day SMA | ~10-week MA (short-term trend) |
| **MA150** | 150-day SMA | ~30-week MA (intermediate trend) |
| **MA200** | 200-day SMA | ~40-week MA (long-term base) |

The MA200 is the most important structural line. A rising MA200 with price above it is a necessary condition for Stage 2.

### Phase bands
The background is shaded according to the rolling daily Stage 2 score:

| Colour | Phase | Score |
|---|---|---|
| 🟢 Green | Strong Stage 2 | 6–7 |
| 🟡 Yellow | Likely Stage 2 | 4–5 |
| 🟠 Orange | Early / Weak Stage 2 | 2–3 |
| *(no shade)* | Not Stage 2 | 0–1 |

Phase bands only appear once the MA200 has enough history to be computed (200 trading days from the start of the data).

### Log scale
By default the y-axis uses a **logarithmic scale**, which shows percentage moves consistently regardless of price level — a move from 100 to 200 (100%) looks the same as a move from 500 to 1000 (100%). This is almost always preferable for multi-year charts. Toggle it off in the chart controls if you need a linear view.

---

## How to use

1. Enter an NSE symbol in the sidebar (e.g. `RELIANCE`, `HDFCBANK`, `TATAPOWER`).
2. The chart loads automatically. Pan and zoom with your mouse or trackpad.
3. Hover over any bar to see the date, close price, and all three MAs.
4. Look for **transitions from no-shade → orange → yellow → green** as the stock builds its Stage 2 structure.
5. The most reliable entries are when the stock has been in a green phase for several weeks, pulls back to the MA50 without the phase deteriorating, and bounces with volume.

---

## Common patterns to look for

### Clean Stage 2 advance
Long stretches of green or yellow with price staying above the MA50 and the MAs fanning upward. Brief dips to MA50 followed by resumptions are healthy.

### Stage 2 breakdown warning
Phase colour shifts from green → yellow → orange over several weeks, price crosses below the MA50, and the MA50 begins to flatten. This is a warning to reduce or exit.

### Basing before breakout
Extended period of no shading (Stage 1), MAs flat, price moving sideways. When this is followed by a strong volume day and the phase colour begins to appear, it may mark the early Stage 2 breakout.

---

## Symbol lookup

If you enter a symbol that is not found, the app will suggest the closest match from the constituent universe. Symbols should be entered as they appear on NSE (no `.NS` suffix needed).
