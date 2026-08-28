# Ichimoku Cloud — A Comprehensive Practical Tutorial

Ichimoku Kinko Hyo is a complete trend-following framework designed to answer several questions from a single chart:

- What is the dominant trend?
- Is momentum strengthening or weakening?
- Where is equilibrium?
- Where are likely support and resistance zones?
- Is a breakout structurally strong or weak?
- Is the market trending, transitioning, or simply chopping sideways?

At first glance it looks complicated because it contains five plotted lines plus a shaded cloud. In practice, the system becomes much easier once you stop treating it as five separate indicators and start thinking of it as a **map of price equilibrium across different time horizons**.

> **Interpretation note:** Ichimoku classifications are technical-analysis conventions, not statistically guaranteed forecasts. Treat support, resistance, and signal strength as context to combine with price structure, liquidity, volatility, and risk management.

---

## 1. The Core Idea

Ichimoku is best understood as an **equilibrium and trend-structure system**.

The central question is:

> Where is current price relative to short-, medium-, and longer-term equilibrium?

The standard settings are:

- Tenkan-sen: **9**
- Kijun-sen: **26**
- Senkou Span B: **52**
- Forward displacement: **26**
- Chikou displacement: **26 periods backward**

Usually written as:

`9, 26, 52`

These are the traditional parameters and are still the best place to start.

---

## 2. The Five Components

| Component | Calculation | Main interpretation |
|---|---|---|
| **Tenkan-sen** | `(9-period highest high + 9-period lowest low) / 2` | Fast equilibrium / short-term trend |
| **Kijun-sen** | `(26-period highest high + 26-period lowest low) / 2` | Medium-term equilibrium / trend anchor |
| **Senkou Span A** | `(Tenkan + Kijun) / 2`, plotted 26 periods forward | One edge of future cloud |
| **Senkou Span B** | `(52-period highest high + 52-period lowest low) / 2`, plotted 26 periods forward | Other edge of future cloud |
| **Chikou Span** | Current closing price plotted 26 periods backward | Historical confirmation |

The area between Senkou Span A and Senkou Span B is called the:

> **Kumo — the Cloud**

---

## 3. Important: These Are Not Moving Averages

Tenkan and Kijun are often described as similar to moving averages, but mathematically they are different.

A moving average averages every closing price in its lookback window.

Ichimoku instead finds the midpoint of the recent range.

For example:

```text
9-period highest high = 120
9-period lowest low   = 100

Tenkan = (120 + 100) / 2
       = 110
```

This means the Tenkan is the midpoint of the entire 9-period price range.

That makes the Ichimoku lines useful as measures of **market equilibrium**, rather than merely smoothed price.

---

## 4. A Useful Mental Model

The easiest way to remember the system:

```text
Tenkan  = FAST equilibrium
Kijun   = MEDIUM equilibrium
Kumo    = LONGER-TERM trend/equilibrium zone
Chikou  = HISTORICAL confirmation
```

The philosophy is approximately:

> **Trade in the direction of the larger equilibrium structure, and use shorter-term equilibrium to judge momentum and entry quality.**

---

## 5. Start With the Cloud

If you learn only one thing first, learn this:

```text
                PRICE
                  ↑

              Above cloud
          = bullish territory

        ███████████████
        █    KUMO     █
        ███████████████

              Below cloud
          = bearish territory
```

### Price Above the Cloud

Generally bullish.

The market has moved above its medium/longer-term equilibrium structure.

Interpretation:

> Buyers are in control.

### Price Inside the Cloud

Neutral, transitional, or uncertain.

This is often where traders get chopped up.

Interpretation:

> The market has no strong directional edge.

### Price Below the Cloud

Generally bearish.

Interpretation:

> Sellers are in control.

---

## 6. The Basic Regime Filter

A very simple Ichimoku framework is:

```text
Price above Kumo  -> Prefer longs
Price inside Kumo -> Neutral / avoid forcing trades
Price below Kumo  -> Prefer shorts or avoid longs
```

For a long-only investor, this can become:

```text
Above cloud -> eligible
Inside cloud -> caution
Below cloud -> avoid / watchlist
```

This alone can act as a dynamic trend filter.

---

## 7. Tenkan-Sen — The Fast Line

The Tenkan represents short-term equilibrium.

Formula:

```text
Tenkan =
(highest high over 9 periods + lowest low over 9 periods) / 2
```

It responds relatively quickly to changes in price.

### Rising Tenkan

Short-term equilibrium is rising.

Usually bullish.

### Falling Tenkan

Short-term equilibrium is falling.

Usually bearish.

### Flat Tenkan

The 9-period range has stopped shifting.

This often happens during consolidation.

---

## 8. Kijun-Sen — The Most Important Line

The Kijun is often the single most useful line after the cloud.

Formula:

```text
Kijun =
(highest high over 26 periods + lowest low over 26 periods) / 2
```

Think of it as:

> **The market's medium-term equilibrium.**

In a healthy uptrend, price frequently pulls back toward the Kijun and then resumes higher.

Example:

```text
                price
                  /
                /
              /
      Tenkan /
           /
--------------------- Kijun
        ███████
        Kumo
```

A Kijun pullback can therefore act like dynamic support.

In a downtrend, the opposite can occur.

---

## 9. Why the Kijun Can Behave Like a Magnet

Imagine:

```text
26-period high = 160
26-period low  = 90

Kijun = (160 + 90) / 2
      = 125
```

If current price has surged to 160, it is far above medium-term equilibrium.

Eventually one of two things often happens:

1. Price returns toward Kijun.
2. Price moves sideways while Kijun catches up.

This is why a stock can remain bullish while still being temporarily overextended.

---

## 10. Flat Kijun

A flat Kijun is particularly interesting.

```text
Price        /\    /\
            /  \__/  \

Kijun  ----------------------
```

A flat Kijun means the 26-period highest high and lowest low have not changed enough to move the midpoint.

It often becomes a strong equilibrium reference.

Traders sometimes describe a flat Kijun as having a **magnetic effect** on price.

Do not treat that as a physical law, but it is a useful visual phenomenon to watch.

---

## 11. Tenkan vs Kijun

This relationship measures shorter-term momentum relative to medium-term equilibrium.

### Bullish

```text
Tenkan > Kijun
```

### Bearish

```text
Tenkan < Kijun
```

---

## 12. Tenkan-Kijun Crosses

When Tenkan crosses above Kijun:

> **Bullish TK cross**

When Tenkan crosses below Kijun:

> **Bearish TK cross**

But Ichimoku does **not** treat every crossover equally.

The location relative to the cloud matters.

---

## 13. Strength of a Bullish TK Cross

| Location of bullish TK cross | Signal strength |
|---|---|
| **Above the cloud** | Strong |
| **Inside the cloud** | Moderate / neutral |
| **Below the cloud** | Weak |

Why?

A bullish crossover below a bearish cloud is still fighting the dominant trend.

---

## 14. Strength of a Bearish TK Cross

| Location of bearish TK cross | Signal strength |
|---|---|
| **Below the cloud** | Strong |
| **Inside the cloud** | Moderate |
| **Above the cloud** | Weak |

This is one of the most important Ichimoku concepts.

A bearish TK cross above a strong rising cloud may simply mean:

> short-term correction inside a larger uptrend.

It does **not** automatically mean the stock has turned bearish.

---

## 15. Senkou Span A

Formula:

```text
Span A = (Tenkan + Kijun) / 2
```

Then it is plotted **26 periods forward**.

Span A reacts faster than Span B.

---

## 16. Senkou Span B

Formula:

```text
Span B =
(highest high over 52 periods + lowest low over 52 periods) / 2
```

Then plotted **26 periods forward**.

Span B is slower and reflects longer-term equilibrium.

---

## 17. The Future Cloud

If:

```text
Span A > Span B
```

the cloud is generally considered bullish.

If:

```text
Span A < Span B
```

the cloud is bearish.

---

## 18. Why Is the Cloud Plotted Into the Future?

This is often misunderstood.

The future cloud is **not a price prediction**.

It does not mean:

> "The system predicts price will be here 26 days later."

Instead:

> Today's equilibrium calculations are shifted forward to show potential future support/resistance structure.

Conceptually:

```text
Today                         Future
  |                              |
  V                              V

Current price action       ███████████
                           Future cloud
                           ███████████
```

It gives you a visual picture of how today's equilibrium structure extends into the future.

---

## 19. Bullish Future Cloud

Typical characteristics:

- Span A above Span B
- cloud often rising
- price above cloud

This suggests supportive bullish structure.

---

## 20. Bearish Future Cloud

Typical characteristics:

- Span A below Span B
- cloud often falling
- price below cloud

This suggests persistent bearish structure.

---

## 21. Cloud Thickness

Cloud thickness matters.

### Thick cloud

```text
██████████████████
██████████████████
██████████████████
```

Represents greater separation between the two equilibrium measures.

Often acts as stronger support/resistance.

### Thin cloud

```text
██████████████████
```

Represents compressed equilibrium.

Price may pass through more easily.

---

## 22. Cloud Slope

The direction of the cloud adds context to its color.

### Rising bullish cloud

Supportive evidence of bullish trend structure; not confirmation on its own.

### Flat cloud

Possible equilibrium / consolidation.

### Falling bearish cloud

Persistent bearish structure.

---

## 23. The Kumo Twist

A Kumo twist occurs when Span A and Span B cross.

The future cloud changes from bullish to bearish or vice versa.

This can signal a change in underlying structure.

But:

> **A Kumo twist by itself is not a buy or sell signal.**

It is best treated as evidence that the previous trend structure is weakening or changing.

---

## 24. Chikou Span

The Chikou Span is simply:

> **Today's closing price plotted 26 periods backward.**

Suppose today's close is 500.

Ichimoku plots:

```text
500
```

on the chart **26 sessions ago**.

---

## 25. Why Chikou Exists

Chikou asks:

> **Is today's price strong relative to the market structure that existed 26 periods ago?**

For bullish confirmation, ideally Chikou is:

- above historical price candles
- above the historical cloud
- relatively free of congestion

For bearish confirmation:

- below historical price
- below historical cloud

---

## 26. Chikou as a Clearance Test

Imagine today's breakout looks bullish.

But Chikou is trapped inside a dense group of historical candles.

That tells you:

> The breakout still faces historical congestion.

By contrast:

```text
Chikou
   ↑
   |
   |        clear space
   |
old candles ███████
```

suggests stronger confirmation.

---

## 27. Chikou in Sideways Markets

If Chikou is constantly moving through historical candles:

> the market is probably congested.

This is another way Ichimoku reveals sideways conditions.

---

## 28. Textbook Strong Bullish Structure

A strong bullish Ichimoku setup generally has:

1. Price above cloud
2. Tenkan above Kijun
3. Bullish TK cross ideally above cloud
4. Future cloud bullish
5. Kijun rising
6. Chikou above historical price
7. Cloud itself rising
8. Price not absurdly extended from Kijun

Conceptually:

```text
                         PRICE
                           /
                         /
                Tenkan  /
                      /
              Kijun  /
------------------------------

        ███████████████
        █ Bullish Kumo █
        ███████████████

Chikou also clear above old price
```

---

## 29. Textbook Strong Bearish Structure

Reverse the conditions:

1. Price below cloud
2. Tenkan below Kijun
3. Bearish TK cross below cloud
4. Future cloud bearish
5. Kijun falling
6. Chikou below historical price
7. Cloud falling

Conceptually:

```text
        ███████████████
        █ Bearish Kumo █
        ███████████████

------------------------------ Kijun
                     \
                      \ Tenkan
                       \
                        \ PRICE
```

---

## 30. The Three Main Market Regimes

Ichimoku becomes easier if you classify every chart into one of three states.

### A. Bullish

```text
Price > Cloud
```

Prefer long setups.

### B. Bearish

```text
Price < Cloud
```

Avoid longs or favor bearish setups.

### C. Neutral

```text
Price inside Cloud
```

Avoid forcing trades.

---

## 31. A More Detailed Five-State Framework

### State 1 — Strong Bull

```text
Price > cloud
Tenkan > Kijun
Future cloud bullish
Chikou clear
```

### State 2 — Bullish Correction

```text
Price > cloud
but Tenkan < Kijun
```

Larger trend intact, short-term momentum weak.

### State 3 — Transition

```text
Price inside cloud
```

Trend uncertain.

### State 4 — Bearish Rally

```text
Price < cloud
but Tenkan > Kijun
```

Short-term improvement inside a bearish regime.

### State 5 — Strong Bear

```text
Price < cloud
Tenkan < Kijun
Future cloud bearish
Chikou below old price
```

---

## 32. Bullish Kumo Breakout

One of the most important signals is when price moves:

```text
Below cloud
     ↓
Inside cloud
     ↓
Above cloud
```

This can indicate a trend reversal.

But the strongest version is not just the initial breakout.

You ideally want:

- price to close above cloud
- Tenkan > Kijun
- Kijun rising
- future cloud turning bullish
- Chikou clearing old price

---

## 33. False Cloud Breakouts

A common failure pattern:

```text
          Price breaks above
                 /\
                /  \
████████████████    \
██████ CLOUD ███     \
████████████████      \ back inside
```

A brief move above the Kumo followed immediately by re-entry can be a false breakout.

Confirmation matters.

---

## 34. Healthy Bullish Pullback

One of the best trend-following situations is:

```text
Strong uptrend
      /
     /
    /
   /\
  /  \   <- pullback
 /    \____
          ↑
       Kijun
```

Ideally:

- price remains above cloud
- Kijun remains rising
- Tenkan may temporarily weaken
- price finds support near Tenkan/Kijun
- trend resumes

This can be preferable to chasing a vertically extended stock.

---

## 35. Healthy Bearish Rally

Reverse situation:

```text
Price below cloud
rallies toward Kijun/cloud
fails
resumes decline
```

Bear-market rallies often look impressive in isolation.

Ichimoku helps identify whether they actually changed the larger structure.

---

## 36. Trend vs Momentum

This distinction is essential.

Suppose:

```text
Price > cloud
Tenkan crosses below Kijun
```

What happened?

- Trend: still bullish
- Momentum: weakening

Therefore:

> **Momentum deterioration is not automatically trend reversal.**

Likewise:

```text
Price < cloud
Tenkan crosses above Kijun
```

means:

- Trend: still bearish
- Momentum: improving

This may just be a countertrend bounce.

---

## 37. Equilibrium vs Extension

Imagine a stock:

```text
Price = 500
Kijun = 420
```

The trend may be extremely bullish.

But price may also be highly extended.

Therefore always separate:

```text
Trend quality
```

from:

```text
Entry quality
```

A stock can be an excellent trend and a poor immediate entry.

---

## 38. Sideways Markets and Whipsaws

Ichimoku performs best in directional trends.

It performs poorly when:

- cloud is flat
- price repeatedly crosses cloud
- Tenkan and Kijun repeatedly cross
- Chikou is tangled in old candles

Conceptually:

```text
Price  ~~~~~~~~
Tenkan ~~~~~~~~
Kijun  ~~~~~~~~
Cloud  ████████
```

This means:

> **There is no meaningful directional edge.**

One of Ichimoku's great strengths is that it gives you permission to do nothing.

---

## 39. How to Recognize a Base

A potential base often shows:

- falling Kijun starts flattening
- falling cloud becomes thinner
- price stops making major new lows
- Tenkan/Kijun begin crossing frequently
- future Kumo compresses

This is not yet bullish.

It means:

> **The bearish trend may be losing force.**

Confirmation only comes later if price establishes itself above the cloud.

---

## 40. How to Recognize a New Stage-2-Type Trend

A useful sequence is:

```text
1. Downtrend
2. Kijun flattens
3. Cloud thins
4. Price enters cloud
5. Price exits above cloud
6. Tenkan > Kijun
7. Future cloud becomes bullish
8. Kijun turns upward
9. Chikou clears historical price
10. Price begins forming higher highs and higher lows
```

This is very compatible with Stage Analysis and momentum investing.

---

## 41. Ichimoku and Stage Analysis

Approximate conceptual mapping:

| Stage / momentum concept | Ichimoku analogue |
|---|---|
| Stage 1 base | Flat/thin cloud, price around cloud |
| Stage 2 uptrend | Price above rising bullish cloud |
| Stage 3 distribution | Flattening cloud, price repeatedly entering cloud |
| Stage 4 decline | Price below falling bearish cloud |
| Breakout | Cloud breakout |
| Pullback to MA | Pullback toward Kijun |
| Trend deterioration | Bearish TK cross / loss of Kijun |
| Trend failure | Re-entry into and break below cloud |

They are not identical systems, but they fit together naturally.

---

## 42. Ichimoku and Momentum Investing

If you already use momentum ranking, Ichimoku can be used as a secondary structural filter.

Example:

```text
Momentum-ranked stock universe
              ↓
      Ichimoku classification
```

Possible categories:

| Grade | Structure |
|---|---|
| **A+** | Price above cloud + TK bullish + future cloud bullish + Chikou clear |
| **A** | Price above cloud + TK bullish |
| **B** | Price above cloud but TK bearish |
| **C** | Price inside cloud |
| **D** | Price below cloud |

This can help distinguish strong momentum stocks from unstable ones.

---

## 43. A Simple Long-Only Ichimoku System

A very simple filter:

```text
Price > cloud
AND
Tenkan > Kijun
AND
Future cloud bullish
```

Optional confirmation:

```text
Chikou > historical price
```

Then look for:

- breakout
- pullback to Tenkan/Kijun
- high consolidation followed by continuation

---

## 44. Conservative Long Setup

Require:

```text
Price > Kumo
Tenkan > Kijun
Future Kumo bullish
Kijun rising
Chikou clear
```

Entry:

```text
Pullback toward Tenkan/Kijun
OR
breakout above recent resistance
```

---

## 45. Aggressive Reversal Setup

Earlier entry, more risk.

Look for:

```text
Price entering cloud
Tenkan > Kijun
Kijun flattening
Future cloud thinning
```

But recognize:

> This is still transition, not confirmed trend.

---

## 46. Conservative Reversal Setup

Wait for:

```text
Price above cloud
Tenkan > Kijun
Future cloud bullish
Chikou clear
```

You sacrifice the exact bottom in exchange for greater confirmation.

That is entirely consistent with trend-following philosophy.

---

## 47. Support Hierarchy in a Bull Trend

Roughly:

```text
Price
  ↓
Tenkan
  ↓
Kijun
  ↓
Top of Kumo
  ↓
Bottom of Kumo
```

The deeper price falls through this hierarchy, the more the trend weakens.

---

## 48. Warning Hierarchy in a Bull Trend

### Mild warning

Price falls below Tenkan.

### Stronger warning

Price falls below Kijun.

### Serious warning

Price enters cloud.

### Major regime failure

Price closes and remains below cloud.

---

## 49. Warning Hierarchy in a Bear Trend

Reverse it.

### Mild improvement

Price rises above Tenkan.

### Stronger improvement

Price rises above Kijun.

### Meaningful transition

Price enters cloud.

### Major reversal evidence

Price breaks and holds above cloud.

---

## 50. Cloud as Support and Resistance

In an uptrend:

> **Kumo often acts as support.**

In a downtrend:

> **Kumo often acts as resistance.**

Example bearish rally:

```text
████████████████ CLOUD
        ↑
       /\
      /  \
     /    \
price      \ declines again
```

This is why rallies into the cloud during a bear trend often fail.

---

## 51. Multiple Flat Levels

Flat Kijun and flat Span B levels can become especially important.

Why?

Because they represent persistent equilibrium values.

If several Ichimoku components cluster around the same price:

```text
Flat Kijun
Flat Span B
Previous support
```

that price zone may become structurally important.

---

## 52. Timeframes

Ichimoku can be used on:

- intraday charts
- daily charts
- weekly charts
- monthly charts

For investing, daily and weekly are usually the most useful.

A useful multi-timeframe approach:

```text
Weekly -> structural trend
Daily  -> entry / pullback / breakout
```

Example:

```text
Weekly above bullish cloud
+
Daily Kijun pullback
=
potentially attractive trend-following setup
```

---

## 53. Daily vs Weekly Conflict

Suppose:

```text
Weekly = bullish
Daily = bearish correction
```

Interpretation:

> Possible pullback inside a larger uptrend.

Suppose:

```text
Weekly = bearish
Daily = bullish
```

Interpretation:

> Possibly only a countertrend rally.

Higher timeframe context matters.

---

## 54. Why Standard 9/26/52 Settings Matter

You will see alternative settings such as:

- 10 / 30 / 60
- 20 / 60 / 120
- 7 / 22 / 44

But do not optimize parameters before understanding the standard system.

Otherwise it becomes easy to fit historical noise.

Start with:

```text
9 / 26 / 52
```

and learn how price behaves around those structures.

---

## 55. Common Mistakes

### Mistake 1: Trading every TK cross

Wrong.

Cross location relative to the cloud matters.

### Mistake 2: Treating a Kumo twist as a prediction

The future cloud is not forecasting price.

It shows projected equilibrium structure.

### Mistake 3: Buying because price is above the cloud

A stock may be extremely extended from Kijun.

Trend can be bullish while entry quality is poor.

### Mistake 4: Calling every rally a reversal

If price remains below a bearish cloud, it may simply be a countertrend rally.

### Mistake 5: Ignoring Chikou congestion

A breakout with Chikou trapped in old price can be weaker than one with clear space.

### Mistake 6: Using Ichimoku in heavy chop

Flat cloud + repeated TK crosses = low-value signals.

### Mistake 7: Treating all green clouds as equally bullish

Consider:

- slope
- thickness
- position relative to price
- trend of Kijun
- Chikou confirmation

---

## 56. A Practical Chart-Reading Order

When looking at an Ichimoku chart, read it in this order.

### Step 1 — Price vs Cloud

Ask:

```text
Above?
Inside?
Below?
```

This determines regime.

### Step 2 — Cloud Structure

Ask:

- bullish or bearish?
- rising or falling?
- thick or thin?
- expanding or compressing?

### Step 3 — Tenkan vs Kijun

Ask:

- Tenkan above or below?
- cross recent?
- where did the cross occur?
- are the lines rising or falling?

### Step 4 — Kijun

Ask:

- rising?
- falling?
- flat?
- how far is price from it?

This helps distinguish:

```text
strong trend
```

from:

```text
overextended trend
```

### Step 5 — Chikou

Ask:

- above historical price?
- below?
- tangled?

This is confirmation.

### Step 6 — Price Structure

Finally check ordinary technical structure:

- higher highs / higher lows
- lower highs / lower lows
- support/resistance
- consolidation
- breakout
- failed breakout

Ichimoku should not replace basic price reading.

---

## 57. A Fast 30-Second Checklist

For every chart:

```text
1. Price above / inside / below cloud?
2. Future cloud bullish or bearish?
3. Cloud rising, falling or flat?
4. Tenkan above or below Kijun?
5. Kijun rising, falling or flat?
6. Chikou clear or congested?
7. Is price stretched from Kijun?
8. Is this trend, transition or chop?
```

---

## 58. Scoring a Bullish Chart

A simple qualitative scoring framework:

```text
+2 Price above cloud
+1 Tenkan > Kijun
+1 Kijun rising
+1 Future cloud bullish
+1 Future cloud rising
+1 Chikou above old price
+1 Price structure higher highs/higher lows
-1 Extreme stretch from Kijun
-1 Cloud very thin / unstable
```

This is not an official Ichimoku formula.

It is merely a useful framework for comparing charts consistently.

---

## 59. Example Classifications

### Strong Emerging Bull

```text
Price recently broke above cloud
Tenkan > Kijun
Future cloud turning bullish
Kijun rising
Chikou clear
```

Interpretation:

> New trend may be beginning.

### Mature Bull

```text
Price above rising cloud for months
Kijun rising steadily
Future cloud bullish
Chikou clear
```

Interpretation:

> Established trend.

### Bullish Correction

```text
Price still above cloud
Tenkan < Kijun
Kijun flattening
```

Interpretation:

> Short-term weakness, larger trend intact.

### Transition / Base

```text
Price around cloud
Cloud thin and flat
Tenkan/Kijun cross repeatedly
Chikou congested
```

Interpretation:

> No edge yet.

### Mature Bear

```text
Price below falling cloud
Tenkan < Kijun
Kijun falling
Future cloud bearish
Chikou below old price
```

Interpretation:

> Persistent downtrend.

### Bearish Trend Stabilizing

```text
Price below cloud
Kijun flattening
Cloud thinning
Price no longer making rapid new lows
```

Interpretation:

> Downtrend weakening, but bullish reversal not confirmed.

---

## 60. Ichimoku Does Not Predict Tops and Bottoms

Ichimoku is deliberately late.

It is designed to sacrifice:

```text
the exact bottom
```

in exchange for:

```text
evidence that the trend has actually changed
```

Likewise, it may not exit at the absolute top.

It tries to keep you with the trend until structural evidence deteriorates.

That is the nature of trend-following.

---

## 61. The Most Important Distinction

Always separate:

```text
TREND
```

from:

```text
MOMENTUM
```

and from:

```text
ENTRY QUALITY
```

Example:

```text
Price well above rising cloud
Tenkan > Kijun
Kijun rising
```

Trend quality may be excellent.

But if:

```text
Price = 500
Kijun = 420
```

entry quality may be poor because the stock is stretched.

Conversely, a pullback to Kijun may improve entry quality without damaging the broader trend.

---

## 62. Ichimoku and Risk Management

Ichimoku can help structure exits, though stop placement should still account for volatility and position sizing.

Possible trailing references:

- Tenkan — tight
- Kijun — medium
- cloud edge — loose
- opposite side of cloud — regime-based

Example:

```text
Aggressive trader -> Tenkan
Swing trader      -> Kijun
Trend follower    -> Cloud
```

The looser the reference, the more noise you tolerate.

---

## 63. Why the Cloud Can Be More Informative Than a Single Moving Average

A moving average gives one line.

Ichimoku gives a **zone**.

Markets do not always reverse at an exact number.

A cloud better reflects the idea that support and resistance are often regions.

---

## 64. Ichimoku as a Complete Narrative

Instead of saying:

```text
RSI = 67
MACD bullish
Price above MA
```

Ichimoku gives one integrated story:

```text
Price is above longer-term equilibrium.
Short-term equilibrium is above medium-term equilibrium.
Medium-term equilibrium is rising.
Future support is rising.
Current price has historical clearance.
```

That is why it can function as a complete system.

---

## 65. What Ichimoku Is Best At

Ichimoku is particularly good for:

- identifying sustained trends
- filtering countertrend rallies
- recognizing trend deterioration
- finding pullbacks within trends
- distinguishing base-building from confirmed reversal
- visualizing dynamic support/resistance
- avoiding sideways markets

---

## 66. What Ichimoku Is Not Best At

It is less suitable for:

- exact bottom picking
- exact top picking
- ultra-short mean-reversion trades
- highly illiquid stocks
- chaotic gap-driven markets without follow-through
- range trading when price is constantly crossing the cloud

---

## 67. A Good Learning Sequence

Do not try to master everything at once.

### Level 1

Only study:

```text
Price vs Kumo
```

### Level 2

Add:

```text
Tenkan vs Kijun
```

### Level 3

Add:

```text
Kijun slope and price distance from Kijun
```

### Level 4

Add:

```text
Future cloud direction/thickness
```

### Level 5

Add:

```text
Chikou
```

### Level 6

Combine with:

```text
ordinary price structure
volume
momentum
Stage Analysis
relative strength
```

---

## 68. The One-Sentence Summary

Ichimoku can be summarized as:

> **Trade in the direction of the larger equilibrium structure, use Tenkan and Kijun to judge momentum and pullbacks, use the cloud to classify the market regime, and use Chikou as confirmation.**

---

## 69. The One-Page Cheat Sheet

```text
==============================================================
                    ICHIMOKU CHEAT SHEET
==============================================================

PRICE VS CLOUD
--------------------------------------------------------------
Above cloud        = Bullish
Inside cloud       = Neutral / transition
Below cloud        = Bearish

TENKAN VS KIJUN
--------------------------------------------------------------
Tenkan > Kijun     = Bullish momentum
Tenkan < Kijun     = Bearish momentum

TK CROSS STRENGTH
--------------------------------------------------------------
Bull cross above cloud = Strong
Bull cross inside      = Medium
Bull cross below       = Weak

Bear cross below cloud = Strong
Bear cross inside      = Medium
Bear cross above       = Weak

FUTURE CLOUD
--------------------------------------------------------------
Span A > Span B    = Bullish Kumo
Span A < Span B    = Bearish Kumo

Rising cloud       = Trend strengthening
Flat cloud         = Equilibrium / range
Thin cloud         = Easier to penetrate
Thick cloud        = Stronger support/resistance

KIJUN
--------------------------------------------------------------
Rising             = Medium-term trend improving
Falling            = Medium-term trend deteriorating
Flat                = Persistent equilibrium level

Price far above    = Possible overextension
Pullback to Kijun  = Potential trend entry

CHIKOU
--------------------------------------------------------------
Above old price    = Bullish confirmation
Below old price    = Bearish confirmation
Inside old price   = Congestion / uncertainty

IDEAL BULL
--------------------------------------------------------------
Price > cloud
Tenkan > Kijun
Kijun rising
Future cloud bullish
Cloud rising
Chikou clear above old price

IDEAL BEAR
--------------------------------------------------------------
Price < cloud
Tenkan < Kijun
Kijun falling
Future cloud bearish
Cloud falling
Chikou clear below old price

WARNING HIERARCHY IN UPTREND
--------------------------------------------------------------
Lose Tenkan        = Mild
Lose Kijun         = Stronger warning
Enter cloud        = Trend uncertainty
Break below cloud  = Regime failure

MARKET STATE
--------------------------------------------------------------
Above cloud + aligned lines        = Trend
Inside cloud + crossing lines      = Transition
Flat thin cloud + repeated crosses = Chop
==============================================================
```

---

## 70. Final Practical Rule

When looking at an Ichimoku chart, do not begin by asking:

> "Should I buy?"

Begin by asking:

```text
What regime is this market in?
```

Then:

```text
How strong is that regime?
```

Then:

```text
Is momentum aligned with the regime?
```

Then:

```text
Is price close enough to equilibrium to offer a reasonable entry?
```

That sequence prevents many bad interpretations.

A chart can be:

- bullish but overextended
- bullish but correcting
- bearish but stabilizing
- bearish but experiencing a rally
- neutral and building a base
- completely directionless

Ichimoku's real power is helping you distinguish those states quickly.

---

## Suggested Practice

For each chart, try to answer these eight questions before looking at any other indicator:

1. Is price above, inside, or below the cloud?
2. Is the future cloud bullish or bearish?
3. Is the cloud rising, falling, or flat?
4. Is Tenkan above or below Kijun?
5. Is Kijun rising, falling, or flat?
6. Is Chikou clear or congested?
7. Is price stretched from Kijun?
8. Is this a trend, a correction, a transition, or chop?

If you can answer those reliably, you understand most of what Ichimoku is trying to tell you.
