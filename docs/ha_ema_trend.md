# HA + EMA Trend

This page applies a long-only Heikin-Ashi no-wick strategy to one NSE stock at a time. It is an independent
technical-analysis tool and does not change the Momentum Backtest or Live Signal portfolio.

## Entry rules

A new **BUY** decision appears only while the strategy is in cash, after a completed weekly candle, and all eight
gates pass:

1. The previous weekly Heikin-Ashi candle has both an upper and lower wick (indecision).
2. The current weekly Heikin-Ashi candle is bullish.
3. The current weekly Heikin-Ashi Open equals its Low within the selected tolerance (resolution).
4. Weekly Close is above the fast weekly EMA (default 10 weeks).
5. Weekly Close is above the slow weekly EMA (default 30 weeks).
6. The 10-week EMA is above the 30-week EMA.
7. Average weekly share volume over 30 weeks exceeds 100,000 shares.
8. The 52-week point-to-point return exceeds 50%.

## Exit rule

An **EXIT** decision occurs after a completed weekly candle while long when that weekly Heikin-Ashi Open equals its
High within tolerance. A two-wick candle takes no action. EMA, volume and one-year return do not participate in the
exit. Hollow circles show the weekly close where a decision was confirmed, while the prominent triangles show its
execution at the first available market open of the following week. Holding phases run between those execution
dates: green is a completed gross gain, red is a completed gross loss, blue is still open, and unshaded periods are
cash. Phase colors exclude costs and tax; the replay below is the source of truth for net results.

## Weekend decision schedule

Daily observations are aggregated into actual weekly Open, High, Low, Close and total Volume. All HA candles, EMA
10/30 values, 30-week average volume and 52-week return values are then calculated from this weekly series. The
latest in-progress week may be displayed to provide the following week's opening execution, but it cannot create a
new decision until the week is complete. If Friday is an exchange holiday, the last available session earlier that
week completes the weekly candle. The resulting order executes at the actual Open of the first NSE session in the
following week—normally Monday, or Tuesday/later after a holiday. A latest decision with no following-week price
observation remains pending and is not treated as an execution.

The candle-view control changes only the display. **Normal OHLC** (the default) shows the prices that actually
traded, making gaps and executions easiest to inspect. **Heikin-Ashi** shows the synthetic candles used by the
strategy rules and retains an actual-close reference line. Signals, execution markers, holding phases and replay
results do not change when the view is switched.

## ₹1 lakh replay

The strategy and buy-and-hold comparison both start with ₹1,00,000 at the first following-week Open after the first
BUY decision. Each buys the maximum whole shares at that same opening price. The strategy retains residual cash,
exits at the following week's first Open after an EXIT decision, and compounds the proceeds into later entries.
Trading cost and flat brokerage
assumptions are configurable. If no BUY has a following session available for execution, no comparison is shown.
The equity chart also plots buy-and-hold from the exact same first execution open and initial capital.

Pre-tax return includes the configured trading costs but excludes capital-gains tax. The post-tax figure is a
strategy-only estimate using configurable STCG/LTCG rates, financial-year netting and the configured LTCG exemption.
It excludes other investments, surcharge, cess, dividends and personal tax circumstances. When a position remains
open, the headline comparison uses a clearly identified hypothetical sale at the final close.

## Interpretation limits

- Daily adjusted OHLCV is aggregated first; all strategy indicators and HA candles are calculated weekly.
- Weekend decisions execute at the first available NSE Open in the following week.
- The first 52 weeks cannot pass the one-year return gate.
- Average share volume is not the same as traded rupee value.
- Historical results are illustrative and are not investment or tax advice.
