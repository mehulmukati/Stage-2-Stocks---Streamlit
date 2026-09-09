# Ichimoku Strategy Replay

The replay converts the descriptive 9 / 26 / 52 Ichimoku chart into explicit, long-only rules. A signal is evaluated only after a candle is complete and is executed at the next observed candle's actual Open.

## Presets

| Preset | Entry | Exit | Character |
|---|---|---|---|
| **Aggressive** | Bullish TK cross while price is inside or above the Kumo; strong and neutral crosses qualify | Close below Kijun | Earlier signals and higher turnover |
| **Balanced** | Price above Kumo, Tenkan above Kijun, bullish forward Kumo, and Close above the High 26 periods earlier | Close enters the Kumo | Confirmed trend with moderate protection |
| **Conservative** | Bullish Kumo breakout with aligned TK, bullish forward Kumo, Chikou clearance, non-falling Kijun, and two qualifying closes | Close below the entire Kumo | Fewer trades and more pullback tolerance |

Changing a resolved preset rule makes the active strategy **Custom**. If Custom exactly matches a preset, the replay identifies that match.

## Meaningful configuration

The available controls depend on the primary entry event. A bullish TK cross already implies Tenkan has crossed above Kijun. A Kumo breakout already implies the current Close is above the cloud. Those redundant choices are therefore not presented.

Entry confirmation means the complete qualifying setup remains valid for the requested number of consecutive completed candles. For a TK cross, the initial cross arms the setup and the resulting alignment must remain valid; another cross is not required.

One primary technical exit is selected. Optional fixed, trailing, and maximum-holding exits are safety overrides. The first completed exit condition wins.

## Point-in-time safeguards

- The displayed cloud at a signal date contains only spans calculated from earlier observations and displaced forward.
- The forward-cloud filter uses spans calculable on the signal date and conceptually projects them forward. It never reads later market candles.
- Chikou confirmation compares the current Close with the Close or High 26 periods earlier.
- Daily rules use completed daily candles. Weekly rules use completed weekly candles.
- A last-candle signal without a following Open remains unexecuted.

## ₹1 lakh accounting

- Long-only and fully invested when a BUY can be executed
- Whole shares with residual cash retained
- Configurable percentage cost and fixed brokerage on each side
- Open positions marked to the latest Close
- Hypothetical end-date liquidation for headline pre-tax and estimated post-tax values
- Illustrative Indian STCG/LTCG calculation using financial-year netting
- Buy-and-hold begins at the selected strategy's first executable BUY

The four-strategy chart uses a different, deliberately common comparison start. Aggressive, Balanced, Conservative, and Custom all begin with the same cash on the earliest date where every strategy has sufficient indicator history. Each curve stays flat in cash until its own first execution.

## Interpreting signals

Hollow circles mark completed-candle signals. Solid triangles mark next-Open executions. The chart and trade ledger are generated from the same resolved configuration.

This replay is an illustrative technical-strategy simulation, not investment or tax advice. Daily and weekly OHLC data cannot reproduce intraday stop fills, slippage, market impact, rejected orders, or personal tax circumstances.
