# Quant-Portfolio-Maker

Quant-Portfolio-Maker is a standalone Streamlit application within this repository. Run it with
`streamlit run apps/quant_portfolio_maker/app.py`. It uses the same market-data, charting and calculation modules as
the screeners while keeping its navigation, controls and replay workflow separate from the main application.

## Enhanced Ichimoku

The enhanced workspace adds configurable long-only strategy rules to the standard Ichimoku chart. Aggressive,
Balanced and Conservative presets can be compared from a common cash start, or adjusted into a Custom strategy.
Completed-candle signals execute at the next observed market Open. The replay supports whole shares, costs,
brokerage, tax assumptions and trade-ledger export.

## HA + EMA Trend

The weekly HA + EMA workspace looks for a bullish no-lower-wick Heikin-Ashi resolution after a two-wick candle,
subject to EMA alignment, average-volume and one-year-return gates. A no-upper-wick candle exits a held position.
Completed-week decisions execute at the first available Open of the following week.

## Portfolio Lab

Portfolio Lab applies one of the built-in quant methods across the same index universe used by the Momentum tools.
Version 1 is a long-only fixed-slot portfolio: exits run before entries and each successful entry receives
`1 / fixed slots` of current portfolio value. Unfilled slots remain cash and existing holdings drift without routine
rebalancing. Same-session HA + EMA candidates can use either the strategy composite or configurable-session relative
strength versus Nifty 100. Relative strength is the stock return factor divided by the Nifty 100 return factor, using
only observations available through the completed signal date. Ichimoku continues to use its strategy score. Exact
score ties are resolved by ticker.

The backtest uses historical constituent snapshots when available, minimum-history and liquidity gates, whole
shares, costs, brokerage and optional tax rates. Every decision is retained with its execution or skip outcome,
rule diagnostics and reason. Results include benchmark NAV, CAGR, Sharpe, Sortino, drawdown, Calmar, cash/exposure,
capacity, signal conversion, holding-period, trade-quality, concentration, cost and tax metrics, plus downloadable
NAV, trades and the complete decision log.

Portfolio Lab reads the bundled ten-year full-OHLCV research baseline, whose coverage is shown in the run progress
and results. Requests beginning before that baseline are rejected explicitly instead of being silently shortened.

New methods should implement the shared contract in `quant_strategy.py` and register an adapter in
`quant_strategy_registry.py`; the portfolio simulator and UI do not need a strategy-specific branch.

Both tools are illustrative research simulations rather than investment or tax advice.
