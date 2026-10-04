# Multi Charts

Multi Charts is a top-level section with two comparison pages. It uses the existing Stage 2,
standard Ichimoku, and HA + EMA calculations. Enhanced Ichimoku is not included.

## One Stock, Three Views

Enter an NSE symbol in the sidebar. The page shows the three analyses and a compact snapshot in
a 2-by-2 board. Each chart has a text link that opens its full page for detailed inspection.

The display range is shared by default. Turn off the shared-range toggle to choose a different
range for each chart. Ichimoku can use daily or weekly candles. HA + EMA uses the current full-page
strategy defaults and normal OHLC candle display; its signals are calculated from weekly
Heikin-Ashi candles. Range controls change only the visible chart window, not the history used to
calculate indicators.

## Many Stocks, One View

Choose one analysis and provide symbols with a one-column UTF-8 CSV headed `Symbol` or `Ticker`,
or paste symbols separated by commas, whitespace, or semicolons. You can use both inputs together.
Repeated symbols appear once, in first-occurrence order. NSE symbols should omit the `.NS` suffix.

Select 1-4 rows and 1-3 columns per page. The grid preserves input order; use the compact
pagination bar below the charts to move with Previous/Next or select a page number. Each
tile includes a compact chart, the latest available price date, and the analysis-specific state.
Invalid or unavailable symbols stay in the grid as explanatory tiles.

**Prepare PDF of all pages** calculates every supplied symbol and creates a PDF with the selected
number of rows and columns on every page. The PDF preserves the input order, includes error tiles,
and labels pages and generation time. A dense grid may be difficult to read, so the app warns for
3 columns with 3 or more rows. PDF preparation is on demand; the visible page loads independently.

Stage 2 retains its existing score out of 8. Ichimoku and HA + EMA show their native conditions
and signals rather than an artificial combined score.
