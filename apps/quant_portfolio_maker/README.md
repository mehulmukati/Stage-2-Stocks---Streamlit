# Quant-Portfolio-Maker

Quant-Portfolio-Maker is a standalone Streamlit application within the main repository. It provides two single-stock
strategy research workspaces:

- **Enhanced Ichimoku** — configurable long-only entry, exit and risk rules with preset comparison and a next-open
  ₹1 lakh replay.
- **HA + EMA Trend** — completed-week Heikin-Ashi turn signals with EMA, volume and one-year-return filters plus the
  same replay and tax-accounting framework.

The package contains the Streamlit presentation layer only. Indicator calculation, market-data loading, charting,
execution timing and replay accounting continue to come from the repository's shared root modules. Run it from the
repository root with `streamlit run apps/quant_portfolio_maker/app.py`.
