"""Index overlays read the same accepted pricing file as all other pages."""

import market_data as shared_market

NSE_INDEX_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty Next 50": "^NSMIDCP",
    "Nifty Midcap 150": "NIFTYMIDCAP150.NS",
    "Nifty Smallcap 250": "NIFTYSMLCAP250.NS",
    "Nifty Microcap 250": "NIFTYMICROCAP250.NS",
}


def load_index_overlay_series(names, start, end):
    snapshot = shared_market.load_snapshot(end)
    series = snapshot.series("index_price", names)
    result = {name: s.loc[start:end] for name, s in series.items() if not s.loc[start:end].empty}
    return result, [name for name in names if name not in result]
