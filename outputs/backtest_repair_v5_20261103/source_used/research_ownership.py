"""Convert confirmed merger entitlements into separate, locked research units."""

import numpy as np
import pandas as pd


def merger_claim(
    old, successor, suspension, listing, nominal_ratio, old_quote, successor_quotes, *, mark_from_listed_class
):
    suspension, listing = pd.Timestamp(suspension), pd.Timestamp(listing)
    if (
        old == successor
        or suspension >= listing
        or old_quote.name >= suspension
        or listing not in successor_quotes.index
        or successor_quotes.index.has_duplicates
    ):
        raise ValueError("Dated merger identity or listing quote does not bind")
    listing_quote = successor_quotes.loc[listing]
    values = [
        nominal_ratio,
        old_quote.adjustment_factor,
        listing_quote.adjustment_factor,
        old_quote.Close,
        listing_quote.Close,
        listing_quote.Volume,
    ]
    if not np.isfinite(values).all() or min(values) <= 0:
        raise ValueError("Positive old and executable successor quotes and unit bases required")
    ratio = nominal_ratio * old_quote.adjustment_factor / listing_quote.adjustment_factor
    locked = "CLAIM:" + old
    pending = successor_quotes.loc[(successor_quotes.index >= suspension) & (successor_quotes.index < listing)]
    marks = pd.DataFrame(columns=["Close", "High", "Volume", "is_tradable"], index=pd.DatetimeIndex([]))
    if mark_from_listed_class:
        if not np.allclose(pending.adjustment_factor, listing_quote.adjustment_factor, rtol=1e-12, atol=0):
            raise ValueError("Successor has a unit-basis seam during the lock")
        close = pending.loc[np.isfinite(pending.Close) & pending.Close.gt(0) & pending.Volume.gt(0), "Close"]
        marks = pd.DataFrame({"Close": close * ratio, "High": np.nan, "Volume": 0, "is_tradable": False})
    return (
        [
            {
                "effective_date": suspension.date().isoformat(),
                "old_symbol": old,
                "successor_symbol": locked,
                "share_ratio": 1.0,
            },
            {
                "effective_date": listing.date().isoformat(),
                "old_symbol": locked,
                "successor_symbol": successor,
                "share_ratio": ratio,
            },
        ],
        marks,
    )
