"""Parse staged vendor evidence without publishing or inventing price bars."""

from __future__ import annotations

import math

import pandas as pd


def parse_vendor_chart(payload, expected_symbol):
    chart = payload.get("chart", {})
    if chart.get("error") or not chart.get("result"):
        raise ValueError("Vendor history unavailable")
    result = chart["result"]
    if len(result) != 1:
        raise ValueError("Ambiguous vendor result")
    result = result[0]
    meta = result.get("meta", {})
    if meta.get("symbol") != expected_symbol:
        raise ValueError("Vendor symbol mismatch")
    if meta.get("currency") != "INR" or meta.get("exchangeTimezoneName") != "Asia/Kolkata":
        raise ValueError("Unsupported vendor currency/calendar")
    stamps = result.get("timestamp", [])
    indicators = result.get("indicators", {})
    quotes = indicators.get("quote", [])
    adjusted = indicators.get("adjclose", [])
    if len(quotes) != 1 or len(adjusted) != 1 or not stamps:
        raise ValueError("Missing vendor quote/adjustment evidence")
    quote = quotes[0]
    adj = adjusted[0].get("adjclose", [])
    if any(len(quote.get(key, [])) != len(stamps) for key in ["open", "high", "low", "close", "volume"]):
        raise ValueError("Truncated vendor quote arrays")
    if len(adj) != len(stamps):
        raise ValueError("Truncated vendor adjustment array")
    dates = pd.to_datetime(stamps, unit="s", utc=True).tz_convert("Asia/Kolkata").tz_localize(None).normalize()
    if dates.duplicated().any():
        raise ValueError("Duplicate vendor session")
    rows = []
    for i, day in enumerate(dates):
        values = [quote[k][i] for k in ["open", "high", "low", "close"]] + [adj[i]]
        valid = all(v is not None and math.isfinite(v) and v > 0 for v in values)
        volume = quote["volume"][i]
        valid = valid and volume is not None and math.isfinite(volume) and volume >= 0
        if valid:
            op, hi, lo, cl, ac = values
            valid = lo <= min(op, cl) * 1.001 and hi >= max(op, cl) / 1.001 and lo <= hi
        factor = adj[i] / quote["close"][i] if valid else None
        row = dict(
            vendor_symbol=expected_symbol,
            date=day,
            raw_vendor_close=quote["close"][i],
            adjusted_close=adj[i],
            adjustment_factor=factor,
            reported_volume=volume,
            status="observed" if valid else "unsupported_vendor_bar",
        )
        for key in ["open", "high", "low", "close"]:
            row["adjusted_" + key] = quote[key][i] * factor if valid else None
        rows.append(row)
    return pd.DataFrame(rows), result.get("events", {})
