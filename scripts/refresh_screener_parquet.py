"""Compatibility command for the shared full-history price publisher."""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.refresh_market_data import refresh_prices


def _validate_current_tradability(df: pd.DataFrame, symbols: list[str], max_stale_sessions: int = 3) -> None:
    """Fail closed when a current constituent lacks a recent positive-volume bar."""
    dates = pd.DatetimeIndex(pd.to_datetime(df["date"]).dropna().unique()).sort_values()
    if dates.empty:
        raise RuntimeError("Screener refresh produced no trading dates")
    cutoff = dates[max(0, len(dates) - max_stale_sessions - 1)]
    valid = df[(pd.to_numeric(df["Close"], errors="coerce") > 0) & (pd.to_numeric(df["Volume"], errors="coerce") > 0)]
    last_by_symbol = pd.to_datetime(valid["date"]).groupby(valid["symbol"]).max()
    stale = sorted(
        symbol
        for symbol in symbols
        if symbol not in last_by_symbol.index or pd.Timestamp(last_by_symbol[symbol]) < cutoff
    )
    if stale:
        sample = ", ".join(stale[:20])
        suffix = f" (+{len(stale) - 20} more)" if len(stale) > 20 else ""
        raise RuntimeError(
            f"Current-constituent tradability validation failed at cutoff {cutoff.date()}: {sample}{suffix}. "
            "Refresh constituents or register the relevant corporate action before publishing data."
        )


# ──────────────────────────────────────────────
# Download helpers
# ──────────────────────────────────────────────


def main(force_full=False):
    return refresh_prices(force_full=force_full)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full", action="store_true", help="Refresh full history while retaining accepted observations"
    )
    main(parser.parse_args().full)
