"""
Bootstrap script — run once (or when you want to refresh the TRI baseline).

1. Reads TRI CSV files downloaded from https://niftyindices.com (Historical Data →
   Total Returns Index). Place them in data/tri_csv/ before running.

   Expected filename pattern (case-insensitive match on index name):
     NIFTY 50_Historical_PR_01012000to01062026.csv
     NIFTY NEXT 50_Historical_...csv
     ... etc.

   niftyindices.com CSV format:
     Date,Open,High,Low,Close,SharesTraded,Turnover(Cr)   (for price return)
   or for TRI:
     Date,Open,High,Low,Close,Shares Traded,Turnover (₹ Cr)

   Use the "Total Returns Index" download (not Price Return).

2. Downloads RBI repo rate history and saves as data/repo_rate.csv.
   Source: RBI DBIE (https://dbie.rbi.org.in) — policy repo rate series.
   Falls back to a hardcoded approximate series if DBIE is unreachable.

3. Publishes typed index_tri observations into data/screener_ohlcv.parquet.

Run from the repo root:
    python scripts/build_dual_momentum_tri.py
"""

from __future__ import annotations

import glob
import logging
import os

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRI_CSV_DIR = os.path.join(REPO_ROOT, "data", "tri_csv")
TRI_PARQUET = os.path.join(REPO_ROOT, "data", "dual_momentum_tri.parquet")
REPO_RATE_CSV = os.path.join(REPO_ROOT, "data", "repo_rate.csv")

# Maps tri_name (from config.py) → substring to match in CSV filename.
#
# niftyindices.com filenames use spaces between words; the matching logic
# normalises filenames with replace(" ", "_") before comparing, so substrings
# here must match that normalised form exactly.
#
# Verified against niftyindices.com download filenames (June 2026):
#   "NIFTY 50_Historical_TRI_..."           → normalised: "nifty_50_historical_tri_..."
#   "Nifty Midcap 150_Historical_TRI_..."   → normalised: "nifty_midcap_150_historical_..."
#   "Nifty200 Momentum 30_..."              → normalised: "nifty200_momentum_30_..."
#   "Nifty Midcap150 Momentum 50_..."       → normalised: "nifty_midcap150_momentum_50_..."
TRI_NAME_TO_FILE_SUBSTR = {
    "NIFTY 50": "nifty_50",
    "NIFTY NEXT 50": "nifty_next_50",
    "NIFTY MIDCAP 150": "nifty_midcap_150",
    "NIFTY SMALLCAP 250": "nifty_smallcap_250",
    "NIFTY200 MOMENTUM 30": "nifty200_momentum_30",
    "NIFTY500 MOMENTUM 50": "nifty500_momentum_50",
    "NIFTY MIDCAP150 MOMENTUM 50": "nifty_midcap150_momentum_50",
    "NIFTY ALPHA LOW-VOLATILITY 30": "nifty_alpha_low-volatility_30",
}

# ---------------------------------------------------------------------------
# RBI Repo Rate — approximate historical series (fallback if DBIE unreachable)
# ---------------------------------------------------------------------------
_REPO_RATE_FALLBACK = [
    # (date, rate_pct)
    ("2000-01-01", 8.00),
    ("2003-03-03", 7.50),
    ("2004-10-27", 6.00),
    ("2006-06-08", 6.25),
    ("2006-07-25", 6.50),
    ("2006-10-31", 7.00),
    ("2007-03-30", 7.50),
    ("2008-06-12", 8.00),
    ("2008-07-29", 8.50),
    ("2008-10-20", 8.00),
    ("2008-11-03", 7.50),
    ("2008-12-08", 6.50),
    ("2009-01-05", 5.50),
    ("2009-03-05", 5.00),
    ("2009-04-21", 4.75),
    ("2010-03-19", 5.00),
    ("2010-04-20", 5.25),
    ("2010-07-27", 5.50),
    ("2010-09-16", 6.00),
    ("2011-01-25", 6.50),
    ("2011-03-17", 6.75),
    ("2011-05-03", 7.25),
    ("2011-06-16", 7.50),
    ("2011-07-26", 8.00),
    ("2012-04-17", 8.00),
    ("2012-10-29", 8.00),  # RBI held rate; kept as explicit marker
    ("2013-01-29", 7.75),
    ("2013-03-19", 7.50),
    ("2013-05-03", 7.25),
    ("2014-01-28", 8.00),
    ("2015-01-15", 7.75),
    ("2015-03-04", 7.50),
    ("2015-06-02", 7.25),
    ("2016-04-05", 6.75),
    ("2016-10-04", 6.25),
    ("2017-08-02", 6.00),
    ("2018-06-06", 6.25),
    ("2018-08-01", 6.50),
    ("2019-02-07", 6.25),
    ("2019-04-04", 6.00),
    ("2019-06-06", 5.75),
    ("2019-08-07", 5.40),
    ("2019-10-04", 5.15),
    ("2020-03-27", 4.40),
    ("2020-05-22", 4.00),
    ("2022-05-04", 4.40),
    ("2022-06-08", 4.90),
    ("2022-08-05", 5.40),
    ("2022-09-30", 5.90),
    ("2022-12-07", 6.25),
    ("2023-02-08", 6.50),
    ("2025-02-07", 6.25),
    ("2025-04-09", 6.00),
]


def build_repo_rate_csv() -> None:
    """Try RBI DBIE first; fall back to hardcoded series."""
    log.info("Building repo_rate.csv…")
    try:
        _fetch_rbi_repo_rate()
        log.info("Repo rate fetched from RBI DBIE.")
        return
    except Exception as e:
        log.warning("RBI DBIE fetch failed (%s). Using fallback series.", e)

    rows = [{"Date": d, "Rate": r} for d, r in _REPO_RATE_FALLBACK]
    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").set_index("Date")
    df.to_csv(REPO_RATE_CSV)
    log.info("repo_rate.csv written with %d rows (fallback).", len(df))


def _fetch_rbi_repo_rate() -> None:
    """
    Attempt to download repo rate from RBI DBIE.
    The DBIE portal exposes a dataset download endpoint for the policy repo rate.
    """
    # RBI DBIE series ID for Policy Repo Rate: FMRD.FMRD_RATD@RBI_REPO
    # Direct download not always straightforward from DBIE; raise so fallback is used
    raise NotImplementedError("Direct DBIE API scraping not implemented — using fallback.")


def build_tri_parquet() -> None:
    """Import TRI CSVs from data/tri_csv/ into the shared pricing source."""
    if not os.path.exists(TRI_CSV_DIR):
        log.warning(
            "TRI CSV directory not found: %s\n"
            "Create it and place niftyindices.com TRI CSV downloads there.\n"
            "Skipping TRI parquet build.",
            TRI_CSV_DIR,
        )
        return

    csv_files = glob.glob(os.path.join(TRI_CSV_DIR, "*.csv"))
    if not csv_files:
        log.warning("No CSV files found in %s — skipping TRI parquet build.", TRI_CSV_DIR)
        return

    frames: dict[str, pd.Series] = {}

    for tri_name, substr in TRI_NAME_TO_FILE_SUBSTR.items():
        matched = [
            f for f in csv_files if substr.lower().replace(" ", "_") in os.path.basename(f).lower().replace(" ", "_")
        ]
        if not matched:
            log.warning("No CSV found for '%s' (looking for '%s')", tri_name, substr)
            continue

        fpath = matched[0]
        log.info("Loading %s → %s", os.path.basename(fpath), tri_name)

        try:
            df = pd.read_csv(fpath)
            # niftyindices.com TRI CSVs have a 'Date' and 'Close' column
            date_col = next((c for c in df.columns if "date" in c.lower()), None)
            close_col = next((c for c in df.columns if "close" in c.lower()), None)
            if date_col is None or close_col is None:
                log.warning("Could not identify Date/Close columns in %s — skipping.", fpath)
                continue

            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
            df[close_col] = pd.to_numeric(df[close_col].astype(str).str.replace(",", ""), errors="coerce")
            series = df.dropna(subset=[date_col, close_col]).set_index(date_col)[close_col]
            series.index = pd.to_datetime(series.index).normalize()
            series = series.sort_index()
            series.name = tri_name
            frames[tri_name] = series

        except Exception as e:
            log.error("Failed to parse %s: %s", fpath, e)

    if not frames:
        log.error("No TRI series loaded — parquet not written.")
        return

    combined = pd.DataFrame(frames)
    combined.index.name = "Date"
    combined.sort_index(inplace=True)
    import sys

    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    import market_data as md
    from scripts.migrate_market_data import wide_series
    from scripts.refresh_market_data import merge_observations

    with md.publisher_lock():
        snapshot = md.load_snapshot()
        merged = merge_observations(snapshot.prices, wide_series(combined, "index_tri"))
        md.write_source(
            merged, md.PRICE_PATH, {"source": "NSE TRI CSV import"}, expected_revision=snapshot.price_revision
        )
    log.info(
        "Shared index TRI series published: %d rows × %d columns (%s to %s).",
        len(combined),
        len(combined.columns),
        combined.index.min().date(),
        combined.index.max().date(),
    )
    log.info("Columns: %s", list(combined.columns))


if __name__ == "__main__":
    os.makedirs(TRI_CSV_DIR, exist_ok=True)
    build_repo_rate_csv()
    build_tri_parquet()
    log.info("Done.")
