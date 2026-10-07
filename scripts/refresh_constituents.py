"""Refresh current membership from NSE: python scripts/refresh_constituents.py.

All five downloads must validate before any reference file is replaced.
Publishes dated snapshots into the one shared constituent file; preserves earlier history.
"""

import csv
import hashlib
import io
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "https://www.niftyindices.com/IndexConstituent/"
SOURCES = {
    "Nifty 50": ("ind_nifty50list.csv", 50),
    "Nifty Next 50": ("ind_niftynext50list.csv", 50),
    "Nifty Midcap 150": ("ind_niftymidcap150list.csv", 150),
    "Nifty Smallcap 250": ("ind_niftysmallcap250list.csv", 250),
    "Nifty Microcap 250": ("ind_niftymicrocap250_list.csv", 250),
}


def parse_symbols(raw: bytes, expected: int) -> list[str]:
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    if not {"Symbol", "ISIN Code", "Company Name"}.issubset(reader.fieldnames or []):
        raise ValueError("NSE response is not a constituent CSV")
    symbols = [(row.get("Symbol") or "").strip() for row in reader]
    if any(not re.fullmatch(r"[A-Z0-9&.-]+", symbol) for symbol in symbols):
        raise ValueError("Empty or invalid NSE symbol")
    if len(symbols) != len(set(symbols)):
        raise ValueError("Duplicate NSE symbols")
    # Corporate actions can temporarily add securities, including DUMMY* entries.
    # Preserve NSE's published membership rather than truncating it to the index name.
    if not expected <= len(symbols) <= expected + max(5, expected // 20):
        raise ValueError(f"Unexpected constituent count: {len(symbols)} (expected about {expected})")
    return symbols


def download(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*"})
    with urlopen(request, timeout=30) as response:
        return response.read()


def write_atomic(path: Path, content: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temp_path = Path(handle.name)
        handle.write(content)
    try:
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def refresh(root=ROOT, fetch=download, effective_date=None, effective_source=None):
    import sys

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import pandas as pd

    import market_data as md

    constituents = {}
    sources = {}
    for name, (filename, expected) in SOURCES.items():
        url = BASE_URL + filename
        raw = fetch(url)
        constituents[name] = parse_symbols(raw, expected)
        sources[name] = {
            "url": url,
            "count": len(constituents[name]),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    verified_at = datetime.now(timezone.utc).isoformat()
    observed = pd.Timestamp(verified_at).tz_convert("Asia/Kolkata").date()
    if effective_date and not effective_source:
        raise ValueError("An effective date requires its official source")
    path = Path(root) / "data" / "constituents.parquet"
    with md.publisher_lock():
        history, accepted = md._read(path)
        updated = md.append_membership_snapshot(
            history,
            constituents,
            effective_date or observed,
            verified_at,
            effective_source or "NSE current constituent CSVs",
            "official" if effective_date else "observed_not_effective",
        )
        metadata = {
            "verified_at": verified_at,
            "sources": sources,
            "membership_date_basis": "official" if effective_date else "observed_not_effective",
        }
        revision = md.write_source(updated, path, metadata, expected_revision=accepted["revision"])
    return metadata | {"revision": revision}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--effective-date")
    parser.add_argument("--effective-source")
    args = parser.parse_args()
    result = refresh(effective_date=args.effective_date, effective_source=args.effective_source)
    print(f"Published shared constituent revision {result['revision']} verified at {result['verified_at']}")
