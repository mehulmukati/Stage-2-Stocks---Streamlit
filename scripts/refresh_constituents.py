"""Refresh current membership from NSE: python scripts/refresh_constituents.py.

All five downloads must validate before any reference file is replaced.
Historical compositions are deliberately not changed by a current snapshot.
"""

import csv
import hashlib
import io
import json
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


def refresh(root: Path = ROOT, fetch=download) -> dict:
    constituents = {}
    sources = {}
    for name, (filename, expected) in SOURCES.items():
        url = BASE_URL + filename
        raw = fetch(url)
        constituents[name] = parse_symbols(raw, expected)
        sources[name] = {"url": url, "count": len(constituents[name]), "sha256": hashlib.sha256(raw).hexdigest()}
    content = (json.dumps(constituents, indent=2) + "\n").encode("utf-8")
    metadata = {
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "constituents_sha256": hashlib.sha256(json.dumps(constituents, sort_keys=True).encode()).hexdigest(),
        "sources": sources,
    }
    # A crash between writes leaves a hash mismatch, which blocks LiveSignal safely.
    write_atomic(root / "constituents.json", content)
    write_atomic(root / "constituents.meta.json", (json.dumps(metadata, indent=2) + "\n").encode("utf-8"))
    return metadata


if __name__ == "__main__":
    result = refresh()
    print(f"Verified from NSE at {result['verified_at']}")
    for index, source in result["sources"].items():
        print(f"  {index}: {source['count']} constituents")
