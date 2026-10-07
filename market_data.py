"""The two authoritative market files. Readers never download or choose a fallback.

Parquet metadata carries revision/provenance; process caches follow file revisions.
Only the shared publisher and the explicit migration command may write these files.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
PRICE_PATH = ROOT / "data" / "screener_ohlcv.parquet"
MEMBERSHIP_PATH = ROOT / "data" / "constituents.parquet"
PRICE_COLUMNS = ["symbol", "date", "Open", "High", "Low", "Close", "Volume", "series_type"]
DISPLAY_NAMES = {
    "NIFTY50": "Nifty 50",
    "NIFTYNEXT50": "Nifty Next 50",
    "NIFTYMIDCAP150": "Nifty Midcap 150",
    "NIFTYSMALLCAP250": "Nifty Smallcap 250",
    "NIFTYMICROCAP250": "Nifty Microcap 250",
}
_lock = threading.RLock()
_cache: dict[str, tuple[tuple, pd.DataFrame, dict]] = {}


def index_key(name: str) -> str:
    return "".join(c for c in str(name).upper() if c.isalnum())


def display_index(name: str) -> str:
    return DISPLAY_NAMES.get(index_key(name), str(name))


def file_token(path: Path | str) -> tuple:
    path = Path(path)
    stat = path.stat()
    return str(path.resolve()), stat.st_mtime_ns, stat.st_size


def read_footer(path):
    # Explicit file lifetime is essential for atomic replacement on Windows.
    with open(path, "rb") as handle:
        return pq.read_metadata(handle)


def _read(path: Path | str) -> tuple[pd.DataFrame, dict]:
    path = Path(path)
    key = str(path.resolve())
    with _lock:
        token = file_token(path)
        cached = _cache.get(key)
        if cached and cached[0] == token:
            return cached[1], cached[2]
        for _ in range(3):
            with open(path, "rb") as handle:
                table = pq.read_table(handle)
            if file_token(path) == token:
                break
            token = file_token(path)
        else:
            raise RuntimeError(f"Market source changed repeatedly while reading {path.name}")
        meta = json.loads((table.schema.metadata or {}).get(b"market_data", b"{}"))
        if not meta.get("revision"):
            raise RuntimeError(f"{path.name} has not been migrated. Run scripts/migrate_market_data.py")
        df = table.to_pandas()
        df.attrs.update(meta)
        _cache[key] = (token, df, meta)
        return df, meta


def source_revisions() -> tuple[str, str]:
    # Read only the footers for lightweight page/session cache invalidation.
    revisions = []
    for path in (PRICE_PATH, MEMBERSHIP_PATH):
        metadata = read_footer(path).metadata or {}
        revision = json.loads(metadata.get(b"market_data", b"{}")).get("revision")
        if not revision:
            raise RuntimeError(f"{path.name} has not been migrated")
        revisions.append(revision)
    return tuple(revisions)


def revision_label(revisions: tuple[str, str]) -> str:
    return f"Prices {revisions[0][:12]} · Constituents {revisions[1][:12]}"


@dataclass(frozen=True)
class MarketSnapshot:
    prices: pd.DataFrame
    membership: pd.DataFrame
    price_revision: str
    constituent_revision: str
    as_of: pd.Timestamp | None = None

    @property
    def revisions(self) -> tuple[str, str]:
        return self.price_revision, self.constituent_revision

    def symbols(self, indices: list[str] | None = None, as_of=None) -> set[str]:
        return {s for values in self.constituents(indices, as_of).values() for s in values}

    def constituents(self, indices: list[str] | None = None, as_of=None) -> dict[str, list[str]]:
        date = pd.Timestamp(as_of) if as_of is not None else self.as_of
        selected = indices if indices is not None else list(DISPLAY_NAMES)
        return constituents_at(self.membership, date, selected)

    def ohlcv(self, symbols=None, start=None, as_of=None) -> dict[str, pd.DataFrame]:
        date = pd.Timestamp(as_of) if as_of is not None else self.as_of
        frame = self.prices[self.prices.series_type == "equity"]
        if symbols is not None:
            frame = frame[frame.symbol.isin(symbols)]
        if date is not None:
            frame = frame[frame.date <= date]
        if start is not None:
            frame = frame[frame.date >= pd.Timestamp(start)]
        result = {}
        for symbol, group in frame.groupby("symbol", sort=True):
            sub = group.set_index("date")[["Open", "High", "Low", "Close", "Volume"]].sort_index()
            sub.attrs["source_revisions"] = self.revisions
            result[symbol] = sub
        return result

    def series(self, series_type: str, symbols=None) -> dict[str, pd.Series]:
        frame = self.prices[self.prices.series_type == series_type]
        if symbols is not None:
            frame = frame[frame.symbol.isin(symbols)]
        if self.as_of is not None:
            frame = frame[frame.date <= self.as_of]
        result = {}
        for symbol, group in frame.groupby("symbol", sort=True):
            series = group.set_index("date").Close.dropna().sort_index()
            series.name = symbol
            series.attrs["source_revisions"] = self.revisions
            result[symbol] = series
        return result


def load_snapshot(as_of=None) -> MarketSnapshot:
    for _ in range(3):
        before = file_token(PRICE_PATH), file_token(MEMBERSHIP_PATH)
        prices, pm = _read(PRICE_PATH)
        membership, cm = _read(MEMBERSHIP_PATH)
        if before == (file_token(PRICE_PATH), file_token(MEMBERSHIP_PATH)):
            return MarketSnapshot(
                prices,
                membership,
                pm["revision"],
                cm["revision"],
                pd.Timestamp(as_of).normalize() if as_of is not None else None,
            )
    raise RuntimeError("Market sources changed while creating a snapshot; retry")


def constituents_at(frame: pd.DataFrame, as_of=None, indices=None) -> dict[str, list[str]]:
    selected = frame
    if as_of is not None:
        selected = selected[selected.TIME_STAMP <= pd.Timestamp(as_of)]
    if indices:
        keys = {index_key(i) for i in indices}
        selected = selected[selected.INDEX_NAME.map(index_key).isin(keys)]
    result = {}
    for name, group in selected.groupby("INDEX_NAME", sort=True):
        latest = group[group.TIME_STAMP == group.TIME_STAMP.max()]
        result[display_index(name)] = sorted(s for s in latest.SYMBOL.unique() if not s.startswith("DUMMY"))
    return result


def normalize_prices(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    if "series_type" not in frame:
        frame["series_type"] = "equity"
    missing = set(PRICE_COLUMNS) - set(frame)
    if missing:
        raise ValueError(f"Missing price columns: {sorted(missing)}")
    frame = frame[PRICE_COLUMNS]
    frame["date"] = pd.to_datetime(frame.date, errors="raise").dt.normalize()
    for column in ("symbol", "series_type"):
        if frame[column].isna().any() or frame[column].astype(str).str.strip().eq("").any():
            raise ValueError(f"Invalid {column}")
        frame[column] = frame[column].astype(str)
    frame["symbol"] = frame.symbol.str.strip()
    if frame.date.isna().any() or frame.duplicated(["series_type", "symbol", "date"]).any():
        raise ValueError("Missing dates or duplicate price observations")
    for column in ("Open", "High", "Low", "Close", "Volume"):
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
        if np.isinf(frame[column].to_numpy(dtype=float)).any():
            raise ValueError(f"Infinite {column}")
    if frame.Close.isna().any() or frame.Close.le(0).any():
        raise ValueError("Close must be finite and positive")
    equity = frame[frame.series_type == "equity"]
    # Retain legacy close-only history without inventing candle values. Consumers
    # requiring complete candles must exclude/report these rows explicitly.
    if equity[["Open", "High", "Low"]].le(0).any().any() or frame.Volume.dropna().lt(0).any():
        raise ValueError("Invalid OHLCV values")
    # Existing vendors have small OHLC rounding differences; reject gross inversions.
    if (equity.High + 0.02 < equity.Low).any():
        raise ValueError("High below Low")
    return frame.sort_values(["series_type", "symbol", "date"]).reset_index(drop=True)


def normalize_membership(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    required = {"INDEX_NAME", "TIME_STAMP", "SYMBOL"}
    if not required.issubset(frame):
        raise ValueError("Incomplete membership schema")
    if frame[list(required)].isna().any().any():
        raise ValueError("Missing membership identity/date")
    frame["TIME_STAMP"] = pd.to_datetime(frame.TIME_STAMP, errors="raise").dt.normalize()
    frame["INDEX_NAME"] = frame.INDEX_NAME.map(lambda s: index_key(s))
    frame["SYMBOL"] = frame.SYMBOL.astype(str).str.strip()
    if frame.SYMBOL.eq("").any() or frame.INDEX_NAME.eq("").any():
        raise ValueError("Empty membership identity")
    return (
        frame.drop_duplicates(["INDEX_NAME", "TIME_STAMP", "SYMBOL"], keep="last")
        .sort_values(["INDEX_NAME", "TIME_STAMP", "SYMBOL"])
        .reset_index(drop=True)
    )


@contextmanager
def publisher_lock(timeout=30):
    """Serialize explicit publishers across processes; never silently steal a lock."""
    path = PRICE_PATH.parent / ".market-publisher.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Another publisher holds {path}; retry or inspect the lock owner")
            time.sleep(0.1)
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


def write_source(frame: pd.DataFrame, path: Path | str, metadata=None, expected_revision=None) -> str:
    """Atomic replacement with optimistic conflict detection (caller owns publisher lock)."""
    path = Path(path)
    if expected_revision is not None:
        existing = read_footer(path).metadata or {}
        accepted = json.loads(existing.get(b"market_data", b"{}")).get("revision")
        if accepted != expected_revision:
            raise RuntimeError(f"Concurrent update of {path.name}; reload before publishing")
    frame = normalize_prices(frame) if path.name == PRICE_PATH.name else normalize_membership(frame)
    revision = uuid.uuid4().hex
    meta = dict(metadata or {})
    meta.update(
        revision=revision,
        schema_version=1,
        generated_at=datetime.now(timezone.utc).isoformat(),
        rows=len(frame),
    )
    table = pa.Table.from_pandas(frame, preserve_index=False)
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"market_data": json.dumps(meta).encode()})
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, suffix=".parquet.tmp")
    os.close(fd)
    try:
        pq.write_table(table, temporary, compression="zstd")
        # Read back before exposing the revision.
        if read_footer(temporary).num_rows != len(frame):
            raise RuntimeError("Published row count differs")
        # Arrow's metadata readers may hold Windows handles until cyclic GC.
        # Close those before replacement; retry transient sync/antivirus locks.
        import gc

        gc.collect()
        for attempt in range(6):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                gc.collect()
                time.sleep(0.5)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    with _lock:
        _cache.pop(str(path.resolve()), None)
    return revision


def append_membership_snapshot(
    history, snapshot: dict[str, list[str]], effective_date, verified_at, source, date_basis="official"
) -> pd.DataFrame:
    """Append complete per-index snapshots; never replace earlier membership dates."""
    date = pd.Timestamp(effective_date).normalize()
    verified = pd.Timestamp(verified_at)
    if verified.tzinfo is None:
        raise ValueError("Verification time must include timezone")
    if date > verified.tz_convert("Asia/Kolkata").tz_localize(None).normalize():
        raise ValueError("Cannot publish unobserved future membership from a current CSV")
    history = normalize_membership(history)
    additions = []
    for name, symbols in snapshot.items():
        key = index_key(name)
        latest = history[history.INDEX_NAME == key]
        if not latest.empty and date < latest.TIME_STAMP.max():
            raise ValueError(f"Cannot append {name} before its latest snapshot")
        previous = constituents_at(history, date, [name]).get(display_index(name), [])
        clean = sorted(set(s for s in symbols if not s.startswith("DUMMY")))
        # An unchanged observation updates file verification metadata, not historical rows.
        if clean == previous:
            continue
        if not latest.empty and date == latest.TIME_STAMP.max():
            raise ValueError(f"Conflicting membership snapshot for {name} on {date.date()}")
        additions.extend(
            dict(
                INDEX_NAME=key,
                TIME_STAMP=date,
                SYMBOL=s,
                VERIFIED_AT=verified.isoformat(),
                SOURCE=source,
                DATE_BASIS=date_basis,
            )
            for s in clean
        )
    return (
        normalize_membership(pd.concat([history, pd.DataFrame(additions)], ignore_index=True)) if additions else history
    )
