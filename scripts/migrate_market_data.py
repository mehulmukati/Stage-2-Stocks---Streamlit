"""One-time, offline consolidation. Legacy files are migration inputs only.

Overlap policy: the legacy replay baseline+delta wins; screener fills missing keys.
Every conflicting close is recorded in the migration report before publication.
No constituent changes are backdated without an explicit sourced effective date.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import market_data as md


def wide_series(frame, series_type):
    frame = frame.copy()
    if "date" not in frame:
        frame.index.name = "date"
        frame = frame.reset_index()
    if "Date" in frame:
        frame = frame.rename(columns={"Date": "date"})
    result = frame.melt(id_vars="date", var_name="symbol", value_name="Close").dropna(subset=["Close"])
    result["series_type"] = series_type
    for column in ("Open", "High", "Low", "Volume"):
        result[column] = float("nan")
    return result[md.PRICE_COLUMNS]


def build_prices(root):
    frames = []
    report = {"inputs": [], "overlap_policy": "replay baseline then delta wins; screener fills missing keys"}
    # Lowest priority first. Never truncate the historical union.
    for name in ("screener_ohlcv.parquet", "backtest_history.parquet", "backtest_delta.parquet"):
        path = root / "data" / name
        if path.exists():
            f = pd.read_parquet(path)
            f["date"] = pd.to_datetime(f.date)
            if "series_type" not in f:
                f["series_type"] = "equity"
            frames.append(f)
            report["inputs"].append({"file": name, "rows": len(f), "latest": str(f.date.max())})
    if not frames:
        raise RuntimeError("No legacy pricing input exists")
    combined = pd.concat(frames, ignore_index=True)
    keys = ["series_type", "symbol", "date"]
    conflicts = combined.groupby(keys).Close.agg(["min", "max"])
    conflicts = conflicts[(conflicts["max"] - conflicts["min"]).abs() > 0.01]
    report["conflicting_close_keys"] = len(conflicts)
    report["conflicting_symbols"] = sorted(set(conflicts.index.get_level_values("symbol")))
    combined = combined.drop_duplicates(keys, keep="last").reset_index(drop=True)
    # Fill missing candle fields only from an observation with the same accepted close.
    for candidate in reversed(frames):
        candidate = candidate.drop_duplicates(keys, keep="last")
        matched = combined.merge(candidate, on=keys, suffixes=("", "_candidate"), how="left")
        agrees = (matched.Close - matched.Close_candidate).abs().le(0.01)
        for column in ("Open", "High", "Low", "Volume"):
            values = matched[column + "_candidate"]
            combined.loc[agrees & combined[column].isna(), column] = values[agrees & combined[column].isna()]
    incomplete = combined[
        (combined.series_type == "equity") & combined[["Open", "High", "Low", "Volume"]].isna().any(axis=1)
    ]
    report["incomplete_candle_rows"] = len(incomplete)
    report["incomplete_candle_symbols"] = sorted(incomplete.symbol.unique())
    for names, kind in [
        (("benchmarks.parquet", "benchmarks_delta.parquet"), "index_price"),
        (("dual_momentum_tri.parquet",), "index_tri"),
    ]:
        items = [
            wide_series(pd.read_parquet(root / "data" / name), kind)
            for name in names
            if (root / "data" / name).exists()
        ]
        if items:
            combined = pd.concat([combined, *items], ignore_index=True).drop_duplicates(keys, keep="last")
    overlay = root / "data" / "index_overlay_prices.parquet"
    if overlay.exists():
        f = pd.read_parquet(overlay).rename(columns={"index": "symbol"})
        f["series_type"] = "index_price"
        for col in ("Open", "High", "Low", "Volume"):
            f[col] = float("nan")
        # Benchmarks are the canonical overlapping index-price series.
        combined = pd.concat([f, combined], ignore_index=True).drop_duplicates(keys, keep="last")
    # Persist available raw TRI inputs directly; no ETF/TRI value substitution.
    for path in sorted((root / "data" / "tri_csv").glob("*.csv")):
        # Imported separately by build_dual_momentum_tri.py after migration.
        report.setdefault("tri_csv_inputs", []).append(path.name)
    combined["date"] = pd.to_datetime(combined.date).dt.normalize()
    combined = combined.drop_duplicates(keys, keep="last")
    for col in ("Open", "High", "Low", "Close"):
        combined[col] = combined[col].astype("float32")
    return md.normalize_prices(combined), report, conflicts.reset_index()


def migrate(root, effective_date=None, effective_source=None):
    root = Path(root)

    existing = root / "data" / "screener_ohlcv.parquet"
    if existing.exists() and b"market_data" in (md.read_footer(existing).metadata or {}):
        raise RuntimeError("Shared pricing is already migrated; use the shared publisher for subsequent updates")
    prices, report, conflicts = build_prices(root)
    history = pd.read_parquet(root / "data" / "compositions.parquet")
    history["TIME_STAMP"] = pd.to_datetime(history.TIME_STAMP)
    history["DATE_BASIS"] = history.get("CONFIDENCE", pd.Series("legacy", index=history.index)).fillna("legacy")
    snapshot = json.loads((root / "constituents.json").read_text())
    metadata = json.loads((root / "constituents.meta.json").read_text())
    import hashlib

    digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
    if digest != metadata["constituents_sha256"]:
        raise RuntimeError("Current constituent snapshot does not match its verification hash")
    observed = pd.Timestamp(metadata["verified_at"]).tz_convert("Asia/Kolkata").date()
    if effective_date and not effective_source:
        raise ValueError("An effective date requires its source URL/document")
    history = md.append_membership_snapshot(
        history,
        snapshot,
        effective_date or observed,
        metadata["verified_at"],
        effective_source or "legacy verified current snapshot",
        "official" if effective_date else "observed_not_effective",
    )
    report["membership_date_basis"] = "official" if effective_date else "observed_not_effective"
    report["membership_snapshot_date"] = str(effective_date or observed)
    # Recovery copies never enter the active application read path.
    out = root / "outputs" / "market_data_migration"
    out.mkdir(parents=True, exist_ok=True)
    conflicts.to_csv(out / "price_conflicts.csv", index=False)
    import shutil

    for path in (root / "data" / "screener_ohlcv.parquet", root / "data" / "constituents.parquet"):
        if path.exists() and not (out / (path.name + ".before")).exists():
            shutil.copy2(path, out / (path.name + ".before"))
    with md.publisher_lock():
        report["price_revision"] = md.write_source(
            prices,
            root / "data" / "screener_ohlcv.parquet",
            {
                "migration": report["overlap_policy"],
                "adjustment_policy": "yahoo_auto_adjust",
                "source": "legacy consolidation",
                "history_rebuild_required": report["conflicting_symbols"],
            },
        )
        report["constituent_revision"] = md.write_source(
            history,
            root / "data" / "constituents.parquet",
            {
                "verified_at": metadata["verified_at"],
                "sources": metadata["sources"],
                "membership_date_basis": report["membership_date_basis"],
            },
        )
    report.update(
        price_rows=len(prices),
        membership_rows=len(history),
        equity_symbols=prices[prices.series_type == "equity"].symbol.nunique(),
    )
    (out / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--effective-date")
    parser.add_argument("--effective-source")
    args = parser.parse_args()
    migrate(md.ROOT, args.effective_date, args.effective_source)
