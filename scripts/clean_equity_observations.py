"""Audit or publish selective equity cleanup. No vendor calls; dry-run by default."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd

import market_data as md
from equity_data import EQUITY_POLICY_VERSION, calendar_provenance, observation_diagnostics, prepare_publication


def sha256(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def cleanup(output="outputs/equity_policy_v2", apply=False):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    snapshot = md.load_snapshot()
    original = snapshot.prices
    source_token = md.file_token(md.PRICE_PATH)
    original_sha = sha256(md.PRICE_PATH)
    planned, removed, repairs = prepare_publication(original)
    planned = md.normalize_prices(planned)
    keys = ["series_type", "symbol", "date"]
    expected = original.loc[
        ~pd.MultiIndex.from_frame(original[keys]).isin(pd.MultiIndex.from_frame(removed[keys]))
    ].copy()
    for repair in repairs:
        mask = (
            expected.series_type.eq("equity")
            & expected.symbol.eq(repair["symbol"])
            & expected.date.eq(pd.Timestamp(repair["date"]))
        )
        expected.loc[mask, "Volume"] = repair["new_volume"]
    # Every retained raw field must survive unchanged except verified repairs.
    pd.testing.assert_frame_equal(md.normalize_prices(expected)[md.PRICE_COLUMNS], planned[md.PRICE_COLUMNS])
    unchanged_files = {
        str(path.name): sha256(path) for path in md.PRICE_PATH.parent.glob("*.parquet") if path != md.PRICE_PATH
    }
    report = {
        "policy_version": EQUITY_POLICY_VERSION,
        "input_revision": snapshot.price_revision,
        "input_sha256": original_sha,
        "input_rows": len(original),
        "output_rows": len(planned),
        "removed_holiday_rows": len(removed),
        "removed_by_date": {str(day.date()): int(count) for day, count in removed.groupby("date").size().items()},
        "verified_repairs": repairs,
        "retained_zero_volume_equity_rows": int((planned.series_type.eq("equity") & planned.Volume.eq(0)).sum()),
        "diagnostics": observation_diagnostics(planned),
        "calendar": calendar_provenance(),
        "applied": False,
    }
    removed.to_csv(output / "removed_holiday_rows.csv", index=False)
    pd.DataFrame(repairs).to_csv(output / "verified_volume_repairs.csv", index=False)
    uncertain = planned[planned.series_type.eq("equity") & planned.price_status.eq("unverified")]
    uncertain.to_csv(output / "retained_uncertain_observations.csv", index=False)
    (output / "cleanup_dry_run.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    if apply:
        with md.publisher_lock():
            if md.file_token(md.PRICE_PATH) != source_token:
                raise RuntimeError("Price source changed during audit; rerun cleanup before publication")
            archive = output / "archive" / f"screener_ohlcv.{snapshot.price_revision}.parquet.before"
            archive.parent.mkdir(exist_ok=True)
            if not archive.exists():
                shutil.copy2(md.PRICE_PATH, archive)
            if sha256(archive) != original_sha:
                raise RuntimeError("Recovery archive hash differs from the audited source")
            metadata = dict(original.attrs)
            metadata["equity_cleanup"] = {
                "input_revision": snapshot.price_revision,
                "input_sha256": original_sha,
                "removed_by_date": report["removed_by_date"],
                "verified_repairs": repairs,
            }
            # The canonical writer repeats the same idempotent transformation.
            revision = md.write_source(original, md.PRICE_PATH, metadata, expected_revision=snapshot.price_revision)
            accepted = md.load_snapshot().prices
            pd.testing.assert_frame_equal(accepted[md.PRICE_COLUMNS], planned[md.PRICE_COLUMNS])
            report.update(
                applied=True, output_revision=revision, output_sha256=sha256(md.PRICE_PATH), archive=str(archive)
            )
        report["other_parquet_files_unchanged"] = all(
            sha256(md.PRICE_PATH.parent / name) == value for name, value in unchanged_files.items()
        )
        if not report["other_parquet_files_unchanged"]:
            raise RuntimeError("A parquet outside the cleanup scope changed during publication")
        (output / "cleanup_applied.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {key: value for key, value in report.items() if key not in {"verified_repairs", "calendar"}}, indent=2
        )
    )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Archive the original and atomically publish the audited cleanup"
    )
    parser.add_argument("--output", default="outputs/equity_policy_v2")
    args = parser.parse_args()
    cleanup(args.output, args.apply)
