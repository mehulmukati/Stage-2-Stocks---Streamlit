import hashlib
import json
from datetime import datetime

import pandas as pd

import data


def _prepare_reference_files(tmp_path, monkeypatch, compositions: pd.DataFrame) -> None:
    monkeypatch.setattr(data, "__file__", str(tmp_path / "data.py"))
    (tmp_path / "data").mkdir()
    (tmp_path / "constituents.json").write_text('{"NIFTY 50": ["A"]}', encoding="utf-8")
    (tmp_path / "nse_holidays.json").write_text("{}", encoding="utf-8")
    compositions.to_parquet(tmp_path / "data" / "compositions.parquet", index=False)
    monkeypatch.setattr(data, "load_nse_holidays", lambda: frozenset({"2026-01-26"}))
    fixed_datetime = type(
        "FixedDateTime",
        (datetime,),
        {"now": classmethod(lambda cls, tz=None: cls(2026, 9, 2))},
    )
    monkeypatch.setattr(data, "datetime", fixed_datetime)


def test_old_composition_effective_date_is_not_treated_as_stale(tmp_path, monkeypatch):
    _prepare_reference_files(
        tmp_path,
        monkeypatch,
        pd.DataFrame(
            {
                "INDEX_NAME": ["NIFTY 50"],
                "TIME_STAMP": ["2026-07-31"],
                "SYMBOL": ["A"],
            }
        ),
    )

    issues = data.check_data_freshness()

    assert not any("compositions.parquet" in message for _, message in issues)


def test_invalid_composition_history_is_blocking(tmp_path, monkeypatch):
    _prepare_reference_files(
        tmp_path,
        monkeypatch,
        pd.DataFrame(
            {
                "INDEX_NAME": ["NIFTY 50"],
                "TIME_STAMP": [None],
                "SYMBOL": ["A"],
            }
        ),
    )

    issues = data.check_data_freshness()

    assert any(level == "error" and "compositions.parquet" in message for level, message in issues)


def _prepare_verified_snapshot(tmp_path, monkeypatch, verified_at):
    _prepare_reference_files(
        tmp_path,
        monkeypatch,
        pd.DataFrame(
            {
                "INDEX_NAME": ["NIFTY 50"],
                "TIME_STAMP": ["2026-07-31"],
                "SYMBOL": ["A"],
            }
        ),
    )
    snapshot = json.loads((tmp_path / "constituents.json").read_text())
    digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
    (tmp_path / "constituents.meta.json").write_text(
        json.dumps(
            {
                "verified_at": verified_at,
                "constituents_sha256": digest,
            }
        )
    )


def test_recent_file_does_not_hide_stale_verification(tmp_path, monkeypatch):
    _prepare_verified_snapshot(tmp_path, monkeypatch, "2026-07-01T00:00:00+00:00")
    assert any(
        "constituents.json" in message and "63 days old" in message for _, message in data.check_data_freshness()
    )


def test_verified_snapshot_clears_blocker(tmp_path, monkeypatch):
    _prepare_verified_snapshot(tmp_path, monkeypatch, "2026-09-02T00:00:00+00:00")
    assert data.check_data_freshness() == []


def test_changed_snapshot_requires_reverification(tmp_path, monkeypatch):
    _prepare_verified_snapshot(tmp_path, monkeypatch, "2026-09-02T00:00:00+00:00")
    (tmp_path / "constituents.json").write_text('{"NIFTY 50": ["B"]}')
    assert any(level == "error" and "verification failed" in message for level, message in data.check_data_freshness())


def test_running_process_reads_refreshed_membership(tmp_path, monkeypatch):
    _prepare_verified_snapshot(tmp_path, monkeypatch, "2026-09-02T00:00:00+00:00")
    assert data._load_constituents() == {"NIFTY 50": ["A"]}
    (tmp_path / "constituents.json").write_text('{"NIFTY 50": ["B"]}')
    assert data._load_constituents() == {"NIFTY 50": ["B"]}


def test_checkout_line_endings_do_not_invalidate_verification(tmp_path, monkeypatch):
    _prepare_verified_snapshot(tmp_path, monkeypatch, "2026-09-02T00:00:00+00:00")
    (tmp_path / "constituents.json").write_bytes(b'{\r\n  "NIFTY 50": ["A"]\r\n}\r\n')
    assert data.check_data_freshness() == []


def test_tradable_universe_excludes_nse_placeholders_without_changing_snapshot(tmp_path, monkeypatch):
    _prepare_verified_snapshot(tmp_path, monkeypatch, "2026-09-02T00:00:00+00:00")
    snapshot = {"Nifty Smallcap 250": ["HEGAM", "DUMMYHEG"], "Nifty Microcap 250": ["A", "DUMMYINGL1"]}
    path = tmp_path / "constituents.json"
    path.write_text(json.dumps(snapshot))
    assert data._load_constituents() == {"Nifty Smallcap 250": ["HEGAM"], "Nifty Microcap 250": ["A"]}
    assert json.loads(path.read_text()) == snapshot
