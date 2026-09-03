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
