import pandas as pd
import pytest

from scripts.merge_remote_parquet import MergeError, merge_frames


def _prices(rows):
    return pd.DataFrame(rows, columns=["symbol", "date", "Close"]).assign(
        date=lambda frame: pd.to_datetime(frame["date"]),
        Close=lambda frame: frame["Close"].astype("float32"),
    )


def test_merge_adds_remote_rows_and_keeps_local_overlap():
    remote = _prices([("A", "2026-09-01", 100), ("B", "2026-09-01", 200)])
    local = _prices([("A", "2026-09-01", 101), ("C", "2026-09-01", 300)])

    merged, summary = merge_frames(remote, local)

    assert list(merged["symbol"]) == ["A", "B", "C"]
    assert float(merged.loc[merged["symbol"] == "A", "Close"].iloc[0]) == 101
    assert summary == {
        "keys": ["symbol", "date"],
        "remote_rows": 2,
        "local_rows": 2,
        "remote_only_rows": 1,
        "local_only_rows": 1,
        "overlap_rows": 1,
        "merged_rows": 3,
    }


def test_merge_rejects_schema_mismatch():
    remote = _prices([("A", "2026-09-01", 100)])
    local = remote.rename(columns={"Close": "AdjustedClose"})

    with pytest.raises(MergeError, match="schema mismatch"):
        merge_frames(remote, local)


def test_merge_rejects_duplicate_keys():
    remote = _prices([("A", "2026-09-01", 100), ("A", "2026-09-01", 101)])
    local = _prices([("A", "2026-09-01", 100)])

    with pytest.raises(MergeError, match="duplicate key rows"):
        merge_frames(remote, local)


def test_merge_requires_explicit_keys_for_unknown_schema():
    remote = pd.DataFrame({"id": [1], "value": ["remote"]})
    local = pd.DataFrame({"id": [2], "value": ["local"]})

    with pytest.raises(MergeError, match="could not infer row keys"):
        merge_frames(remote, local)

    merged, summary = merge_frames(remote, local, ["id"])
    assert list(merged["id"]) == [1, 2]
    assert summary["merged_rows"] == 2
