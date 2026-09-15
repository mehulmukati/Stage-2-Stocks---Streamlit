import subprocess
from pathlib import Path

import pandas as pd
import pytest

from scripts.merge_remote_parquet import MergeError, classify_history, merge_frames, run


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
        "overlap_winner": "local",
    }


def test_merge_can_prefer_remote_overlap():
    remote = _prices([("A", "2026-09-01", 100), ("B", "2026-09-01", 200)])
    local = _prices([("A", "2026-09-01", 101), ("C", "2026-09-01", 300)])

    merged, summary = merge_frames(remote, local, prefer="remote")

    assert float(merged.loc[merged["symbol"] == "A", "Close"].iloc[0]) == 100
    assert summary["overlap_winner"] == "remote"


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


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _commit(repo: Path, message: str, *paths: str) -> None:
    _git(repo, "add", "--", *paths)
    _git(repo, "commit", "-m", message)


def _write_prices(repo: Path, rows) -> None:
    target = repo / "data" / "prices.parquet"
    target.parent.mkdir(exist_ok=True)
    _prices(rows).to_parquet(target, index=False)


def _repositories(tmp_path: Path) -> tuple[Path, Path, Path]:
    remote = tmp_path / "remote.git"
    upstream = tmp_path / "upstream"
    local = tmp_path / "local"
    _git(tmp_path, "init", "--bare", str(remote))
    _git(tmp_path, "init", "-b", "main", str(upstream))
    for repo in (upstream,):
        _git(repo, "config", "user.name", "Test User")
        _git(repo, "config", "user.email", "test@example.com")
    _write_prices(upstream, [("A", "2026-09-01", 100)])
    _commit(upstream, "base", "data/prices.parquet")
    _git(upstream, "remote", "add", "origin", str(remote))
    _git(upstream, "push", "-u", "origin", "main")
    _git(tmp_path, "clone", "--branch", "main", str(remote), str(local))
    _git(local, "config", "user.name", "Test User")
    _git(local, "config", "user.email", "test@example.com")
    return remote, upstream, local


def _remote_update(upstream: Path, rows) -> None:
    _write_prices(upstream, rows)
    _commit(upstream, "remote data", "data/prices.parquet")
    _git(upstream, "push", "origin", "main")


def test_history_classifier_covers_equal_ahead_behind_and_diverged(tmp_path):
    _, upstream, local = _repositories(tmp_path)
    _git(local, "fetch", "origin", "main")
    assert classify_history(local) == "equal"

    (local / "local.txt").write_text("local", encoding="utf-8")
    _commit(local, "local work", "local.txt")
    assert classify_history(local) == "ahead"

    _remote_update(upstream, [("A", "2026-09-01", 102)])
    _git(local, "fetch", "origin", "main")
    assert classify_history(local) == "diverged"

    behind = tmp_path / "behind"
    _git(tmp_path, "clone", "--branch", "main", str(tmp_path / "remote.git"), str(behind))
    _git(behind, "fetch", "origin", "main")
    assert classify_history(behind) == "equal"
    _remote_update(upstream, [("A", "2026-09-01", 103)])
    _git(behind, "fetch", "origin", "main")
    assert classify_history(behind) == "behind"


def test_run_fast_forwards_clean_file_and_accepts_remote_overlap(tmp_path, monkeypatch):
    _, upstream, local = _repositories(tmp_path)
    _remote_update(upstream, [("A", "2026-09-01", 102), ("B", "2026-09-01", 200)])
    monkeypatch.chdir(local)

    summary = run("data/prices.parquet", "origin", "main", None)

    frame = pd.read_parquet(local / "data" / "prices.parquet")
    assert summary["overlap_winner"] == "remote"
    assert float(frame.loc[frame["symbol"] == "A", "Close"].iloc[0]) == 102
    assert _git(local, "rev-parse", "HEAD") == _git(local, "rev-parse", "origin/main")
    assert not _git(local, "status", "--short", "--", "data/prices.parquet")


def test_run_equal_and_ahead_histories_need_no_integration_commit(tmp_path, monkeypatch):
    _, _, local = _repositories(tmp_path)
    monkeypatch.chdir(local)
    equal_head = _git(local, "rev-parse", "HEAD")

    run("data/prices.parquet", "origin", "main", None)

    assert _git(local, "rev-parse", "HEAD") == equal_head
    (local / "local.txt").write_text("local", encoding="utf-8")
    _commit(local, "local work", "local.txt")
    ahead_head = _git(local, "rev-parse", "HEAD")

    run("data/prices.parquet", "origin", "main", None)

    assert _git(local, "rev-parse", "HEAD") == ahead_head
    assert classify_history(local) == "ahead"


def test_run_fast_forwards_and_preserves_dirty_local_rows(tmp_path, monkeypatch):
    _, upstream, local = _repositories(tmp_path)
    _remote_update(upstream, [("A", "2026-09-01", 102), ("B", "2026-09-01", 200)])
    _write_prices(local, [("A", "2026-09-01", 101), ("C", "2026-09-01", 300)])
    monkeypatch.chdir(local)

    summary = run("data/prices.parquet", "origin", "main", None)

    frame = pd.read_parquet(local / "data" / "prices.parquet").set_index("symbol")
    assert summary["overlap_winner"] == "local"
    assert set(frame.index) == {"A", "B", "C"}
    assert float(frame.loc["A", "Close"]) == 101
    assert (
        subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"], cwd=local, check=False).returncode
        == 0
    )
    assert _git(local, "rev-parse", "HEAD") != _git(local, "rev-parse", "origin/main")
    assert not _git(local, "status", "--short", "--", "data/prices.parquet")


def test_run_push_commits_and_publishes_dirty_local_overlay(tmp_path, monkeypatch):
    remote, upstream, local = _repositories(tmp_path)
    _remote_update(upstream, [("A", "2026-09-01", 102), ("B", "2026-09-01", 200)])
    _write_prices(local, [("A", "2026-09-01", 101), ("C", "2026-09-01", 300)])
    monkeypatch.chdir(local)

    run("data/prices.parquet", "origin", "main", None, push=True)

    published = tmp_path / "published"
    _git(tmp_path, "clone", "--branch", "main", str(remote), str(published))
    frame = pd.read_parquet(published / "data" / "prices.parquet").set_index("symbol")
    assert set(frame.index) == {"A", "B", "C"}
    assert float(frame.loc["A", "Close"]) == 101
    assert not _git(local, "status", "--short", "--", "data/prices.parquet")


def test_run_creates_real_merge_commit_for_diverged_parquet_history(tmp_path, monkeypatch):
    _, upstream, local = _repositories(tmp_path)
    _write_prices(local, [("A", "2026-09-01", 101), ("C", "2026-09-01", 300)])
    _commit(local, "local data", "data/prices.parquet")
    _remote_update(upstream, [("A", "2026-09-01", 102), ("B", "2026-09-01", 200)])
    monkeypatch.chdir(local)

    summary = run("data/prices.parquet", "origin", "main", None)

    frame = pd.read_parquet(local / "data" / "prices.parquet").set_index("symbol")
    assert summary["overlap_winner"] == "local"
    assert set(frame.index) == {"A", "B", "C"}
    assert float(frame.loc["A", "Close"]) == 101
    assert len(_git(local, "show", "-s", "--format=%P", "HEAD").split()) == 2
    assert (
        subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"], cwd=local, check=False).returncode
        == 0
    )
    assert not _git(local, "status", "--short", "--", "data/prices.parquet")


def test_run_refuses_concurrent_non_parquet_changes(tmp_path, monkeypatch):
    _, upstream, local = _repositories(tmp_path)
    shared_local = local / "shared.txt"
    shared_upstream = upstream / "shared.txt"
    shared_local.write_text("base", encoding="utf-8")
    shared_upstream.write_text("base", encoding="utf-8")
    _commit(local, "local shared base", "shared.txt")
    _commit(upstream, "remote shared base", "shared.txt")
    _git(upstream, "push", "origin", "main")
    _git(local, "fetch", "origin", "main")
    _git(local, "merge", "origin/main")
    shared_local.write_text("local", encoding="utf-8")
    _commit(local, "local shared edit", "shared.txt")
    shared_upstream.write_text("remote", encoding="utf-8")
    _commit(upstream, "remote shared edit", "shared.txt")
    _git(upstream, "push", "origin", "main")
    monkeypatch.chdir(local)

    with pytest.raises(MergeError, match="both branches changed non-parquet files"):
        run("data/prices.parquet", "origin", "main", None)

    assert subprocess.run(["git", "rev-parse", "--verify", "-q", "MERGE_HEAD"], cwd=local, check=False).returncode != 0
