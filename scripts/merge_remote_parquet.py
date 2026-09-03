"""Merge a working-tree Parquet file with the same file from a remote Git branch.

The command fetches the requested remote branch, reads the remote file directly
from ``FETCH_HEAD``, merges rows by their logical key, and atomically replaces
the local file. Local rows win when the same key exists in both versions.

This intentionally does not merge the Git branch, stage files, commit, or push.

Examples
--------
python scripts/merge_remote_parquet.py data/screener_ohlcv.parquet
python scripts/merge_remote_parquet.py data/benchmarks.parquet --keys date
python scripts/merge_remote_parquet.py data/custom.parquet --remote upstream --branch main --keys symbol date
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


class MergeError(RuntimeError):
    """A condition that prevents a safe file-level merge."""


KEY_CANDIDATES = (
    ("symbol", "date"),
    ("INDEX_NAME", "TIME_STAMP", "SYMBOL"),
    ("date",),
)


def _git(repo: Path, *args: str, capture_bytes: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=False,
        capture_output=True,
        text=not capture_bytes,
    )


def _git_text(repo: Path, *args: str) -> str:
    result = _git(repo, *args)
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise MergeError(f"git {' '.join(args)} failed: {detail or 'unknown Git error'}")
    return result.stdout.strip()


def _repo_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise MergeError("the command must be run inside a Git repository")
    return Path(result.stdout.strip()).resolve()


def _resolve_file(repo: Path, value: str) -> tuple[Path, str]:
    candidate = Path(value)
    path = (candidate if candidate.is_absolute() else repo / candidate).resolve()
    try:
        relative = path.relative_to(repo).as_posix()
    except ValueError as exc:
        raise MergeError(f"file must be inside the repository: {path}") from exc
    if path.suffix.lower() != ".parquet":
        raise MergeError(
            f"unsupported file type {path.suffix or '(none)'}; only Parquet files can be merged safely by rows"
        )
    if not path.is_file():
        raise MergeError(f"local file does not exist: {relative}")
    tracked = _git(repo, "ls-files", "--error-unmatch", "--", relative)
    if tracked.returncode:
        raise MergeError(f"local file is not tracked by Git: {relative}")
    if _git(repo, "ls-files", "-u", "--", relative).stdout.strip():
        raise MergeError(f"file already has unresolved Git conflicts: {relative}")
    if _git(repo, "diff", "--cached", "--quiet", "--", relative).returncode == 1:
        raise MergeError(f"file has staged changes; unstage them before merging: {relative}")
    return path, relative


def _infer_keys(columns: pd.Index) -> list[str]:
    available = set(columns)
    for candidate in KEY_CANDIDATES:
        if set(candidate).issubset(available):
            return list(candidate)
    raise MergeError("could not infer row keys; pass them explicitly with --keys KEY [KEY ...]")


def _validate_frame(frame: pd.DataFrame, label: str, keys: list[str]) -> None:
    missing = [key for key in keys if key not in frame.columns]
    if missing:
        raise MergeError(f"{label} file is missing key columns: {', '.join(missing)}")
    if frame[keys].isna().any(axis=None):
        raise MergeError(f"{label} file contains null values in key columns: {', '.join(keys)}")
    duplicates = int(frame.duplicated(keys).sum())
    if duplicates:
        raise MergeError(f"{label} file contains {duplicates} duplicate key rows")


def merge_frames(remote: pd.DataFrame, local: pd.DataFrame, keys: list[str] | None = None) -> tuple[pd.DataFrame, dict]:
    """Return a remote+local row merge and summary; local duplicate keys win."""
    if set(remote.columns) != set(local.columns):
        remote_only = sorted(set(remote.columns) - set(local.columns))
        local_only = sorted(set(local.columns) - set(remote.columns))
        raise MergeError(f"schema mismatch (remote-only columns={remote_only}, local-only columns={local_only})")

    local = local.loc[:, remote.columns]
    merge_keys = list(keys) if keys else _infer_keys(remote.columns)
    _validate_frame(remote, "remote", merge_keys)
    _validate_frame(local, "local", merge_keys)

    remote_index = pd.MultiIndex.from_frame(remote[merge_keys])
    local_index = pd.MultiIndex.from_frame(local[merge_keys])
    remote_only_count = len(remote_index.difference(local_index))
    local_only_count = len(local_index.difference(remote_index))
    overlap_count = len(remote_index.intersection(local_index))

    merged = pd.concat([remote, local], ignore_index=True)
    merged = merged.drop_duplicates(merge_keys, keep="last").sort_values(merge_keys).reset_index(drop=True)
    _validate_frame(merged, "merged", merge_keys)
    summary = {
        "keys": merge_keys,
        "remote_rows": len(remote),
        "local_rows": len(local),
        "remote_only_rows": remote_only_count,
        "local_only_rows": local_only_count,
        "overlap_rows": overlap_count,
        "merged_rows": len(merged),
    }
    return merged, summary


def _read_remote_file(repo: Path, relative: str) -> tuple[pd.DataFrame, Path]:
    handle = tempfile.NamedTemporaryFile(dir=repo, prefix=".remote-parquet-", suffix=".parquet", delete=False)
    temp_path = Path(handle.name)
    try:
        result = subprocess.run(
            ["git", "show", f"FETCH_HEAD:{relative}"],
            cwd=repo,
            check=False,
            stdout=handle,
            stderr=subprocess.PIPE,
        )
    finally:
        handle.close()
    if result.returncode:
        temp_path.unlink(missing_ok=True)
        detail = result.stderr.decode(errors="replace").strip()
        raise MergeError(f"remote branch does not contain {relative}: {detail or 'git show failed'}")
    try:
        return pd.read_parquet(temp_path), temp_path
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        raise MergeError(f"remote {relative} is not a readable Parquet file: {exc}") from exc


def _write_atomic(frame: pd.DataFrame, target: Path) -> None:
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False) as fh:
        temp_path = Path(fh.name)
    try:
        frame.to_parquet(temp_path, index=False, compression="snappy")
        check = pd.read_parquet(temp_path)
        if len(check) != len(frame) or list(check.columns) != list(frame.columns):
            raise MergeError("written Parquet failed row-count or schema verification")
        os.replace(temp_path, target)
    finally:
        temp_path.unlink(missing_ok=True)


def run(file: str, remote: str, branch: str | None, keys: list[str] | None) -> dict:
    repo = _repo_root()
    local_path, relative = _resolve_file(repo, file)
    remote_url = _git_text(repo, "remote", "get-url", remote)
    selected_branch = branch or _git_text(repo, "branch", "--show-current")
    if not selected_branch:
        raise MergeError("cannot infer a branch in detached HEAD state; pass --branch")

    print(f"Repository : {repo}")
    print(f"Local file : {relative}")
    print(f"Remote     : {remote} ({remote_url})")
    print(f"Branch     : {selected_branch}")
    print(f"Fetching {remote}/{selected_branch}...")
    _git_text(repo, "fetch", remote, selected_branch)

    remote_frame, remote_temp = _read_remote_file(repo, relative)
    try:
        local_frame = pd.read_parquet(local_path)
        merged, summary = merge_frames(remote_frame, local_frame, keys)
        _write_atomic(merged, local_path)
    except MergeError:
        raise
    except Exception as exc:
        raise MergeError(f"could not merge {relative}: {exc}") from exc
    finally:
        remote_temp.unlink(missing_ok=True)

    print(f"Keys       : {', '.join(summary['keys'])}")
    print(f"Remote rows: {summary['remote_rows']:,} ({summary['remote_only_rows']:,} remote-only)")
    print(f"Local rows : {summary['local_rows']:,} ({summary['local_only_rows']:,} local-only)")
    print(f"Overlap    : {summary['overlap_rows']:,} (local rows retained)")
    print(f"Merged rows: {summary['merged_rows']:,}")
    status = _git_text(repo, "status", "--short", "--", relative) or "clean (merged content matches HEAD)"
    print(f"Git status : {status}")
    print("No commit was created and nothing was pushed.")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file", help="Repository-relative or absolute path to a tracked Parquet file")
    parser.add_argument("--remote", default="origin", help="Git remote to fetch (default: origin)")
    parser.add_argument("--branch", help="Remote branch (default: current local branch)")
    parser.add_argument("--keys", nargs="+", help="Logical row-key columns (default: infer known schemas)")
    args = parser.parse_args()
    try:
        run(args.file, args.remote, args.branch, args.keys)
        return 0
    except MergeError as exc:
        print(f"Cannot merge: {exc}", file=sys.stderr)
        print("No commit was created and nothing was pushed.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
