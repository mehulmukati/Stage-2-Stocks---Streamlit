"""Reconcile a Parquet file and its Git history with a remote branch.

The command fetches the requested remote branch, reads the remote file directly
from ``FETCH_HEAD`` and merges rows by their logical key. It then inspects the
commit graph: behind branches are fast-forwarded and diverged branches receive a
real merge commit, so the next push is fast-forwardable. A resulting local
overlay is committed with only the requested parquet staged. Local overlap wins
only when the working file or local branch changed the parquet; otherwise the
clean, newer remote file wins.

The command never pushes unless ``--push`` is supplied. Use ``--file-only`` for
the original behavior (rewrite the file without integrating Git history).

Examples
--------
python scripts/merge_remote_parquet.py data/screener_ohlcv.parquet
python scripts/merge_remote_parquet.py data/screener_ohlcv.parquet --push
python scripts/merge_remote_parquet.py data/screener_ohlcv.parquet --file-only
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
    """A condition that prevents a safe parquet or history merge."""


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


def merge_frames(
    remote: pd.DataFrame,
    local: pd.DataFrame,
    keys: list[str] | None = None,
    prefer: str = "local",
) -> tuple[pd.DataFrame, dict]:
    """Return a keyed row merge; ``prefer`` selects the winner on overlap."""
    if prefer not in {"local", "remote"}:
        raise MergeError("prefer must be either 'local' or 'remote'")
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

    frames = [remote, local] if prefer == "local" else [local, remote]
    merged = pd.concat(frames, ignore_index=True)
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
        "overlap_winner": prefer,
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


def _write_bytes_atomic(content: bytes, target: Path) -> None:
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False) as fh:
        temp_path = Path(fh.name)
        fh.write(content)
    try:
        os.replace(temp_path, target)
    finally:
        temp_path.unlink(missing_ok=True)


def _git_bytes(repo: Path, *args: str) -> bytes:
    result = _git(repo, *args, capture_bytes=True)
    if result.returncode:
        detail = result.stderr.decode(errors="replace").strip()
        raise MergeError(f"git {' '.join(args)} failed: {detail or 'unknown Git error'}")
    return result.stdout


def _git_succeeds(repo: Path, *args: str) -> bool:
    return _git(repo, *args).returncode == 0


def classify_history(repo: Path, local_ref: str = "HEAD", remote_ref: str = "FETCH_HEAD") -> str:
    """Classify two commits as equal, ahead, behind or diverged."""
    local_oid = _git_text(repo, "rev-parse", local_ref)
    remote_oid = _git_text(repo, "rev-parse", remote_ref)
    if local_oid == remote_oid:
        return "equal"
    if _git_succeeds(repo, "merge-base", "--is-ancestor", local_ref, remote_ref):
        return "behind"
    if _git_succeeds(repo, "merge-base", "--is-ancestor", remote_ref, local_ref):
        return "ahead"
    return "diverged"


def _path_changed(repo: Path, old_ref: str, new_ref: str, relative: str) -> bool:
    return not _git_succeeds(repo, "diff", "--quiet", old_ref, new_ref, "--", relative)


def _path_dirty(repo: Path, relative: str) -> bool:
    return not _git_succeeds(repo, "diff", "--quiet", "--", relative)


def _frames_equal(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> bool:
    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    left_sorted = left.sort_values(keys).reset_index(drop=True)
    right_sorted = right.sort_values(keys).reset_index(drop=True)
    return left_sorted.equals(right_sorted)


def _assert_graph_integration_safe(repo: Path, relative: str, history: str) -> None:
    if _git_succeeds(repo, "rev-parse", "--verify", "-q", "MERGE_HEAD"):
        raise MergeError("a Git merge is already in progress")
    if _git_text(repo, "diff", "--cached", "--name-only"):
        raise MergeError("the index contains staged changes; commit or unstage them before integrating Git history")
    if history not in {"behind", "diverged"}:
        return

    merge_base = _git_text(repo, "merge-base", "HEAD", "FETCH_HEAD")
    remote_changed = set(filter(None, _git_text(repo, "diff", "--name-only", merge_base, "FETCH_HEAD").splitlines()))
    dirty_tracked = set(filter(None, _git_text(repo, "diff", "--name-only").splitlines()))
    unsafe = sorted((dirty_tracked - {relative}) & remote_changed)
    if unsafe:
        raise MergeError("uncommitted files would overlap remote changes: " + ", ".join(unsafe))
    if history == "diverged":
        local_changed = set(filter(None, _git_text(repo, "diff", "--name-only", merge_base, "HEAD").splitlines()))
        concurrent = sorted((local_changed & remote_changed) - {relative})
        if concurrent:
            raise MergeError(
                "both branches changed non-parquet files; merge them manually first: " + ", ".join(concurrent)
            )


def _commit_target(repo: Path, relative: str, remote: str, branch: str) -> None:
    _git_text(repo, "add", "--", relative)
    result = _git(
        repo,
        "commit",
        "--only",
        "-m",
        f"data: reconcile {Path(relative).name} with {remote}/{branch}",
        "--",
        relative,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise MergeError(f"could not commit reconciled parquet: {detail or 'git commit failed'}")


def _merge_diverged_history(
    repo: Path,
    target: Path,
    relative: str,
    merged: pd.DataFrame,
    remote: str,
    branch: str,
) -> None:
    result = _git(repo, "merge", "--no-commit", "--no-ff", "FETCH_HEAD")
    unresolved = set(filter(None, _git_text(repo, "diff", "--name-only", "--diff-filter=U").splitlines()))
    unexpected = sorted(unresolved - {relative})
    if unexpected:
        _git(repo, "merge", "--abort")
        raise MergeError("merge has non-parquet conflicts requiring manual resolution: " + ", ".join(unexpected))
    if result.returncode and relative not in unresolved:
        detail = (result.stderr or result.stdout).strip()
        _git(repo, "merge", "--abort")
        raise MergeError(f"git merge failed: {detail or 'unknown Git error'}")

    _write_atomic(merged, target)
    _git_text(repo, "add", "--", relative)
    remaining = _git_text(repo, "diff", "--name-only", "--diff-filter=U")
    if remaining:
        _git(repo, "merge", "--abort")
        raise MergeError(f"merge still has unresolved files: {remaining.replace(chr(10), ', ')}")

    message = f"Merge {remote}/{branch} with parquet reconciliation"
    commit = _git(repo, "commit", "-m", message)
    if commit.returncode:
        detail = (commit.stderr or commit.stdout).strip()
        raise MergeError(f"parquet was resolved but the merge commit failed: {detail or 'git commit failed'}")


def _fast_forward_with_local_overlay(
    repo: Path,
    target: Path,
    relative: str,
    merged: pd.DataFrame,
    remote_frame: pd.DataFrame,
    keys: list[str],
) -> None:
    original = target.read_bytes()
    dirty = _path_dirty(repo, relative)
    if dirty:
        _write_bytes_atomic(_git_bytes(repo, "show", f"HEAD:{relative}"), target)
    result = _git(repo, "merge", "--ff-only", "FETCH_HEAD")
    if result.returncode:
        if dirty:
            _write_bytes_atomic(original, target)
        detail = (result.stderr or result.stdout).strip()
        raise MergeError(f"fast-forward failed: {detail or 'unknown Git error'}")
    if not _frames_equal(merged, remote_frame, keys):
        _write_atomic(merged, target)


def run(
    file: str,
    remote: str,
    branch: str | None,
    keys: list[str] | None,
    *,
    integrate: bool = True,
    push: bool = False,
) -> dict:
    repo = _repo_root()
    local_path, relative = _resolve_file(repo, file)
    remote_url = _git_text(repo, "remote", "get-url", remote)
    selected_branch = branch or _git_text(repo, "branch", "--show-current")
    if not selected_branch:
        raise MergeError("cannot infer a branch in detached HEAD state; pass --branch")
    current_branch = _git_text(repo, "branch", "--show-current")
    if integrate and selected_branch != current_branch:
        raise MergeError(
            f"graph integration requires the checked-out branch ({current_branch}) "
            f"to match --branch ({selected_branch})"
        )
    if push and not integrate:
        raise MergeError("--push cannot be combined with --file-only")

    print(f"Repository : {repo}")
    print(f"Local file : {relative}")
    print(f"Remote     : {remote} ({remote_url})")
    print(f"Branch     : {selected_branch}")
    print(f"Fetching {remote}/{selected_branch}...")
    _git_text(repo, "fetch", remote, selected_branch)

    remote_frame, remote_temp = _read_remote_file(repo, relative)
    try:
        local_frame = pd.read_parquet(local_path)
        history = classify_history(repo)
        working_dirty = _path_dirty(repo, relative)
        if integrate:
            merge_base = _git_text(repo, "merge-base", "HEAD", "FETCH_HEAD")
            locally_changed = working_dirty or _path_changed(repo, merge_base, "HEAD", relative)
        else:
            locally_changed = True
        overlap_winner = "local" if not integrate or locally_changed else "remote"
        merged, summary = merge_frames(remote_frame, local_frame, keys, prefer=overlap_winner)
        merged_differs = not _frames_equal(merged, local_frame, summary["keys"])

        if not integrate:
            _write_atomic(merged, local_path)
        else:
            _assert_graph_integration_safe(repo, relative, history)
            if history == "behind":
                _fast_forward_with_local_overlay(repo, local_path, relative, merged, remote_frame, summary["keys"])
            elif history == "diverged":
                if working_dirty:
                    if merged_differs:
                        _write_atomic(merged, local_path)
                    _commit_target(repo, relative, remote, selected_branch)
                _merge_diverged_history(repo, local_path, relative, merged, remote, selected_branch)
            elif merged_differs:
                _write_atomic(merged, local_path)
            if history != "diverged" and _path_dirty(repo, relative):
                _commit_target(repo, relative, remote, selected_branch)
    except MergeError:
        raise
    except Exception as exc:
        raise MergeError(f"could not merge {relative}: {exc}") from exc
    finally:
        remote_temp.unlink(missing_ok=True)

    print(f"Keys       : {', '.join(summary['keys'])}")
    print(f"Remote rows: {summary['remote_rows']:,} ({summary['remote_only_rows']:,} remote-only)")
    print(f"Local rows : {summary['local_rows']:,} ({summary['local_only_rows']:,} local-only)")
    print(f"Overlap    : {summary['overlap_rows']:,} ({summary['overlap_winner']} rows retained)")
    print(f"Merged rows: {summary['merged_rows']:,}")
    print(f"History    : {history}")
    print(f"Precedence : {summary['overlap_winner']} rows win on overlap")
    status = _git_text(repo, "status", "--short", "--", relative) or "clean (merged content matches HEAD)"
    print(f"Git status : {status}")
    if push:
        if _path_dirty(repo, relative):
            _commit_target(repo, relative, remote, selected_branch)
        _git_text(repo, "push", remote, f"HEAD:{selected_branch}")
        print(f"Pushed     : {remote}/{selected_branch}")
    else:
        print("Push       : not requested (use --push, or run git push normally)")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file", help="Repository-relative or absolute path to a tracked Parquet file")
    parser.add_argument("--remote", default="origin", help="Git remote to fetch (default: origin)")
    parser.add_argument("--branch", help="Remote branch (default: current local branch)")
    parser.add_argument("--keys", nargs="+", help="Logical row-key columns (default: infer known schemas)")
    parser.add_argument(
        "--file-only",
        action="store_true",
        help="merge parquet rows only; do not fast-forward or merge Git history",
    )
    parser.add_argument("--push", action="store_true", help="push after successful graph integration")
    args = parser.parse_args()
    try:
        run(args.file, args.remote, args.branch, args.keys, integrate=not args.file_only, push=args.push)
        return 0
    except MergeError as exc:
        print(f"Cannot merge: {exc}", file=sys.stderr)
        print("Nothing was pushed.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
