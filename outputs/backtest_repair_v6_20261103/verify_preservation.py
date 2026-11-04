"""Check that Round 6 did not change protected inputs or prior sealed evidence."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


if __name__ == "__main__":
    r5 = HERE.parent / "backtest_repair_v5_20261103"
    report = {}
    for version in (3, 4, 5):
        directory = HERE.parent / f"backtest_repair_v{version}_20261103"
        paths = json.loads((directory / "delivery_manifest.json").read_text())["sha256"]
        for name, expected in paths.items():
            assert digest(directory / name) == expected, name
        report[f"round_{version}_sealed_files"] = len(paths)
    protected = json.loads((r5 / "baseline.json").read_text())["protected"]
    for item in protected:
        assert digest(ROOT / item["path"]) == item["sha256"], item["path"]
    report["protected_files"] = len(protected)
    print(json.dumps(report, indent=2))
