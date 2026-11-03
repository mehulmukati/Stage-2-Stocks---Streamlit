"""Verify protected inputs, previous seals and candidate raw observations."""
import json
from pathlib import Path
import subprocess

import pandas as pd

from build_reviewed_model import BASE, HERE, ROOT, sha


def main():
    report = {}
    baseline = json.loads((HERE / "baseline.json").read_text())
    for row in baseline["protected"]:
        assert sha(ROOT / row["path"]) == row["sha256"], row["path"]
    report["protected_unchanged"] = len(baseline["protected"])
    for version in (3, 4):
        directory = HERE.parent / f"backtest_repair_v{version}_20261103"
        seal = json.loads((directory / "delivery_manifest.json").read_text())["sha256"]
        for path, digest in seal.items():
            assert sha(directory / path) == digest, path
        report[f"round_{version}_sealed_unchanged"] = len(seal)
    names = ["backtest_engine.py", "backtest_accounting.py", "momentum_engine.py",
             "app.py", "app_backtest.py", "data/nse_session_calendar.json"]
    subprocess.run(["git", "diff", "--exit-code", "93483c1", "--", *names], cwd=ROOT, check=True)
    report["engine_circuit_ui_calendar_unchanged"] = names
    old = BASE / "primary_adjusted_research_candidate.parquet"
    new = HERE / "reviewed_model_v2/primary_adjusted_research_candidate.parquet"
    columns = ["date", "ISIN", "SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE",
               "TOTTRDQTY", "source_row_id", "research_security_id"]
    a,b = [pd.read_parquet(path, columns=columns).sort_values(["date","ISIN"]).reset_index(drop=True)
           for path in (old,new)]
    pd.testing.assert_frame_equal(a,b,check_exact=True)
    report["unchanged_raw_rows"] = len(a)
    (HERE / "independent_preservation.json").write_text(json.dumps(report,indent=2))
    print(report)


if __name__ == "__main__":
    main()
