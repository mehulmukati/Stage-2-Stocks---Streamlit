"""Seal the Round 7 replay artifacts without adding exploratory files."""
import json
from pathlib import Path

from run_matched import OUT, ROOT, sha

NAMES = (
    ".gitattributes", "run_matched.py", "verify_stage.py", "verify_policy_fixture.py", "verify_exports.py",
    "audit_exposure.py", "build_report.py", "seal_delivery.py", "round_7_report.md", "runtime.txt",
    "provisional_close_revision.parquet", "rights_application.json", "run_manifest.json", "run.log",
    "eligibility.csv", "results.csv", "reconciliation.json", "independent_reconciliation.json",
    "independent_stage_verification.json", "policy_fixtures.json", "gate_tests.log", "policy_fixture.log",
    "stage_verification.log", "replay_verification.log", "exposure_audit.log",
    "entitlement_exposure.csv", "local_close_exposure.csv", "short_history_exposure.csv",
    "unverified_vendor_diagnostic.csv", "source_used/momo_run_backtest.py",
    "source_used/MISSING_DATA_POLICY.md", "evidence/RHFL_annual_2018_BSE.pdf",
)


if __name__ == "__main__":
    run = json.loads((OUT / "run_manifest.json").read_text())
    assert run["accepted"] is False
    for name, expected in run["files"].items():
        assert sha(ROOT / Path(name)) == expected, name
    exports = [p.name for p in OUT.glob("*_*.csv")
               if p.name.startswith(("weekly_", "monthly_"))]
    assert len(exports) == 40
    paths = sorted([*NAMES, *exports])
    assert "29 passed" in (OUT / "gate_tests.log").read_text()
    (OUT / "delivery_manifest.json").write_text(json.dumps({
        "baseline_commit": "189489f", "acceptance": "unaccepted, conditional provisional comparison",
        "sha256": {p: sha(OUT / p) for p in paths},
    }, indent=2))
    print("Sealed", len(paths), "files")
