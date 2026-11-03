"""Seal the curated Round 5 delivery, preserving source bytes for replay."""
import json
from pathlib import Path
import shutil

from build_reviewed_model import HERE, ROOT, sha

ROOT_NAMES = (
    "backtest_provenance.py", "data_backtest.py", "historical_price_model.py", "market_data.py",
    "research_acceptance.py", "research_entitlements.py", "research_execution.py", "research_ownership.py",
    "tests/test_calculation_cache_revision.py", "tests/test_research_field_acceptance.py",
    "tests/test_reviewed_action_denominations.py", "tests/test_research_execution_ownership.py",
)
EXACT = (
    ".gitattributes", "baseline.json", "finite_entitlement_worklist.csv", "retained_bulletin_terms.csv",
    "finite_announcement_terms.csv", "finite_attachment_request_basis.csv", "finite_announcement_request_basis.csv",
    "filing_payment_ownership_terms.csv", "filing_text_manifest.csv", "finite_decisions_v3.csv", "finite_decisions_v3.json",
    "ownership_replay.json", "locked_claim_marks.csv", "completed_schedule_v3.csv", "short_history_cases_v3.csv",
    "short_history_listing_evidence.csv", "opposing_interval_decisions_v3.csv", "consumption_audit_v3.log",
    "final_targeted_tests.log", "full_tests.log", "independent_preservation.json", "round_5_report.md",
)


def main():
    delivery = {HERE / name for name in EXACT}
    for name in ROOT_NAMES:
        snapshot = HERE / "source_used" / name
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, snapshot)
        delivery.add(snapshot)
    for pattern in ("*.py", "*_urls.json", "retrieval_*.json", "evidence/*", "visual_checks/*", "reviewed_model_v2/*"):
        delivery.update(p for p in HERE.glob(pattern) if p.is_file())
    payload = dict(baseline="93483c1", accepted=False, published=False, comparison_run=False,
                   final_targeted="24 passed", regression="503 passed before final scope/cache guard additions; targeted recheck passed",
                   sha256={p.relative_to(HERE).as_posix():sha(p) for p in sorted(delivery)})
    (HERE / "delivery_manifest.json").write_text(json.dumps(payload, indent=2))
    print("Sealed", len(delivery), "files")


if __name__ == "__main__":
    main()
