"""Run both former bypasses through the app's loader and the real engine."""
import json
import subprocess

import data_backtest
import research_acceptance
from backtest_engine import run_backtest
from tests.test_research_field_acceptance import inputs


def exercise(mode):
    _, reviewed, cfg, cert = inputs()
    if mode == "empty_invalid":
        cert["price_field_hashes"].clear()
        cert["economics_validated"] = False
    else:
        del cert["price_field_hashes"]["A"]
    frames = data_backtest.load_ohlcv_for_backtest(snapshot=reviewed).symbol_data
    try:
        result = run_backtest(frames, {}, cfg)
    except ValueError as error:
        return {"rejected": str(error)}
    return {
        "reviewed_label": bool(result["run_manifest"]["reviewed_field_scope_sha256"]),
        "fills": sum(
            t["side"] in {"BUY", "SELL"} for rows in result["execution_ledgers"].values() for t in rows
        ),
    }


if __name__ == "__main__":
    baseline = {}
    source = subprocess.check_output(["git", "show", "8217282:research_acceptance.py"]).decode("utf-8-sig")
    exec(compile(source, "8217282:research_acceptance.py", "exec"), baseline)
    current = research_acceptance.validate_consumption
    baseline["field_digest"] = research_acceptance.field_digest  # Keep fixture hash format constant.
    outcomes = {}
    for mode in ("omitted_traded_member", "empty_invalid"):
        research_acceptance.validate_consumption = baseline["validate_consumption"]
        old = exercise(mode)
        research_acceptance.validate_consumption = current
        new = exercise(mode)
        assert old["reviewed_label"] and old["fills"] > 0
        assert "rejected" in new
        outcomes[mode] = {"baseline": old, "round6": new}
    print(json.dumps(outcomes, indent=2))

