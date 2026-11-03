"""Apply the finite Round 5 evidence to the sealed Round 4 candidate."""
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from historical_price_model import back_adjust_primary_quotes
from research_entitlements import apply_reviewed_decisions

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
BASE = HERE.parent / "backtest_repair_v4_20261103/primary_model_v8"


def sha(path):
    return hashlib.file_digest(path.open("rb"), "sha256").hexdigest()


def main():
    decision_path = HERE / "finite_decisions_v3.json"
    output = HERE / "reviewed_model_v2"
    decisions = json.loads(decision_path.read_text())
    price_path = BASE / "primary_adjusted_research_candidate.parquet"
    action_path = BASE / "action_decisions.csv"
    prices = pd.read_parquet(price_path)
    actions = pd.read_csv(action_path, low_memory=False)
    changed = {d["research_security_id"] for d in decisions if d["status"] == "resolved"}
    keys = {d["source_row_id"] for d in decisions if d["status"] == "resolved"}
    new_prices, new_actions = [], []
    for family in sorted(changed):
        raw = prices.loc[prices.research_security_id.eq(family)].set_index("date")
        old = actions.loc[actions.research_security_id.eq(family)]
        rows = old.astype(object).where(old.notna(), None).to_dict("records")
        rows = apply_reviewed_decisions(rows, decisions, ROOT, family)
        for review in [row for row in rows if row.get("review_decision_id")]:
            for row in rows:
                if (row["date"] == review["date"] and row["kind"] == "cash_dividend"
                        and review["kind"] == "reviewed_cash_dividend" and row["cash"] == review["cash"]):
                    row["kind"] = review["kind"]
        rows.sort(key=lambda row: not bool(row.get("review_decision_id")))
        for row in rows:
            for field in ("factor", "applied", "previous_raw_close", "previous_quote_date", "ex_quote_available"):
                row.pop(field, None)
        revised, events = back_adjust_primary_quotes(raw, rows)
        actual = {event["source_row_id"] for event in events if event["applied"]}
        for review in [row for row in rows if row.get("review_decision_id")]:
            for row in rows:
                if (row["date"] == review["date"] and row["kind"] == "cash_dividend"
                        and review["kind"] == "reviewed_cash_dividend" and row["cash"] == review["cash"]):
                    row["kind"] = review["kind"]
        rows.sort(key=lambda row: not bool(row.get("review_decision_id")))
        for row in rows:
            if row["source_row_id"] in keys - actual:
                same = [event for event in events if event["date"] == row["date"] and event["applied"]]
                assert len(same) == 1 and all(row[k] == same[0][k] for k in ("kind", "cash", "share_ratio")), row
                events.append({**row, "applied": False, "reason": "included in aggregate " + same[0]["source_row_id"]})
        # Independent closed-form oracle: use untouched pre-event closes and multiply
        # accepted terms on each earlier row, including exactly one aggregate per date.
        expected = np.ones(len(raw))
        for event in events:
            if not event["applied"]:
                continue
            prior = raw.index < pd.Timestamp(event["date"])
            close = raw.loc[prior, "CLOSE"].iloc[-1]
            if event["kind"] == "reviewed_fully_paid_rights":
                entitlement = event["rights_ratio"]
                factor = (close + entitlement * event["subscription_price"]) / (close * (1 + entitlement))
            else:
                factor = (1 - event["cash"] / close) / event["share_ratio"]
            assert np.isclose(event["factor"], factor, rtol=1e-13, atol=0)
            expected[prior] *= factor
        np.testing.assert_allclose(revised.Close, raw.CLOSE * expected, rtol=1e-13)
        np.testing.assert_array_equal(revised.Volume, raw.TOTTRDQTY)
        new_prices.append(revised.reset_index())
        new_actions.extend(events)
    prices = pd.concat([prices.loc[~prices.research_security_id.isin(changed)], *new_prices], ignore_index=True)
    events = pd.concat([actions.loc[~actions.research_security_id.isin(changed)], pd.DataFrame(new_actions)])
    assert not prices.duplicated(["date", "ISIN"]).any()
    assert keys <= set(events.source_row_id)
    assert not prices.accepted_for_backtest.any()
    output.mkdir()
    prices.to_parquet(output / price_path.name, index=False)
    events.to_csv(output / action_path.name, index=False)
    refs = [decision_path, price_path, action_path, Path(__file__), ROOT / "historical_price_model.py",
            ROOT / "research_entitlements.py", output / price_path.name, output / action_path.name]
    manifest = dict(accepted=False, price_rows=len(prices), reviewed_records=len(keys),
                    reviewed_factors=int(events.loc[events.source_row_id.isin(keys), "applied"].sum()),
                    rebuilt_families=len(changed), sha256={str(p.relative_to(ROOT)): sha(p) for p in refs})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({k:v for k,v in manifest.items() if k != "sha256"}))


if __name__ == "__main__":
    main()


