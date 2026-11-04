"""Trace the remaining action and Close dependencies without producing performance."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from historical_price_model import primary_event_factor
from research_entitlements import apply_reviewed_decisions

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
R5 = HERE.parent / "backtest_repair_v5_20261103"
R4 = HERE.parent / "backtest_repair_v4_20261103"
CANDIDATE = R5 / "reviewed_model_v2/primary_adjusted_research_candidate.parquet"
OFFERS = {
    "nse_actions_2014_2020.json_6057": (
        "CANBK", "2017-02-17", "2017-02-20", 1, 10, 207, "CANBK_BSE_offer.pdf",
        "https://www.bseindia.com/downloads/ipo/finalletterofoffercanarabankclean_160220171440.pdf",
        "BSE final offer PDF pages 1, 240: one fully paid share per ten; full Rs207 on application.",
    ),
    "nse_actions_2014_2020.json_7807": (
        "KARURVYSYA", "2017-10-12", "2017-10-13", 1, 6, 76, "KARURVYSYA_offer_alt.pdf",
        "https://www.kvb.bank.in/docs/rights-issue-17-letter-of-offer-rights-issue-2017.pdf",
        "Issuer final offer PDF pages 1, 42: one fully paid share per six; full Rs76 on application.",
    ),
}
PVR = (
    "PVR", "2020-07-09", "2020-07-10", 7, 94, 784, "PVR_offer.pdf",
    "https://www.sebi.gov.in/sebi_data/attachdocs/jul-2020/1594192456627.pdf",
    "Final offer printed page 52: seven for 94; full Rs784 on application.",
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    decisions = json.loads((R5 / "finite_decisions_v3.json").read_text())
    residual = [row for row in decisions if row["status"] == "unsupported"]
    assert len(residual) == 36
    offers = dict(OFFERS)
    pvr = next(row for row in residual if row["dated_symbol"] == "PVR")
    offers[pvr["decision_id"]] = PVR
    action_path = R5 / "reviewed_model_v2/action_decisions.csv"
    action_rows = pd.read_csv(action_path, low_memory=False)
    reviewed, overrides = [], []
    for row in residual:
        row["affected_profiles"] = "original weekly and monthly Classic potential dependencies"
        row["consumed_field"] = "Close / ownership marks where applicable"
        row["round6_status"] = "residual"
        row["round6_source_url"] = ""
        row["round6_locator"] = ""
        if row["dated_symbol"] == "RELIANCE":
            row["round6_source_url"] = "https://www.ril.com/sites/default/files/2023-01/UpdateonRI17052020.pdf"
            row["round6_locator"] = "17 May issuer notice: Rs314.25 application, Rs314.25 May 2021, Rs628.50 November 2021."
            row["needed"] = "Independent May 13 entitlement valuation at ex-date, reflecting staged calls and tradable rights availability; terms verified."
        if row["dated_symbol"] == "ARVINDFASN":
            row["round6_source_url"] = "https://www.arvindfashions.com/wp-content/uploads/2020/06/FAQ.pdf"
            row["round6_locator"] = "Final issuer FAQ retains March 18 record but reprices to Rs100 and changes ratio to 62:91."
            row["needed"] = "Contemporaneous March 17 claim valuation under terms then known and treatment of June repricing; no retrospective use of final terms."
        if row["decision_id"] not in offers:
            continue
        symbol, ex, record, num, den, price, filename, url, locator = offers[row["decision_id"]]
        assert (row["dated_symbol"], row["date"]) == (symbol, ex)
        matches = action_rows.loc[action_rows.research_security_id.eq(row["research_security_id"]) & action_rows.date.eq(ex)]
        assert row["source_row_id"] in set(matches.source_row_id) and not matches.applied.any()
        quotes = pd.read_parquet(
            CANDIDATE, columns=["date", "CLOSE", "source_row_id", "ISIN"],
            filters=[("research_security_id", "==", row["research_security_id"])],
        ).sort_values("date")
        day = pd.Timestamp(ex)
        assert quotes.loc[quotes.date.eq(day), "ISIN"].tolist() == [row["dated_isin"]]
        previous = quotes.loc[quotes.date.lt(day)].iloc[-1]
        close = float(previous.CLOSE)
        ratio = num / den
        assert close > price > 0
        factor = (close + ratio * price) / (close * (1 + ratio))
        document = HERE / "evidence" / filename
        assert document.is_file(), document
        source = document.relative_to(ROOT).as_posix()
        override = {
            **row, "status": "resolved",
            "normalization": {
                "kind": "reviewed_fully_paid_rights", "supported": True, "share_ratio": 1.0,
                "cash": 0.0, "rights_ratio": ratio, "subscription_price": price, "reviewed_source": source,
            },
            "evidence": [*row["evidence"], {"path": source, "sha256": digest(document), "locator": locator}],
        }
        actions = matches.astype(object).where(matches.notna(), None).to_dict("records")
        actual = apply_reviewed_decisions(actions, [override], ROOT, row["research_security_id"])
        actual = next(action for action in actual if action["source_row_id"] == row["source_row_id"])
        assert abs(primary_event_factor(close, actual) - factor) < 1e-13
        overrides.append(override)
        row.update(
            round6_status="fully paid terms resolved; proposed factor verified; publication pending",
            round6_source_url=url, round6_locator=locator, record_date=record,
            source_pdf_sha256=digest(document),
            proposed_event_factor=factor, previous_raw_close=close,
            previous_quote_date=str(previous.date.date()), previous_quote_source=previous.source_row_id,
            needed="Integrate evidence-bound adjustment in an accepted source revision and replay all consuming intervals.",
        )
        reviewed.append(row)

    seams = pd.read_csv(R5 / "opposing_interval_decisions_v3.csv")
    relevant = seams.loc[seams.rank_window | seams.potential_holding].copy()
    relevant["required_field"] = "Close"
    relevant["resolution_or_evidence"] = relevant.decision.map({
        "within tolerance after reviewed model": "No material residual under the original tolerance.",
        "missing or unresolved primary entitlement": "See listed primary action rows and dated entitlement requirements.",
        "vendor historical identity and close convention unverified": "Require exact historical ISIN mapping and documented opposing adjustment convention before promotion.",
        "documented primary action; opposing factor or quote remains different": "Require dated nominal opposing close and adjustment history to isolate convention from quote error.",
        "opposing local close return; needs dated opposing quote/scale reconciliation": "Require dated opposing nominal closes and split/adjustment basis, or independently verified correction for this interval.",
    })
    relevant["round6_residual_preview"] = relevant.new_residual
    for row in reviewed:
        mask = (
            relevant.family.eq(row["research_security_id"])
            & relevant.previous_date.lt(row["date"]) & relevant.date.ge(row["date"])
        )
        relevant.loc[mask, "round6_residual_preview"] = (
            (1 + relevant.loc[mask, "new_residual"]) * row["proposed_event_factor"] - 1
        )
    relevant.to_csv(HERE / "consumed_close_dependencies.csv", index=False)
    (HERE / "finite_economic_review.json").write_text(json.dumps({
        "inputs": {str(p.relative_to(HERE.parent)): digest(p) for p in (
            R5 / "finite_decisions_v3.json", R5 / "opposing_interval_decisions_v3.csv",
            action_path, CANDIDATE, R4 / "frozen_comparison_plan_v4.json",
        )},
        "accepted": False,
        "decisions": residual,
        "model_overrides": overrides,
    }, indent=2))
    print("Fully paid terms resolved", len(reviewed), "residual action terms or valuation", len(residual)-len(reviewed))
    print(relevant.decision.value_counts().to_string())
    print("Preview material intervals", int(relevant.round6_residual_preview.abs().gt(.001).sum()))


if __name__ == "__main__":
    main()

