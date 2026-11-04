"""Trace which unresolved observations touch the provisional replay."""
import hashlib
import json
from pathlib import Path

import pandas as pd

OUT = Path(__file__).resolve().parent
R5 = OUT.parent / "backtest_repair_v5_20261103"
R6 = OUT.parent / "backtest_repair_v6_20261103"


def main():
    eligibility = pd.read_csv(OUT / "eligibility.csv", parse_dates=["execution", "signal"])
    history = pd.read_parquet(OUT / "provisional_close_revision.parquet",
                              columns=["date", "research_security_id", "ISIN", "SYMBOL", "source_row_id"])
    dated_quotes = history.set_index(["research_security_id", "date"])
    calendar = pd.DatetimeIndex(sorted(history.date.unique()))
    eligibility["window_start"] = calendar[(calendar.get_indexer(eligibility.signal) - 252).clip(0)]
    groups = {s: g for s, g in eligibility.groupby("family")}
    empty = eligibility.iloc[:0]
    paths = {}
    for p in OUT.glob("*_daily.csv"):
        prefix = p.name.removesuffix("_daily.csv")
        day = pd.read_csv(p, parse_dates=["date"])
        held = {r.date: set(json.loads(r.quantities)) for r in day.itertuples()}
        trades = pd.read_csv(OUT / (prefix + "_trades.csv"), parse_dates=["date"])
        sold = trades.loc[trades.side.eq("SELL")].groupby("symbol").date.max().to_dict()
        lots = pd.read_csv(OUT / (prefix + "_closed_lots.csv"), parse_dates=["date", "acquired"])
        paths[prefix] = held, trades, sold, lots

    def exposure(family, start, end):
        frame = groups.get(family, empty)
        visible = frame.loc[frame.window_start.le(end) & frame.signal.ge(start)]
        eligible = visible.loc[visible.eligibility_reason.isin(["", "no_valid_score"]) |
                               visible.eligibility_reason.isna()]
        marks, fills, fifo, held_any = [], [], [], []
        for name, (held, trades, sold, lots) in paths.items():
            if any(family in symbols for symbols in held.values()):
                held_any.append(name)
            previous = max((day for day in held if day < start), default=None)
            held_at_issue = (previous is not None and family in held[previous]) or any(
                family in symbols for day, symbols in held.items() if start <= day <= end
            )
            if held_at_issue:
                marks.append(name)
            actual_trades = trades.loc[trades.symbol.eq(family) & trades.date.between(start, end)]
            if not actual_trades.empty:
                fills.append(name)
            if (lots.symbol.eq(family) & lots.acquired.le(end) & lots.date.ge(start)).any():
                fifo.append(name)
        return dict(
            consuming_signal_windows=len(visible), potentially_score_sensitive=len(eligible),
            closest_app_rank=eligible.app_rank.min(), closest_momo_rank=eligible.momo_rank.min(),
            top30_app=int(eligible.app_rank.le(30).sum()), top30_momo=int(eligible.momo_rank.le(30).sum()),
            actual_mark_or_entitlement_books="|".join(sorted(marks)),
            actual_same_interval_fill_books="|".join(sorted(fills)),
            fifo_basis_or_proceeds_books="|".join(sorted(fifo)),
            any_holdings_books="|".join(sorted(held_any)),
            absent_from_all_exported_books=not held_any,
            held_only_under_momo_ordering=bool(held_any) and all(name.endswith("_momo") for name in held_any),
            held_only_under_app_ordering=bool(held_any) and all(name.endswith("_app") for name in held_any),
        )

    data = json.loads((R6 / "finite_economic_review.json").read_text())
    overrides = {r["decision_id"] for r in data["model_overrides"]}
    economics = []
    for row in data["decisions"]:
        if row["decision_id"] in overrides:
            continue
        day = pd.Timestamp(row["date"])
        family = row["research_security_id"]
        if row["dated_symbol"] == "RELCAPITAL":
            proof = OUT / "evidence/RHFL_annual_2018_BSE.pdf"
            announcement = R5 / "evidence/RELCAPITAL_20170905_announcements.json"
            records = json.loads(announcement.read_text())
            assert "September 6, 2017" in records[16]["attchmntText"]
            assert "September 22, 2017" in records[8]["attchmntText"]
            row["needed"] = (
                "Issuer establishes 1:1 Reliance Home Finance shares, Sep 6 record, Sep 7 allotment, "
                "Sep 22 listing. Require contemporaneous prelisting claim value or documented adjusted "
                "research series, and a defensible parent/child basis allocation for tax replay."
            )
            row["round7_terms_evidence"] = (
                f"{proof.name} SHA-256 {hashlib.sha256(proof.read_bytes()).hexdigest()}, printed p14; "
                f"{announcement.name} SHA-256 {hashlib.sha256(announcement.read_bytes()).hexdigest()}, records 8 and 16"
            )
        economics.append(dict(family=family, symbol=row["dated_symbol"], isin=row["dated_isin"], date=day,
                              source=row["source_row_id"], terms=row["original_subject"], needed=row["needed"], terms_evidence=row.get("round7_terms_evidence", ""),
                              treatment="staged provisional Close series unchanged at unresolved action; no added claim or cash",
                              **exposure(family, day, day)))
    assert len(economics) == 33
    pd.DataFrame(economics).to_csv(OUT / "entitlement_exposure.csv", index=False)

    seams = pd.read_csv(R6 / "consumed_close_dependencies.csv", parse_dates=["previous_date", "date"])
    local = seams.loc[seams.source.str.startswith("original_local:")].copy()
    mapped = []
    for r in local.itertuples():
        quote = dated_quotes.loc[(r.family, r.date)]
        mapped.append(dict(dated_isin=quote.ISIN, dated_symbol=quote.SYMBOL, primary_quote_row=quote.source_row_id,
                           **exposure(r.family, r.previous_date, r.date)))
    pd.concat([local.reset_index(drop=True), pd.DataFrame(mapped)], axis=1).to_csv(
        OUT / "local_close_exposure.csv", index=False
    )
    vendors = seams.loc[~seams.source.str.startswith("original_local:")]
    vendors.to_csv(OUT / "unverified_vendor_diagnostic.csv", index=False)
    assert vendors.decision.eq("vendor historical identity and close convention unverified").sum() == 739
    assert local.decision.eq("opposing local close return; needs dated opposing quote/scale reconciliation").sum() == 236

    listing = pd.read_csv(R5 / "short_history_listing_evidence.csv", parse_dates=["execution"])
    listing = listing.loc[~listing.listing_decision.str.startswith("exact dated")].copy()
    assert len(listing) == 571
    listing = listing.merge(eligibility, on=["frequency", "execution", "family"], how="left", validate="one_to_one")
    listing["missing_to_252"] = 252 - listing.observations
    listing["treatment"] = "retain membership; do not fill unavailable observations or infer predecessor"
    listing["eligibility_dependency"] = listing.eligibility_reason.map(
        lambda reason: "direct 252-observation count" if reason == "insufficient_history"
        else "first resolve observed eligibility reason as well as proving sufficient history"
    )
    listing["evidence_needed"] = (
        "dated exchange listing/identity bridge or actual missing Close/Volume and contemporaneous quote coverage"
    )
    listing.to_csv(OUT / "short_history_exposure.csv", index=False)
    print("unresolved held across event",
          [(r["symbol"], str(r["date"].date())) for r in economics if r["actual_mark_or_entitlement_books"]])
    print("short history reasons", listing.eligibility_reason.value_counts(dropna=False).to_dict())
    print("members absent from all exports with potentially affected ranks",
          sum(r["absent_from_all_exported_books"] and r["potentially_score_sensitive"] > 0 for r in economics))


if __name__ == "__main__":
    main()

