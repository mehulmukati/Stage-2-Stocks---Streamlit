"""Build dated ownership replay inputs without changing membership or tradability."""
import json
from pathlib import Path

import pandas as pd

from build_reviewed_model import HERE, ROOT, sha
from research_ownership import merger_claim

EVENTS = [
    ("TECHNO", "INE286K01024", "INE285K01026", "2018-07-24", "2018-08-07",
     "2018-08-08", "2018-08-09", None, "2018-12-04", 1, False,
     ["TECHNO_ownership_announcements.json", "TECHNO_ratio_record_NSE.zip", "TECHNO_buyback_native_SEBI.pdf"]),
    ("CAPF", "INE688I01017", "INE092T01019", "2018-12-18", "2018-12-27",
     "2018-12-28", "2018-12-31", "2019-01-05", "2019-01-16", 139/10, True,
     ["CAPF_allotment_issuer.pdf", "CAPF_effective_issuer.html", "CAPF_listing_issuer.html", "CAPF_merger_FAQ.html"]),
]


def main():
    path = HERE / "reviewed_model_v2/primary_adjusted_research_candidate.parquet"
    prices = pd.read_parquet(path)
    ledger, marks_all = [], []
    for event in EVENTS:
        name, old, new, effective, last, suspension, record, allotment, listing, ratio, parity, docs = event
        old_series = prices.loc[prices.ISIN.eq(old)].set_index("date").sort_index()
        new_series = prices.loc[prices.ISIN.eq(new)].set_index("date").sort_index()
        assert old_series.index.max() == pd.Timestamp(last)
        actions, marks = merger_claim(old, new, suspension, listing, ratio, old_series.loc[last],
                                      new_series, mark_from_listed_class=parity)
        marks_all.append(marks.assign(claim="CLAIM:" + old).reset_index(names="date"))
        ledger.append(dict(name=name, old=old, successor=new, effective_date=effective, last_old_close=last,
                           suspension=suspension, record=record, allotment=allotment, first_newly_allotted_tradable=listing,
                           nominal_share_ratio=ratio, old_quote_source=old_series.loc[last,"source_row_id"],
                           listing_quote_source=new_series.loc[listing,"source_row_id"],
                           pending_marks=len(marks), actions=actions,
                           pending_policy=("same-class listed share mark; receipt locked" if parity else "carry last actual old mark and timestamp; receipt locked"),
                           fractions=("proportional adjusted research units; trustee sells nominal fractions; no assumed cash receipt"
                                      if parity else "one successor share per old share"),
                           evidence={str(p.relative_to(ROOT)):sha(p) for p in [HERE/"evidence"/d for d in docs]}))
    pd.concat(marks_all, ignore_index=True).to_csv(HERE / "locked_claim_marks.csv", index=False)
    (HERE / "ownership_replay.json").write_text(json.dumps(dict(price_sha256=sha(path), events=ledger), indent=2))
    print([(d["name"], d["pending_marks"], d["actions"][1]["share_ratio"]) for d in ledger])


if __name__ == "__main__":
    main()

