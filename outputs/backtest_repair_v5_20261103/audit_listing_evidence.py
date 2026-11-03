"""Classify only exchange listing facts that bind the exact dated security."""
import pandas as pd

from build_reviewed_model import HERE


def main():
    r3 = HERE.parent / "backtest_repair_v3_20261103"
    master = pd.read_csv(r3 / "evidence/EQUITY_L.csv")
    master.columns = master.columns.str.strip()
    master = master.rename(columns={"ISIN NUMBER": "first_isin", "DATE OF LISTING": "exchange_listing_date"})
    master.exchange_listing_date = pd.to_datetime(master.exchange_listing_date, format="%d-%b-%Y")
    short = pd.read_csv(HERE / "short_history_cases_v3.csv", parse_dates=["first_primary_quote"])
    merged = short.merge(master[["first_isin","exchange_listing_date","SYMBOL"]], on="first_isin", how="left", validate="many_to_one")
    merged["listing_decision"] = "historical exchange listing/successor or coverage evidence needed"
    matching = merged.exchange_listing_date.eq(merged.first_primary_quote)
    merged.loc[matching, "listing_decision"] = "exact dated ISIN exchange listing start; no predecessor history imputed"
    precedes = merged.exchange_listing_date.lt(merged.first_primary_quote)
    merged.loc[precedes, "listing_decision"] = "listing predates first observed quote; verify predecessor/actual missing sessions"
    merged.to_csv(HERE / "short_history_listing_evidence.csv", index=False)
    print(merged.groupby("listing_decision").agg(cases=("family","size"), lineages=("family","nunique")))


if __name__ == "__main__":
    main()
