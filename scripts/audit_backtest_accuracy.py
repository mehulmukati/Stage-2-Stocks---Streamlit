"""Offline identity/coverage audit. Never fetches or publishes shared data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import market_data as md
from backtest_provenance import sha256, write_manifest_exclusive

COMPONENTS = ["Nifty 50", "Nifty Next 50", "Nifty Midcap 150", "Nifty Smallcap 250", "Nifty Microcap 250"]


def audit(candidate, output):
    output.mkdir(parents=True, exist_ok=False)
    snapshot = md.load_snapshot()
    membership = snapshot.membership
    quotes = snapshot.equity_prices()
    source = pd.read_csv(candidate / "source_events_with_decisions.csv")
    proposals = pd.read_csv(candidate / "mapping_proposals.csv")
    observed = pd.read_parquet(candidate / "observed_snapshots.parquet")
    anchored = set(observed.INDEX_NAME.map(md.index_key))
    orphan = source.loc[~source.INDEX_NAME.map(md.index_key).isin(anchored)]
    orphan.to_csv(output / "unanchored_source_events.csv", index=False)
    # Retain every source name independently of today's surviving tickers.
    impact = (
        source.groupby("SCRIP_NAME")
        .agg(
            source_records=("SOURCE_ROW_ID", "count"),
            first_event=("EVENT_DATE", "min"),
            last_event=("EVENT_DATE", "max"),
            affected_indices=("INDEX_NAME", lambda x: "|".join(sorted(set(x)))),
            earliest_exclusions=("INCEPTION_REVIEW_REQUIRED", "sum"),
        )
        .reset_index()
    )
    impact = impact.merge(proposals, on="SCRIP_NAME", how="left")
    impact["decision"] = "quarantined; dated identity evidence required"
    impact["return_impact"] = "unquantified; no trusted substitution or survivor mapping"
    impact.to_csv(output / "unresolved_impact_register.csv", index=False)
    spans = quotes.groupby("symbol").date.agg(["min", "max", "count"])
    price_rows = []
    # Audit all known historical symbols, plus unknown source identities. Never
    # substitute a proposed survivor's prices for a historical name.
    for symbol, g in membership.groupby("SYMBOL"):
        span = spans.loc[symbol] if symbol in spans.index else None
        first = pd.to_datetime(g.TIME_STAMP).min()
        price_rows.append(
            dict(
                identity=symbol,
                identity_status="snapshot_symbol_not_full_security_master",
                first_recorded_membership=first,
                first_price=None if span is None else span["min"],
                last_price=None if span is None else span["max"],
                observations=0 if span is None else span["count"],
                missing_price_history=span is None,
                membership_before_first_observation=span is not None and first < span["min"],
            )
        )
    for name in impact.SCRIP_NAME:
        price_rows.append(
            dict(identity=name, identity_status="unresolved_source_name", missing_price_history=True, observations=0)
        )
    pd.DataFrame(price_rows).to_csv(output / "all_historical_price_coverage.csv", index=False)
    keys = {md.index_key(s): s for s in COMPONENTS + ["Nifty 500", "Nifty LargeMidcap 250"]}
    selected = membership[membership.INDEX_NAME.isin(keys)]
    dates = sorted(pd.to_datetime(selected.TIME_STAMP).unique())
    reports = []
    discrepancies = []
    grouped = {idx: g for idx, g in selected.groupby("INDEX_NAME")}
    by_date = {
        idx: {pd.Timestamp(d): set(rows.SYMBOL) for d, rows in g.groupby("TIME_STAMP")} for idx, g in grouped.items()
    }
    last = {idx: pd.to_datetime(g.TIME_STAMP).max() for idx, g in grouped.items()}
    states = {}
    state_dates = {}
    for date in dates:
        day = pd.Timestamp(date)
        for idx, history in by_date.items():
            if day in history:
                states[idx] = history[day]
                state_dates[idx] = day
            names = states.get(idx, set())
            missing = sorted(s for s in names if s not in spans.index or spans.loc[s, "min"] > day)
            reports.append(
                dict(
                    date=day,
                    index=keys[idx],
                    snapshot_date=state_dates.get(idx),
                    actual_recorded_count=len(names),
                    missing_price_count=len(missing),
                    missing_prices="|".join(missing),
                    history_stops=last[idx],
                    beyond_last_record=day > last[idx],
                    confidence="uncertified",
                    trusted_parent_recovery_allowed=False,
                )
            )
        component = set().union(*(states.get(md.index_key(i), set()) for i in COMPONENTS[:-1]))
        parent = states.get(md.index_key("Nifty 500"), set())
        if component and parent:
            discrepancies.append(
                dict(
                    date=day,
                    component_count=len(component),
                    parent_count=len(parent),
                    parent_only="|".join(sorted(parent - component)),
                    component_only="|".join(sorted(component - parent)),
                    differs=parent != component,
                    precedence="observed component snapshots retained; no automatic substitution",
                )
            )
    pd.DataFrame(reports).to_csv(output / "per_index_date_coverage.csv", index=False)
    pd.DataFrame(discrepancies).to_csv(output / "parent_component_discrepancies.csv", index=False)
    facts = {
        "legacy_index_aliases": {
            "effective_date": "2018-04-02",
            "source": "https://www.niftyindices.com/Press_Release/ind_prs05012018.pdf",
            "renames": {
                "Nifty Free Float Midcap 100": "Nifty Midcap 100",
                "Nifty Free Float Smallcap 100": "Nifty Smallcap 100",
            },
            "membership_rule_change": True,
            "not_equivalent_to": ["Midcap 150", "Smallcap 250"],
        },
        "largemidcap_relationship": {
            "launch": "2017-11-30",
            "base_date": "2005-04-01",
            "source": "https://www.niftyindices.com/Press_Release/ind_prs30112017.pdf",
            "recovery": "not accepted: pre-launch backcalculation and underlying identities need evidence",
        },
        "microcap": {
            "launch": "2021-05-10",
            "first_local_snapshot": "2021-05-11",
            "source": "https://www.niftyindices.com/Press_Release/ind_prs10052021.pdf",
            "pre_launch_members": "not obtained; base date supplies no members",
        },
        "validated_complete_universe_intervals": [],
        "comparison_gate": "closed: historical identities and complete coverage unverified",
        "source_records": len(source),
        "unresolved_records": int(source.SYMBOL.isna().sum()),
        "proposal_methods": proposals.METHOD.value_counts().to_dict(),
        "source_revisions": snapshot.revisions,
        "inputs_sha256": {
            str(p): sha256(p)
            for p in [
                candidate / "source_events_with_decisions.csv",
                candidate / "mapping_proposals.csv",
                md.PRICE_PATH,
                md.MEMBERSHIP_PATH,
            ]
        },
        "upstream_requests": 0,
        "shared_writes": 0,
    }
    write_manifest_exclusive(output / "coverage_gate.json", facts)
    return facts


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(audit(args.candidate, args.output), indent=2, default=str))
