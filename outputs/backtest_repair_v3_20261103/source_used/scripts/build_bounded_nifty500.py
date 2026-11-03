"""Build an isolated July-2017/September-2020 Nifty 500 evidence candidate.

No network, fuzzy identity resolution, source publication or page/startup work.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import market_data as md
from backtest_provenance import sha256, write_manifest_exclusive
from historical_universe import (
    legal_name_key,
    match_event_identity,
    read_bhavcopy,
    read_nifty500_archive,
    validate_constituent_cache,
)


def build(evidence, source_events, output, parsed=None):
    output.mkdir(parents=True, exist_ok=False)
    observations = []
    for path in sorted(evidence.glob("indices*.zip")):
        cache = parsed / (path.stem + ".parquet") if parsed else None
        frame = pd.read_parquet(cache) if cache and cache.exists() else read_nifty500_archive(path)
        validate_constituent_cache(frame, path)
        frame["archive_sha256"] = sha256(path)
        observations.append(frame)
    observations = pd.concat(observations, ignore_index=True)
    observations["_name_key"] = observations.name.map(legal_name_key)
    observations.to_parquet(output / "primary_constituent_observations.parquet", index=False)
    bhav = pd.concat([read_bhavcopy(p) for p in sorted(evidence.glob("cm*.zip"))], ignore_index=True)
    observed_ids = observations.merge(
        bhav[bhav.SERIES.isin(["EQ", "BE", "BZ"])],
        left_on=["date", "symbol"],
        right_on=["date", "SYMBOL"],
        suffixes=("_index", "_exchange"),
    )
    observed_ids.to_csv(output / "monthly_security_observations.csv", index=False)
    lifecycle = observed_ids.groupby("symbol").agg(
        isins=("ISIN", lambda x: "|".join(sorted(set(x)))),
        distinct_security_ids=("ISIN", "nunique"),
        first_observed=("date", "min"),
        last_observed=("date", "max"),
    )
    lifecycle[lifecycle.distinct_security_ids.gt(1)].to_csv(output / "security_lifecycle_worklist.csv")
    src = pd.read_csv(source_events)
    events = src[src.INDEX_NAME.eq("NIFTY 500") & src.EVENT_DATE.between("2017-07-01", "2020-09-01")].copy()
    # Retain inherited proposal scores for provenance without presenting them as
    # the status of newly verified identities or of the unaccepted daily chain.
    events = events.rename(
        columns={
            key: "LEGACY_" + key for key in ["CONFIDENCE", "MATCH_SCORE", "RECONSTRUCTION_ALLOWED"] if key in events
        }
    )
    decisions = []
    aliases = []
    for event in events.to_dict("records"):
        alias, reason = match_event_identity(event, observations, bhav)
        decisions.append(
            {
                **event,
                "SYMBOL": alias["exchange_symbol"] if alias else None,
                "SECURITY_ID": alias["security_id"] if alias else None,
                "IDENTITY_STATUS": "verified" if alias else "unresolved",
                "EVENT_CHAIN_STATUS": "unverified",
                "DAILY_RECONSTRUCTION_ACCEPTED": False,
                "IDENTITY_EVIDENCE": json.dumps(alias, sort_keys=True) if alias else "",
                "REASON": reason,
                "RETRIEVED_AT": "2026-11-03",
                "VERIFIED_AT": "2026-11-03",
            }
        )
        if alias:
            aliases.append(alias)
    decisions = pd.DataFrame(decisions)
    decisions.to_csv(output / "event_identity_decisions.csv", index=False)
    decisions[decisions.IDENTITY_STATUS.eq("unresolved")].to_csv(output / "unresolved_events.csv", index=False)
    decisions.groupby(["SCRIP_NAME", "IDENTITY_STATUS"]).agg(
        events=("SOURCE_ROW_ID", "count"),
        dates=("EVENT_DATE", lambda x: "|".join(sorted(set(x)))),
        reason=("REASON", "first"),
    ).to_csv(output / "name_worklist.csv")
    # One reusable decision may support multiple events, while a date whitelist
    # prevents a sparse evidence span from certifying unobserved intervening aliases.
    grouped_aliases = {}
    for alias in aliases:
        key = (alias["name"], alias["security_id"], alias["exchange_symbol"])
        if key not in grouped_aliases:
            grouped_aliases[key] = {**alias, "verified_effective_dates": [], "evidence": [], "source_observations": []}
        decision = grouped_aliases[key]
        decision["verified_effective_dates"].append(alias["valid_from"])
        decision["valid_from"] = min(decision["valid_from"], alias["valid_from"])
        decision["valid_to"] = max(decision["valid_to"], alias["valid_to"])
        decision["evidence"].extend(alias["evidence"])
        decision["source_observations"].append(dict(date=alias["observed_at"], source=alias["source"]))
    for alias in grouped_aliases.values():
        alias["verified_effective_dates"] = sorted(set(alias["verified_effective_dates"]))
        alias["evidence"] = sorted(set(alias["evidence"]))
    write_manifest_exclusive(
        output / "reviewed_event_aliases.json",
        dict(version="bounded-primary-event-identities-v1", aliases=list(grouped_aliases.values())),
    )

    # Official August 31 is the supplied September 1 anchor's preceding close.
    # Compare, never force equal counts or drop an additional share class.
    anchor = observations[observations.date.eq("2020-08-31")].copy()
    securities = []
    for row in anchor.to_dict("records"):
        facts = bhav[bhav.date.eq(row["date"]) & bhav.SYMBOL.eq(row["symbol"]) & bhav.SERIES.isin(["EQ", "BE", "BZ"])]
        ids = sorted(set(facts.ISIN.dropna()))
        securities.append(
            {
                **row,
                "SECURITY_ID": "ISIN:" + ids[0] if len(ids) == 1 else None,
                "IDENTITY_STATUS": "verified" if len(ids) == 1 else "unresolved",
                "exchange_evidence": "|".join(facts.source_row_id.astype(str)),
            }
        )
    anchor = pd.DataFrame(securities)
    anchor.to_csv(output / "anchor_security_decisions.csv", index=False)
    raw = md.load_snapshot().membership
    local = raw[raw.INDEX_NAME.eq(md.index_key("Nifty 500")) & raw.TIME_STAMP.eq("2020-09-01")]
    anchor_compare = dict(
        official_date="2020-08-31",
        official_count=len(anchor),
        local_date="2020-09-01",
        local_count=len(local),
        official_only=sorted(set(anchor.symbol) - set(local.SYMBOL)),
        local_only=sorted(set(local.SYMBOL) - set(anchor.symbol)),
    )
    write_manifest_exclusive(output / "anchor_comparison.json", anchor_compare)

    # Provisional chain: identity-supported events only; unresolved transitions
    # stay explicitly in the discrepancy register and prevent acceptance.
    # Walk stable security IDs. A ticker rename is not an index entry/exit.
    state = set(anchor.SECURITY_ID.dropna())
    states = {pd.Timestamp("2020-09-01"): set(state)}
    conflicts = []
    for date, group in reversed(list(decisions.groupby("EVENT_DATE", sort=True))):
        states[pd.Timestamp(date)] = set(state)  # post-event state before reversal
        inc = set(group.loc[group.EVENT_TYPE.eq("INCL"), "SECURITY_ID"].dropna())
        exc = set(group.loc[group.EVENT_TYPE.eq("EXCL"), "SECURITY_ID"].dropna())
        if inc - state or exc & state or inc & exc:
            conflicts.append(
                dict(
                    date=date,
                    missing_inclusions=sorted(inc - state),
                    excluded_still_present=sorted(exc & state),
                    same_day_collision=sorted(inc & exc),
                )
            )
        state = (state - inc) | exc
    states[pd.Timestamp("2017-07-01")] = set(state)
    start_state = set(state)
    for date, group in decisions.groupby("EVENT_DATE", sort=True):
        inc = set(group.loc[group.EVENT_TYPE.eq("INCL"), "SECURITY_ID"].dropna())
        exc = set(group.loc[group.EVENT_TYPE.eq("EXCL"), "SECURITY_ID"].dropna())
        state = (state - exc) | inc
    roundtrip = state == set(anchor.SECURITY_ID.dropna())
    rows = [
        dict(INDEX_NAME="NIFTY 500", TIME_STAMP=d, SECURITY_ID=s, CONFIDENCE="unverified_chain")
        for d, syms in states.items()
        for s in sorted(syms)
    ]
    pd.DataFrame(rows).to_parquet(output / "provisional_membership.parquet", index=False)
    mismatches = []
    for date, group in observations.groupby("date"):
        if not pd.Timestamp("2017-07-01") <= date <= pd.Timestamp("2020-08-31"):
            continue
        known = max(d for d in states if d <= date)
        actual = states[known]
        matched = group.merge(
            bhav[bhav.date.eq(date) & bhav.SERIES.isin(["EQ", "BE", "BZ"])], left_on="symbol", right_on="SYMBOL"
        )
        truth = set("ISIN:" + s for s in matched.ISIN.dropna())
        mismatches.append(
            dict(
                date=str(date.date()),
                matches=actual == truth,
                actual_count=len(actual),
                observed_count=len(truth),
                observed_only="|".join(sorted(truth - actual)),
                reconstruction_only="|".join(sorted(actual - truth)),
            )
        )
    initial = observations[observations.date.eq("2017-06-30")].merge(
        bhav[bhav.date.eq("2017-06-30") & bhav.SERIES.isin(["EQ", "BE", "BZ"])], left_on="symbol", right_on="SYMBOL"
    )
    june = set("ISIN:" + s for s in initial.ISIN.dropna())
    mismatches.insert(
        0,
        dict(
            date="2017-07-01",
            matches=start_state == june,
            actual_count=len(start_state),
            observed_count=len(june),
            observed_only="|".join(sorted(june - start_state)),
            reconstruction_only="|".join(sorted(start_state - june)),
        ),
    )
    pd.DataFrame(mismatches).to_csv(output / "monthly_chain_discrepancies.csv", index=False)
    write_manifest_exclusive(output / "event_chain_conflicts.json", dict(conflicts=conflicts, round_trip=roundtrip))

    # All observed and reconstructed eligible identities stay in the price
    # worklist, including unavailable adjusted histories and retired symbols.
    snapshot = md.load_snapshot()
    prices = snapshot.equity_prices()
    spans = prices.groupby("symbol").date.agg(["min", "max", "count"])
    relevant = set(decisions.SYMBOL.dropna()).union(
        *(
            set(g.symbol)
            for d, g in observations.groupby("date")
            if pd.Timestamp("2017-06-30") <= d <= pd.Timestamp("2020-08-31")
        )
    )
    local_scope = prices[prices.date.between("2017-07-01", "2020-09-01")]
    if "price_status" in local_scope:
        local_scope = local_scope[local_scope.price_status.isin(["observed", "verified_mark"])]
    local_counts = local_scope.groupby("symbol").size().to_dict()
    price_rows = []
    for symbol in sorted(relevant):
        primary = bhav[bhav.SYMBOL.eq(symbol) & bhav.SERIES.isin(["EQ", "BE", "BZ"])]
        price_rows.append(
            dict(
                symbol=symbol,
                primary_isins="|".join(sorted(set(primary.ISIN.dropna()))),
                primary_observations=len(primary),
                local_adjusted_observations=local_counts.get(symbol, 0),
                local_first=None if symbol not in spans.index else str(spans.loc[symbol, "min"]),
                local_last=None if symbol not in spans.index else str(spans.loc[symbol, "max"]),
                missing_adjusted_prices=not local_counts.get(symbol, 0),
                vendor_identity="not certified; no current ticker substitution",
                lineage="requires review" if primary.ISIN.nunique() > 1 else "no cross-security lineage assumed",
            )
        )
    pd.DataFrame(price_rows).to_csv(output / "all_eligible_price_coverage.csv", index=False)
    eligible_unknown = decisions[decisions.SYMBOL.isna()]
    eligible_unknown.to_csv(output / "known_names_without_resolved_price_identity.csv", index=False)
    summary = dict(
        scope=["2017-07-01", "2020-09-01"],
        events=len(events),
        distinct_event_names=events.SCRIP_NAME.nunique(),
        resolved_events=int(decisions.IDENTITY_STATUS.eq("verified").sum()),
        resolved_names=int(decisions.loc[decisions.IDENTITY_STATUS.eq("verified"), "SCRIP_NAME"].nunique()),
        unresolved_events=len(eligible_unknown),
        unresolved_names=int(eligible_unknown.SCRIP_NAME.nunique()),
        official_anchor_members=len(anchor),
        verified_anchor_securities=int(anchor.IDENTITY_STATUS.eq("verified").sum()),
        monthly_observation_dates=int(observations.date.nunique()),
        chain_conflicts=len(conflicts),
        monthly_discrepancies=sum(not r["matches"] for r in mismatches),
        round_trip=roundtrip,
        historically_eligible_symbols=len(relevant),
        missing_local_adjusted_histories=sum(r["missing_adjusted_prices"] for r in price_rows),
        accepted=False,
        publication="not attempted: resolve event/anchor/chain/vendor/price evidence before certification",
        source_revisions=list(snapshot.revisions),
        upstream_requests=0,
        code_hashes={
            str(p): sha256(p)
            for p in [
                Path(__file__),
                Path(__file__).parents[1] / "historical_universe.py",
                Path(__file__).parents[1] / "security_identity.py",
            ]
        },
        parsed_input_hashes={str(p): sha256(p) for p in sorted(parsed.glob("indices*.parquet"))} if parsed else {},
        input_hashes={str(p): sha256(p) for p in [source_events, *sorted(evidence.glob("*.zip"))]},
    )
    write_manifest_exclusive(output / "manifest.json", summary)
    return summary


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ["evidence", "source-events", "output"]:
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--parsed", type=Path)
    a = p.parse_args()
    print(json.dumps(build(a.evidence, a.source_events, a.output, a.parsed), indent=2, default=str))
