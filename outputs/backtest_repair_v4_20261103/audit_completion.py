"""Offline, full-worklist and potential-consumption audit; no portfolios or publication."""

from pathlib import Path
import hashlib
import io
import json
import sys
import zipfile

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from backtest_engine import get_rebalance_dates
from historical_price_evidence import parse_vendor_chart
from historical_price_model import close_factor_seam_audit
from momentum_engine import precompute_metrics

OUT = Path(__file__).parent
R3 = ROOT / "outputs/backtest_repair_v3_20261103"


def main(model_name, audit_name):
    model = OUT / model_name
    target = OUT / audit_name
    target.mkdir(exist_ok=False)
    prices = pd.read_parquet(model / "primary_adjusted_research_candidate.parquet")
    prices["date"] = pd.to_datetime(prices.date)
    families = json.loads((model / "identity_families.json").read_text())
    actions = pd.read_csv(model / "unresolved_action_decisions.csv", parse_dates=["date"])
    coverage = pd.read_parquet(R3 / "price_audit_v4/all_eligible_daily_price_coverage.parquet")
    coverage["date"] = pd.to_datetime(coverage.date)
    calendar = pd.DatetimeIndex(sorted(prices.date.unique()))
    scope = calendar[(calendar >= "2017-07-01") & (calendar <= "2020-09-01")]
    assert len(scope) == 783
    # One key is one actual dated security quote, not a ticker resemblance.
    assert not prices.duplicated(["date", "ISIN"]).any()
    quotes = prices.set_index(["date", "ISIN"])
    ledger = pd.read_csv(OUT / "price_completion_ledger_v1.csv", parse_dates=["date"])
    ledger = ledger.merge(
        prices[["date", "ISIN", "research_security_id", "Close", "adjustment_factor"]],
        on=["date", "ISIN"],
        how="left",
        validate="many_to_one",
    )
    ledger["candidate_observation_present"] = ledger.Close.gt(0)
    ledger["unresolved_lineage_actions"] = (
        ledger.research_security_id.map(actions.groupby("research_security_id").size()).fillna(0).astype(int)
    )
    ledger["resolution_status"] = np.where(
        ledger.candidate_observation_present,
        "primary research candidate extracted; adjustment/consumption acceptance pending",
        "unresolved primary observation",
    )
    ledger["resolution_evidence"] = ledger.source_row_id.astype(str) + "|" + model_name + "/action_decisions.csv"

    # Compare independent primary reports for every flagged observation.
    comparisons = []
    flags = coverage[coverage.ohlc_discrepancy_gt_1pct].copy()
    assert len(flags) == 246
    for day, group in flags.groupby("date"):
        archive = OUT / "evidence" / ("PR" + day.strftime("%d%m%y") + ".zip")
        with zipfile.ZipFile(archive) as z:
            members = [m for m in z.namelist() if Path(m).name.lower().startswith("pd") and m.lower().endswith(".csv")]
            assert len(members) == 1
            p = pd.read_csv(io.BytesIO(z.read(members[0])), dtype=str)
        p.columns = p.columns.str.strip()
        for col in p:
            p[col] = p[col].str.strip()
        for r in group.to_dict("records"):
            own = quotes.loc[(day, r["ISIN"])]
            second = p[p.SYMBOL.eq(r["SYMBOL"]) & p.SERIES.eq(own.SERIES)]
            differing = []
            if len(second) == 1:
                for field in ["OPEN", "HIGH", "LOW", "CLOSE"]:
                    alternate = pd.to_numeric(second.iloc[0][field + "_PRICE"], errors="coerce")
                    if not np.isclose(own[field], alternate, atol=0.011, rtol=0):
                        differing.append(field)
            status = (
                "primary reports agree; opposing vendor OHLC rejected under primary-source convention"
                if len(second) == 1 and not differing
                else "unresolved opposing primary intraday observations"
            )
            comparisons.append(
                dict(
                    date=day,
                    ISIN=r["ISIN"],
                    SYMBOL=r["SYMBOL"],
                    primary_report_rows=len(second),
                    differing_fields="|".join(differing),
                    resolution=status,
                    source=str(archive.relative_to(ROOT)),
                    source_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                    raw_ohlc=[own[k] for k in ["OPEN", "HIGH", "LOW", "CLOSE"]],
                    alternate_ohlc=(
                        [second.iloc[0][k + "_PRICE"] for k in ["OPEN", "HIGH", "LOW", "CLOSE"]]
                        if len(second) == 1
                        else None
                    ),
                )
            )
    comparison = pd.DataFrame(comparisons)
    comparison.to_csv(target / "all_246_ohlc_flag_decisions.csv", index=False)
    ledger = ledger.merge(
        comparison[["date", "ISIN", "resolution"]], how="left", on=["date", "ISIN"], validate="many_to_one"
    )
    ledger.loc[ledger.resolution.notna(), "resolution_status"] = ledger.loc[ledger.resolution.notna(), "resolution"]
    # No-positive-quote days were absent from the adjusted-observation worklist.
    genuine = pd.read_csv(R3 / "price_audit_v4/eligible_missing_raw_quotes.csv", parse_dates=["date"])
    genuine["ISIN"] = genuine.security_id.str.removeprefix("ISIN:")
    genuine["SYMBOL"] = "JKIL"
    genuine["row_id"] = genuine.ISIN + ":" + genuine.date.dt.strftime("%Y-%m-%d")
    genuine["resolution_status"] = "genuine no-positive-quote; no fill invented"
    genuine["candidate_observation_present"] = False
    ledger = pd.concat([ledger, genuine], ignore_index=True)
    assert len(ledger) == 34003 and ledger.row_id.is_unique
    ledger["accepted_for_backtest"] = False
    ledger.to_csv(target / "complete_price_completion_ledger.csv", index=False)

    membership = pd.read_parquet(R3 / "candidate_v3/economic_provisional_membership.parquet")
    membership["family"] = membership.SECURITY_ID.str.removeprefix("ISIN:").map(families)
    assert membership.family.notna().all()
    assert not membership.duplicated(["TIME_STAMP", "family"]).any(), "Ordinary and DVR must remain distinct"
    states = {d: set(g.family) for d, g in membership.groupby("TIME_STAMP")}
    groups = {s: g.sort_values("date").set_index("date") for s, g in prices.groupby("research_security_id")}
    warmup = []
    metric_cache = {}

    def metrics_for(family):
        if family not in metric_cache:
            metric_cache[family] = precompute_metrics(
                groups[family][["Open", "High", "Low", "Close", "Volume"]], calendar=calendar
            )
        return metric_cache[family]

    metric_columns = [
        "Sharpe_3M",
        "Sharpe_6M",
        "Sharpe_9M",
        "Sharpe_1Y",
        "Vol_Median",
        "DMA200",
        "52w_High",
        "Circuit_Count",
        "Pos_Days_12M",
    ]
    first_state = states[min(states)]
    for frequency in ["weekly", "monthly"]:
        execution = get_rebalance_dates(scope, frequency)[0]
        rank = calendar[calendar.get_loc(execution) - 1]
        window = calendar[calendar.get_loc(rank) - 251 : calendar.get_loc(rank) + 1]
        assert len(window) == 252
        for family in sorted(first_state):
            g = groups[family]
            f = g[["Open", "High", "Low", "Close", "Volume"]]
            metrics = metrics_for(family).loc[rank]
            warmup.append(
                dict(
                    frequency=frequency,
                    research_security_id=family,
                    first_execution=execution,
                    rank_date=rank,
                    metric_window_start=window[0],
                    count_window_750_start=calendar[calendar.get_loc(rank) - 749],
                    supported_close_count=int(metrics["_count"]),
                    passes_original_count=metrics["_count"] >= 252,
                    passes_engine_default_count=metrics["_count"] >= 750,
                    unavailable_metric_fields="|".join(c for c in metric_columns if pd.isna(metrics[c])),
                    missing_sessions_in_metric_window=int(g.Close.reindex(window).isna().sum()),
                    note="Metric computability only; unresolved action/source seams not accepted",
                )
            )
            warmup[-1].update({column: metrics[column] for column in metric_columns})
    warmup = pd.DataFrame(warmup)
    warmup.to_csv(target / "initial_consumption_warmup.csv", index=False)
    rolling_consumption = []
    for frequency in ["weekly", "monthly"]:
        for execution in get_rebalance_dates(scope, frequency):
            rank = calendar[calendar.get_loc(execution) - 1]
            eligible = states[max(d for d in states if d <= execution)]
            for family in sorted(eligible):
                metrics = metrics_for(family)
                if rank not in metrics.index:
                    rolling_consumption.append(
                        dict(
                            frequency=frequency,
                            execution=execution,
                            rank_date=rank,
                            research_security_id=family,
                            count=0,
                            unavailable_metric_fields="no price history by rank date",
                        )
                    )
                    continue
                m = metrics.loc[rank]
                rolling_consumption.append(
                    dict(
                        frequency=frequency,
                        execution=execution,
                        rank_date=rank,
                        research_security_id=family,
                        count=int(m["_count"]),
                        unavailable_metric_fields="|".join(c for c in metric_columns if pd.isna(m[c])),
                    )
                )
    consumption = pd.DataFrame(rolling_consumption)
    consumption.to_parquet(target / "all_scheduled_signal_consumption.parquet", index=False)
    consumption[consumption.unavailable_metric_fields.ne("")].to_csv(
        target / "scheduled_uncomputable_signals.csv", index=False
    )

    # Potential ownership envelope, not invented actual portfolio holdings.
    # An exit is attempted only on a scheduled date; blocked quotes continue
    # ownership to the next scheduled executable observation or scope end.
    exits = []
    missing_marks = []
    dates = sorted(states)
    for previous, day in zip(dates, dates[1:]):
        for family in sorted(states[previous] - states[day]):
            g = groups[family]
            positive = g.CLOSE.gt(0) & g.TOTTRDQTY.gt(0)
            for frequency in ["weekly", "monthly"]:
                attempts = [d for d in get_rebalance_dates(scope, frequency) if d >= day]
                supported = [d for d in attempts if d in g.index and positive.at[d]]
                sale = supported[0] if supported else None
                end = sale if sale is not None else scope[-1]
                mark_days = scope[(scope >= day) & (scope <= end)]
                unavailable = mark_days[~mark_days.isin(g.index[g.CLOSE.gt(0)])]
                for d in unavailable:
                    prior = g.loc[g.index < d, "CLOSE"].dropna()
                    missing_marks.append(
                        dict(
                            frequency=frequency,
                            exit_date=day,
                            research_security_id=family,
                            date=d,
                            last_raw_mark_date=prior.index[-1] if len(prior) else None,
                            policy="carry held quantity and disclose stale mark; no invented execution",
                        )
                    )
                exits.append(
                    dict(
                        frequency=frequency,
                        exit_date=day,
                        research_security_id=family,
                        historical_symbol=g.loc[g.index <= day, "SYMBOL"].iloc[-1],
                        first_scheduled_attempt=attempts[0] if attempts else None,
                        first_source_quote_sale=sale,
                        blocked_scheduled_attempts=sum(
                            d <= end and (d not in g.index or not positive.get(d, False)) for d in attempts
                        ),
                        exposure_audited_through=end,
                        post_exit_expected_marks=len(mark_days),
                        unavailable_current_marks=len(unavailable),
                        scope_end_still_potentially_held=sale is None,
                        status="potential holding envelope only; actual holdings require supported strategy replay",
                    )
                )
    exits = pd.DataFrame(exits)
    exits.to_csv(target / "potential_post_index_exit_consumption.csv", index=False)
    pd.DataFrame(missing_marks).to_csv(target / "potential_post_exit_missing_marks.csv", index=False)

    # Scope-relevant unresolved actions use first eligibility less 252 sessions
    # through last eligible / potential delayed sale, not all actions since 2014.
    first_eligible = {}
    last_eligible = {}
    for i, day in enumerate(dates):
        end = dates[i + 1] if i + 1 < len(dates) else scope[-1]
        for family in states[day]:
            first_eligible.setdefault(family, day)
            last_eligible[family] = end
    for r in exits.to_dict("records"):
        last_eligible[r["research_security_id"]] = max(
            last_eligible[r["research_security_id"]], r["exposure_audited_through"]
        )
    actions["consumption_start"] = actions.research_security_id.map(
        {s: calendar[max(0, calendar.searchsorted(day) - 252)] for s, day in first_eligible.items()}
    )
    actions["consumption_end"] = actions.research_security_id.map(last_eligible)
    actions["potentially_consumed"] = actions.date.ge(actions.consumption_start) & actions.date.le(
        actions.consumption_end
    )
    actions.to_csv(target / "scoped_unresolved_actions.csv", index=False)

    # Vendor responses are diagnostics, never identity certification by price fit.
    proposals = pd.read_csv(OUT / "vendor_query_identity_proposals.csv")
    additional = []
    for path in sorted((OUT / "evidence").glob("*_extended_yahoo.json")):
        key = path.name.removesuffix("_extended_yahoo.json")
        if key not in set(proposals.query_key):
            ids = sorted(set(prices.loc[prices.SYMBOL.eq(key), "ISIN"]))
            additional.append(
                dict(
                    historical_symbol=key,
                    historical_isins="|".join(ids),
                    query_key=key,
                    decision="Additional partial-gap query; dated-symbol comparison only, no identity promotion",
                )
            )
    if additional:
        proposals = pd.concat([proposals, pd.DataFrame(additional)], ignore_index=True)
    vendor_summary = []
    seams = []
    for r in proposals.to_dict("records"):
        path = OUT / "evidence" / (r["query_key"] + "_extended_yahoo.json")
        if not path.exists():
            continue
        payload = json.loads(path.read_text())
        try:
            bars, events = parse_vendor_chart(payload, r["query_key"] + ".NS")
        except ValueError as exc:
            vendor_summary.append({**r, "status": str(exc)})
            continue
        ids = {families[s] for s in r["historical_isins"].split("|") if s in families}
        vendor_summary.append(
            {
                **r,
                "status": "retained diagnostic; proposed identity not promoted",
                "bars": len(bars),
                "first": str(bars.date.min()),
                "last": str(bars.date.max()),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
        for family in ids:
            g = groups[family]
            audit = close_factor_seam_audit(g.CLOSE, bars.set_index("date").adjusted_close, g.adjustment_factor)
            audit = audit[audit.checked]
            audit["vendor_query_key"] = r["query_key"]
            audit["research_security_id"] = family
            seams.append(audit.reset_index(names="date"))
    pd.DataFrame(vendor_summary).to_csv(target / "extended_vendor_response_audit.csv", index=False)
    # The original local observations are also tested for close-factor changes.
    # This uses the retained Round 3 dated joins, not a new fuzzy symbol join.
    local = coverage[
        coverage.price_source.eq("protected_local_same_dated_exchange_label") & coverage.Close.gt(0)
    ].copy()
    local["research_security_id"] = local.ISIN.map(families)
    for family, g in local.groupby("research_security_id"):
        own = groups[family]
        assert not g.date.duplicated().any()
        audit = close_factor_seam_audit(own.CLOSE, g.set_index("date").Close, own.adjustment_factor)
        audit = audit[audit.checked]
        audit["vendor_query_key"] = "original_local:" + family
        audit["research_security_id"] = family
        seams.append(audit.reset_index(names="date"))
    seam = pd.concat(seams, ignore_index=True)
    seam[seam.mismatch].to_csv(target / "opposing_vendor_close_factor_seams.csv", index=False)
    seam.groupby("vendor_query_key").agg(
        checked_intervals=("checked", "sum"), opposing_intervals=("mismatch", "sum")
    ).to_csv(target / "vendor_factor_seam_counts.csv")
    summary = dict(
        canonical_missing_adjusted=33754,
        canonical_ohlc_flags=246,
        complete_ledger_rows=len(ledger),
        primary_candidate_observations_on_original_missing=int(ledger.iloc[:33754].candidate_observation_present.sum()),
        genuine_missing_positive_quotes=len(genuine),
        flags_primary_agree=int(comparison.resolution.str.startswith("primary reports agree").sum()),
        flags_primary_oppose=int(comparison.resolution.str.startswith("unresolved").sum()),
        warmup_sessions=int((calendar < "2017-07-01").sum()),
        total_raw_model_rows=len(prices),
        unresolved_actions_all=len(actions),
        unresolved_actions_potentially_consumed=int(actions.potentially_consumed.sum()),
        post_exit_cases=len(exits),
        post_exit_blocked_cases=int(exits.blocked_scheduled_attempts.gt(0).sum()),
        post_exit_no_sale_within_scope=int(exits.scope_end_still_potentially_held.sum()),
        post_exit_missing_marks=len(missing_marks),
        vendor_checked_intervals=len(seam),
        vendor_opposing_factor_intervals=int(seam.mismatch.sum()),
        accepted=False,
        publication_performed=False,
        strategy_comparison_run=False,
    )
    (target / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main(*sys.argv[1:])
