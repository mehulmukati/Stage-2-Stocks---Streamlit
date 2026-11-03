"""Replay completed rebalance windows and the retained opposing observations."""
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from backtest_engine import extend_sessions, get_rebalance_dates
from historical_price_evidence import parse_vendor_chart
from historical_price_model import close_factor_seam_audit

HERE = Path(__file__).resolve().parent
R4 = HERE.parent / "backtest_repair_v4_20261103"
R3 = HERE.parent / "backtest_repair_v3_20261103"
AUDIT = R4 / "consumption_audit_v7"
MODEL = HERE / "reviewed_model_v2"


def main():
    prices = pd.read_parquet(MODEL / "primary_adjusted_research_candidate.parquet")
    groups = {k: g.sort_values("date").set_index("date") for k, g in prices.groupby("research_security_id")}
    cal = pd.DatetimeIndex(sorted(prices.date.unique()))
    end = pd.Timestamp("2020-09-01")
    scope = cal[(cal >= "2017-07-01") & (cal <= end)]
    calendar_grid = extend_sessions(cal, end + pd.Timedelta(days=190))
    families = json.loads((R4 / "primary_model_v8/identity_families.json").read_text())
    m = pd.read_parquet(R3 / "candidate_v3/economic_provisional_membership.parquet")
    m["family"] = m.SECURITY_ID.str.removeprefix("ISIN:").map(families)
    assert m.family.notna().all()
    states = {d: set(g.family) for d, g in m.groupby("TIME_STAMP")}
    dates = sorted(states)
    positive = {k: (g.index[g.Close.gt(0)].to_numpy(), g.ISIN.iloc[0], g.SYMBOL.iloc[0]) for k,g in groups.items()}
    dependencies, holding = defaultdict(set), defaultdict(set)
    schedule, short = [], []
    for frequency in ("weekly", "monthly"):
        completed = [d for d in get_rebalance_dates(calendar_grid, frequency) if scope[0] < d <= scope[-1]]
        for execution in completed:
            j = cal.get_loc(execution)
            rank, start = cal[j-1], cal[j-252]
            state = states[max(d for d in dates if d <= execution)]
            schedule.append((frequency, execution, rank, start, cal[j-750], len(state)))
            rank_window = set(cal[j-251:j])
            for family in state:
                quote_dates, first_isin, first_symbol = positive[family]

                n = np.searchsorted(quote_dates, rank.to_datetime64(), side="right")
                if n < 252:
                    short.append((frequency, execution, rank, family, n, quote_dates[0] if n else pd.NaT,
                                  first_isin if n else None, first_symbol if n else None))
                else:
                    # Returns/rolling indicators consume this fixed calendar window.
                    dependencies[family].update(rank_window)
            # Any selected position could survive to the next executable rebalance.
            following = next((d for d in completed if d > execution), end)
            rank_window = set(cal[j-251:j])
            for family in state:
                holding[family].update(cal[(cal >= execution) & (cal <= following)])
    exits = pd.read_csv(AUDIT / "potential_post_index_exit_consumption.csv", parse_dates=["exit_date", "exposure_audited_through"])
    for r in exits.itertuples():
        holding[r.research_security_id].update(cal[(cal >= r.exit_date) & (cal <= r.exposure_audited_through)])
    pd.DataFrame(schedule, columns=["frequency", "execution", "rank", "window252", "window750", "members"]).to_csv(HERE / "completed_schedule_v3.csv", index=False)
    pd.DataFrame(short, columns=["frequency", "execution", "rank", "family", "observations", "first_primary_quote",
                                "first_isin", "first_symbol"]).to_csv(HERE / "short_history_cases_v3.csv", index=False)
    actions = pd.read_csv(MODEL / "action_decisions.csv", parse_dates=["date"], low_memory=False)
    action_groups = {k: g for k,g in actions.groupby("research_security_id")}
    local = pd.read_parquet(R3 / "price_audit_v4/all_eligible_daily_price_coverage.parquet")
    local = local.loc[local.price_source.eq("protected_local_same_dated_exchange_label") & local.Close.gt(0)].copy()
    local["family"] = local.ISIN.map(families)
    local = {k: g.set_index("date").Close for k,g in local.groupby("family")}
    before = pd.read_csv(AUDIT / "opposing_vendor_close_factor_seams.csv", parse_dates=["date"])
    results = []
    for (family, key), rows in before.groupby(["research_security_id", "vendor_query_key"]):
        g = groups[family]
        original = key.startswith("original_local:")
        if original:
            opposing = local[family]
        else:
            path = R4 / "evidence" / (key + "_extended_yahoo.json")
            bars, _ = parse_vendor_chart(json.loads(path.read_text()), key + ".NS")
            opposing = bars.set_index("date").adjusted_close
        audit = close_factor_seam_audit(g.CLOSE, opposing, g.adjustment_factor)
        overlap = audit.index
        own_actions = action_groups.get(family, actions.iloc[:0])
        for r in rows.itertuples():
            d = r.date
            previous = overlap[overlap.get_loc(d)-1]
            value = audit.at[d, "factor_change_residual"]
            interval = own_actions.loc[own_actions.date.gt(previous) & own_actions.date.le(d)]
            supported = interval.loc[interval.applied]
            unresolved = interval.loc[~interval.applied & ~interval.reason.fillna("").str.startswith(
                ("no mandatory", "included in aggregate"))]
            rank, held = d in dependencies[family], d in holding[family]
            if not audit.at[d, "mismatch"]:
                status = "within tolerance after reviewed model"
            elif len(unresolved):
                status = "missing or unresolved primary entitlement"
            elif not original:
                status = "vendor historical identity and close convention unverified"
            elif len(supported):
                status = "documented primary action; opposing factor or quote remains different"
            else:
                status = "opposing local close return; needs dated opposing quote/scale reconciliation"
            results.append((family, key, previous, d, r.factor_change_residual, value, rank, held, status,
                            "|".join(unresolved.source_row_id), "|".join(supported.source_row_id)))
    result = pd.DataFrame(results, columns=["family","source","previous_date","date","old_residual","new_residual",
                                           "rank_window","potential_holding","decision","unresolved_actions","applied_actions"])
    assert len(result) == len(before) == 2927
    result.to_csv(HERE / "opposing_interval_decisions_v3.csv", index=False)
    print(result.groupby("decision").size().to_string())
    print("potentially consumed", int((result.rank_window | result.potential_holding).sum()))
    print("schedules", [(f, sum(row[0] == f for row in schedule), str(max(row[1] for row in schedule if row[0] == f))) for f in ("weekly", "monthly")])
    print("short rows", len(short))


if __name__ == "__main__":
    main()




