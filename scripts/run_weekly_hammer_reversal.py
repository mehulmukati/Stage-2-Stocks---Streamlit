"""Run the frozen weekly hammer milestone from the repository root."""

import argparse
import json
import platform
import shlex
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd

from backtest_provenance import sha256, write_manifest_exclusive
from equity_data import calendar_definition, calendar_provenance
from market_data import MEMBERSHIP_PATH, PRICE_PATH, file_token, load_snapshot
from weekly_hammer_reversal import (
    HORIZONS,
    SEGMENTS,
    VARIANTS,
    WEEK,
    HammerGeometry,
    evaluate_outcomes,
    exchange_weeks,
    hammer_features,
    membership_schedule,
    weekly_candles,
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "outputs/weekly_hammer_reversal"
R3 = ROOT / "outputs/backtest_repair_v3_20261103"
R5 = ROOT / "outputs/backtest_repair_v5_20261103"
MEMBERSHIP = R3 / "candidate_v3/economic_provisional_membership.parquet"
MEMBERSHIP_SCOPE = R3 / "candidate_v3/manifest.json"
PRICE = R5 / "reviewed_model_v2/primary_adjusted_research_candidate.parquet"
ACTIONS = R5 / "reviewed_model_v2/action_decisions.csv"
CALENDAR = R3 / "calendar_reconciliation.json"
EXCHANGE = R3 / "candidate_v3/raw_exchange_observations.parquet"
EVENTS = R3 / "candidate_v3/event_identity_and_timing_decisions.csv"
VALIDATION = ROOT / "data/validation_report.csv"
LOCAL_EVENTS = ROOT / "data/events.csv"
LOCAL_ACTIONS = ROOT / "data/corporate_actions.json"
COSTS = ((10.0, 0.0), (10.0, 5.0), (20.0, 10.0))
RESAMPLES = 600
MIN_TARGET_WEEKS = 12


def frozen_inputs(mode, snapshot):
    if mode == "bounded":
        membership = pd.read_parquet(MEMBERSHIP).rename(columns={"SECURITY_ID": "security_id"})
        prices = pd.read_parquet(
            PRICE, columns=["date", "SYMBOL", "research_security_id", "Open", "High", "Low", "Close", "Volume"]
        ).rename(columns={"SYMBOL": "ticker"})
        prices["security_id"] = "ISIN:" + prices.pop("research_security_id")
        prices["is_tradable"] = prices.Volume.gt(0)
        # The bounded archive has a complete, independently checked exchange calendar.
        observed = pd.read_parquet(EXCHANGE, columns=["date"]).date.drop_duplicates()
        sessions = pd.DatetimeIndex(prices.date.unique()).union(pd.DatetimeIndex(observed)).sort_values()
        actions = pd.read_csv(
            ACTIONS, usecols=["research_security_id", "date", "kind", "applied"], parse_dates=["date"]
        )
        actions["security_id"] = "ISIN:" + actions.pop("research_security_id")
        volume_break = (
            ~actions.kind.isin(
                ["cash_dividend", "reviewed_cash_dividend", "cash_terms_unresolved", "no_mandatory_holder_distribution"]
            )
            & actions.kind.notna()
        )
        price_break = ~actions.applied & actions.kind.ne("no_mandatory_holder_distribution") & actions.kind.notna()
        start, end = map(pd.Timestamp, json.loads(MEMBERSHIP_SCOPE.read_text(encoding="utf-8"))["scope"])
        price_cutoff = min(end, prices.date.max())
        return membership, prices, sessions, actions[volume_break], actions[price_break], start, end, price_cutoff

    membership = snapshot.membership.loc[
        snapshot.membership.INDEX_NAME.eq("NIFTY500") & snapshot.membership.SOURCE.eq("F1_SNAPSHOT")
    ].copy()
    if membership.empty:
        raise RuntimeError("No dated Nifty 500 F1 snapshots are available")
    membership["security_id"] = "NSE:" + membership.SYMBOL
    prices = snapshot.equity_prices().rename(columns={"symbol": "ticker"})
    prices["security_id"] = "NSE:" + prices.ticker
    sessions = snapshot.trading_calendar()
    actions = pd.DataFrame(json.loads(LOCAL_ACTIONS.read_text(encoding="utf-8"))["actions"])
    actions = actions.rename(columns={"effective_date": "date"})
    actions["security_id"] = "NSE:" + actions.old_symbol
    actions["date"] = pd.to_datetime(actions.date)
    start = max(membership.TIME_STAMP.min(), pd.Timestamp("2020-09-02"))
    return membership, prices, sessions, actions, actions, start, membership.TIME_STAMP.max(), snapshot.as_of


def study(mode, snapshot, destination, costs):
    membership, prices, sessions, volume_breaks, price_breaks, start, end, cutoff = frozen_inputs(mode, snapshot)
    cutoff = min(cutoff, snapshot.as_of)
    # In a verified year the known future session schedule can establish completion of a shortened week.
    closed_through = cutoff
    calendar = calendar_definition()
    if cutoff.year in calendar["verified_years"]:
        sunday = cutoff.to_period(WEEK).end_time.normalize()
        weekdays = pd.bdate_range(cutoff + pd.Timedelta(days=1), sunday)
        future = weekdays.difference(pd.DatetimeIndex(calendar["closed_sessions"])).union(
            pd.DatetimeIndex(calendar["special_sessions"])
        )
        if not ((future > cutoff) & (future <= sunday)).any():
            closed_through = sunday
    weeks = exchange_weeks(sessions[sessions <= cutoff], cutoff, closed_through)
    decisions = membership_schedule(membership, weeks, start, min(end, cutoff))
    if decisions.empty:
        raise RuntimeError(f"No usable membership weeks for {mode}")
    decisions["signal_id"] = np.arange(len(decisions))
    decisions["segment"] = ""
    for label, (first, last) in SEGMENTS.items():
        decisions.loc[decisions.signal_date.between(first, last), "segment"] = label
    benchmark_rows = snapshot.prices.loc[
        snapshot.prices.series_type.eq("index_price")
        & snapshot.prices.symbol.eq("Nifty 500")
        & snapshot.prices.date.le(cutoff)
    ]
    benchmark = benchmark_rows.set_index("date").Open.sort_index()
    if benchmark.empty:
        print("Nifty 500 index opens unavailable. Benchmark-relative results will be missing.")

    symbols = set(decisions.security_id)
    prices = prices.loc[prices.security_id.isin(symbols) & prices.date.isin(sessions) & prices.date.le(cutoff)]
    prices = prices.sort_values(["security_id", "date"]).set_index("date")
    quotes = dict(iter(prices.groupby("security_id", sort=False)))
    no_prices = prices.iloc[:0]
    price_dates = price_breaks.groupby("security_id").date.agg(list).to_dict()
    volume_dates = volume_breaks.groupby("security_id").date.agg(list).to_dict()
    feature_parts, outcome_parts = [], []
    for security_id, group in decisions.groupby("security_id", sort=False):
        daily = quotes.get(security_id, no_prices)
        # Limit each feature calculation to the observed life of this quote series and the requested decisions.
        tickers = daily.ticker.reindex(pd.DatetimeIndex(group.signal_date), method="ffill")
        group = group.assign(ticker=tickers.to_numpy())
        if mode == "snapshots":
            group["ticker"] = group.security_id.str.removeprefix("NSE:")
        observed = np.isfinite(daily.Open) & daily.is_tradable
        observed_counts = observed.groupby(daily.index.to_period(WEEK)).sum()
        group["tradable_open_sessions"] = observed_counts.reindex(group.week, fill_value=0).to_numpy()
        local_first = min(daily.index.min(), group.signal_date.min()) if len(daily) else group.signal_date.min()
        local_weeks = weeks.loc[local_first.to_period(WEEK) : group.week.max()]
        raw_weeks = weekly_candles(daily, local_weeks)
        trading_price_events = price_dates.get(security_id, [])
        features = hammer_features(
            raw_weeks,
            pd.DatetimeIndex(volume_dates.get(security_id, [])).to_period(WEEK),
            pd.DatetimeIndex(trading_price_events).to_period(WEEK),
        )
        selected = group.join(features, on="week")
        selected["eligibility"] = np.select(
            [selected.price_basis_flag, selected.tradable_open_sessions.eq(0), ~selected.complete_candle],
            ["unresolved_price_basis_week", "no_tradable_open", "missing_or_invalid_session_candle"],
            default="eligible",
        )
        feature_parts.append(selected)
        eligible = selected.loc[selected.eligibility.eq("eligible")]
        if not eligible.empty:
            outcome_parts.append(evaluate_outcomes(eligible, daily, weeks, cutoff, benchmark, trading_price_events))

    ledger = pd.concat(feature_parts, ignore_index=True).sort_values("signal_id")
    ledger["oversold_nonhammer"] = ledger.rsi.le(35) & ~ledger.A
    opportunities = ledger[ledger.eligibility.eq("eligible")]
    outcomes = pd.concat(outcome_parts, ignore_index=True)
    outcomes["path_sessions"] = (
        pd.Series(
            sessions.searchsorted(outcomes.exit_date) - sessions.searchsorted(outcomes.entry_date), index=outcomes.index
        )
        .where(outcomes.status.eq("filled"))
        .astype("Int64")
    )
    outcomes["excursions_complete"] = outcomes.path_sessions.eq(outcomes.excursion_sessions).fillna(False)
    for cost, slip in costs:
        outcomes[cost_column(cost, slip)] = (1 + outcomes.gross_return) * (1 - cost / 10000) * (1 - slip / 10000) / (
            (1 + cost / 10000) * (1 + slip / 10000)
        ) - 1

    evaluation = outcomes.merge(
        opportunities[
            [
                "signal_id",
                "signal_date",
                "week",
                "ticker",
                "security_id",
                "segment",
                "common_ready",
                "green",
                "oversold_nonhammer",
                *VARIANTS,
            ]
        ],
        on="signal_id",
        validate="many_to_one",
    )
    coverage = export_coverage(destination, ledger, evaluation, weeks)
    ledger["week"] = ledger.week.astype(str)
    ledger.to_parquet(destination / "stock_week_ledger.parquet", index=False)
    ledger.loc[ledger.A].to_csv(destination / "signal_ledger.csv", index=False)
    outcomes[["entry_week", "exit_week"]] = outcomes[["entry_week", "exit_week"]].astype(str)
    outcomes.sort_values(["signal_id", "holding_weeks"]).to_parquet(destination / "outcome_ledger.parquet", index=False)
    summary, comparisons = summarize(evaluation, costs)
    summary.to_csv(destination / "summary.csv", index=False)
    comparisons.to_csv(destination / "comparisons.csv", index=False)
    return dict(
        label=mode,
        start=str(ledger.signal_date.min().date()),
        end=str(ledger.signal_date.max().date()),
        price_cutoff=str(cutoff.date()),
        latest_complete_week=str(weeks.loc[weeks.complete, "signal_date"].max().date()),
        raw_price_start=str(prices.index.min().date()),
        raw_price_end=str(prices.index.max().date()),
        benchmark_start=None if benchmark.empty else str(benchmark.index.min().date()),
        benchmark_end=None if benchmark.empty else str(benchmark.index.max().date()),
        universe_stock_weeks=len(ledger),
        eligible_stock_weeks=len(opportunities),
        price_event_rows=len(price_breaks),
        coverage=coverage,
        summary=summary,
        comparisons=comparisons,
    )


def cost_column(cost, slip):
    return f"net_cost_{cost:g}bps_slip_{slip:g}bps"


def export_coverage(destination, ledger, evaluation, weeks):
    ledger["year"] = ledger.signal_date.dt.year
    ledger["snapshot_age_days"] = (ledger.signal_date - ledger.TIME_STAMP).dt.days
    ledger["has_observed_ticker"] = ledger.ticker.notna()
    week_index = ["year", "signal_date", "TIME_STAMP"]
    coverage = (
        ledger.groupby(week_index)
        .agg(
            members=("security_id", "size"),
            observed_label=("has_observed_ticker", "sum"),
            complete_candles=("complete_candle", "sum"),
            common_ready=("common_ready", "sum"),
            price_flags=("price_basis_flag", "sum"),
            volume_flags=("volume_basis_flag", "sum"),
            snapshot_age_days=("snapshot_age_days", "first"),
        )
        .join(ledger.groupby([*week_index, "eligibility"]).size().unstack(fill_value=0))
        .reset_index()
    )
    coverage.to_csv(destination / "membership_and_candle_coverage.csv", index=False)
    if "eligible" not in coverage:
        coverage["eligible"] = 0
    year = (
        coverage.groupby("year")
        .agg(
            weeks=("signal_date", "size"),
            member_weeks=("members", "sum"),
            eligible_weeks=("eligible", "sum"),
            common_ready=("common_ready", "sum"),
            mean_members=("members", "mean"),
            max_snapshot_age_days=("snapshot_age_days", "max"),
        )
        .reset_index()
    )
    year.to_csv(destination / "coverage_by_year.csv", index=False)
    evaluation.groupby([evaluation.signal_date.dt.year.rename("year"), "holding_weeks", "status"]).size().rename(
        "count"
    ).to_csv(destination / "execution_coverage.csv")
    exchange = weeks.copy()
    exchange.index = exchange.index.astype(str)
    exchange.to_csv(destination / "exchange_weeks.csv", index_label="week")
    return year


def statistics(sample, costs):
    filled = sample.loc[sample.status.eq("filled")]
    gains = filled.gross_return[filled.gross_return > 0]
    losses = filled.gross_return[filled.gross_return < 0]
    gross = filled.gross_return
    excursion = filled.loc[filled.excursions_complete]
    count = len(sample)
    exits = filled.sort_values("entry_date")
    previous_exit = exits.groupby("security_id").exit_date.cummax().groupby(exits.security_id).shift()
    statuses = sample.status.value_counts()
    censored = sum(statuses.get(k, 0) for k in ["segment_censored", "right_censored_entry", "right_censored_exit"])
    result = dict(
        observations=count,
        distinct_weeks=sample.week.nunique(),
        stocks=sample.security_id.nunique(),
        signal_first=sample.signal_date.min(),
        signal_last=sample.signal_date.max(),
        first_fill=filled.entry_date.min(),
        last_exit=filled.exit_date.max(),
        round_trips=len(filled),
        entry_fills=int((sample.entry_date.notna() & sample.status.ne("segment_censored")).sum()),
        fill_rate=len(filled) / count if count else np.nan,
        censor_rate=censored / count if count else np.nan,
        censored=censored,
        missing_entry=statuses.get("missing_entry", 0),
        missing_exit=statuses.get("missing_exit", 0),
        unresolved_price_event=statuses.get("unresolved_price_event", 0),
        same_stock_overlapping_round_trips=int(exits.entry_date.lt(previous_exit).sum()),
        mean=gross.mean(),
        median=gross.median(),
        expectancy=gross.mean(),
        win_rate=gross.gt(0).mean(),
        payoff=gains.mean() / -losses.mean() if len(losses) else np.nan,
        profit_factor=gains.sum() / -losses.sum() if len(losses) else np.nan,
        complete_excursions=len(excursion),
        mean_mae=excursion.mae.mean(),
        mean_mfe=excursion.mfe.mean(),
        percentile_05=gross.quantile(0.05),
        percentile_25=gross.quantile(0.25),
        percentile_75=gross.quantile(0.75),
        percentile_95=gross.quantile(0.95),
        benchmark_pairs=filled.relative_return.count(),
        mean_matched_benchmark=filled.benchmark_open_return.mean(),
        mean_relative=filled.relative_return.mean(),
    )
    for cost, slip in costs:
        name = cost_column(cost, slip)
        result[f"mean_{name}"] = filled[name].mean()
    return result


def paired_weeks(target, reference):
    a = target.groupby("week").gross_return.agg(["mean", "size"])
    b = reference.groupby("week").gross_return.mean()
    return a.join(b.rename("reference"), how="inner").assign(delta=lambda f: f["mean"] - f.reference)


def block_interval(paired, horizon, calendar):
    """Moving blocks of consecutive weeks with all same-week stock outcomes kept together."""
    block = horizon + 1
    if len(paired) < max(MIN_TARGET_WEEKS, 2 * block) or len(calendar) < 2 * block:
        return np.nan, np.nan
    weights = paired["size"].reindex(calendar, fill_value=0)
    totals = (paired.delta * paired["size"]).reindex(calendar, fill_value=0)
    rng = np.random.default_rng(20261120)
    starts = rng.integers(0, len(calendar) - block + 1, (RESAMPLES, int(np.ceil(len(calendar) / block))))
    indices = (starts[:, :, None] + np.arange(block)).reshape(RESAMPLES, -1)[:, : len(calendar)]
    numerator, denominator = totals.to_numpy()[indices].sum(axis=1), weights.to_numpy()[indices].sum(axis=1)
    valid = denominator > 0
    return np.quantile(numerator[valid] / denominator[valid], [0.025, 0.975])


def paired_comparison(target, reference, horizon, calendar):
    target = target.loc[target.status.eq("filled")]
    reference = reference.loc[reference.status.eq("filled")]
    paired = paired_weeks(target, reference)
    matched = target[target.week.isin(paired.index)]
    top = (
        matched.security_id.value_counts()
        .rename_axis("security_id")
        .reset_index(name="count")
        .sort_values(["count", "security_id"], ascending=[False, True])
        .head(5)
        .security_id
    )
    reduced = paired_weeks(target[~target.security_id.isin(top)], reference[~reference.security_id.isin(top)])
    lo, hi = block_interval(paired, horizon, calendar)
    return dict(
        target_fills=len(target),
        reference_fills=len(reference),
        matched_weeks=len(paired),
        matched_target_fills=len(matched),
        matched_reference_fills=int(reference.week.isin(paired.index).sum()),
        matched_first=paired.index.min(),
        matched_last=paired.index.max(),
        matched_delta=np.average(paired.delta, weights=paired["size"]) if len(paired) else np.nan,
        ci_lower=lo,
        ci_upper=hi,
        excluding_top5_delta=np.average(reduced.delta, weights=reduced["size"]) if len(reduced) else np.nan,
        excluding_top5_weeks=len(reduced),
        top5_fill_share=matched.security_id.isin(top).mean(),
        inference_status="sparse" if np.isnan(lo) else "block_interval",
    )


def summarize(evaluation, costs):
    summary, comparisons = [], []
    for (segment, horizon), frame in evaluation.groupby(["segment", "holding_weeks"], sort=True):
        calendar = pd.period_range(frame.week.min(), frame.week.max(), freq=WEEK)
        for common in (False, True):
            sample = frame.loc[frame.common_ready] if common else frame
            sample_name = "common_ready" if common else "variant_ready"
            cohorts = {"all_eligible": sample, "green": sample[sample.green]}
            cohorts.update({variant: sample[sample[variant]] for variant in VARIANTS})
            matched_control = sample.oversold_nonhammer & sample.week.isin(cohorts["B"].week)
            cohorts["oversold_nonhammer"] = sample.loc[matched_control]
            if common:
                for parent, child in zip(VARIANTS, VARIANTS[1:]):
                    cohorts[f"{parent}_excluded_by_{child}"] = sample.loc[sample[parent] & ~sample[child]]
            for key, subset in cohorts.items():
                summary.append(
                    dict(segment=segment, horizon=horizon, sample=sample_name, cohort=key, **statistics(subset, costs))
                )
            pairs = [("A", "all_eligible"), ("A", "green"), ("B", "oversold_nonhammer")]
            if common:
                for parent, child in zip(VARIANTS, VARIANTS[1:]):
                    pairs.extend([(child, parent), (child, f"{parent}_excluded_by_{child}")])
            for target, reference in pairs:
                comparisons.append(
                    dict(
                        segment=segment,
                        horizon=horizon,
                        sample=sample_name,
                        target=target,
                        reference=reference,
                        **paired_comparison(cohorts[target], cohorts[reference], horizon, calendar),
                    )
                )
    return pd.DataFrame(summary), pd.DataFrame(comparisons)


def pct(value):
    return f"{value * 100:.2f}%" if pd.notna(value) else "—"


def report(results, destination, costs):
    primary_cost = cost_column(*costs[0])
    lines = [
        "# Weekly bullish hammer reversal · Baseline",
        "",
        "The eight-week return is the primary endpoint. Results are single-stock event returns with independent, "
        "potentially overlapping trades. Figures are historical and provisional.",
        "",
        "## Data audit",
        "",
        "The local runtime Parquets provide the later sample and the Nifty 500 price index. Local equity coverage "
        "starts in September 2016. The Nifty 500 F1 snapshot chain ends in March 2024. The earlier event chain "
        "reconciles 28 notices and 39 official monthly observations for July 2017–September 2020. The legacy "
        "reconstruction before that period was excluded. No 2025-onward holdout is evaluated.",
        "",
        "- **Bounded study.** Dated, independently reconciled economic-ex-date membership joined by the existing "
        "research security ID across documented alias changes. The reviewed NSE bhavcopy model applies a common "
        "factor to OHLC for supported splits, bonuses and cash distributions, with certain reviewed entitlements. "
        "Returns are on that modeled adjusted basis. They may capture some dividends and are neither nominal fills "
        "nor a certified total-return series. Marked unmodeled price events exclude affected signal weeks, reset "
        "price indicators, and exclude trades crossing those dates. Unresolved action identity and entitlement "
        "gaps remain. Reported volume is nominal. Known share-count changes invalidate the event week for RVOL "
        "and reset its reference.",
        "- **Later snapshot diagnostic.** Each observed F1 snapshot is used from its recorded date to the next "
        "snapshot, within the support period. Effective dates between snapshots, historical ISIN continuity and "
        "delisted coverage "
        "are unverified. NSE symbol is a provisional key. The shared runtime carries adjusted equity OHLC with "
        "provider-dependent conventions and no complete historical row-level factor or volume-change ledger. "
        "The local merger file supplies known symbol-specific exclusions. Unrecorded corporate actions and provider "
        "seams remain possible and may affect geometry, RVOL and returns. No series is spliced across the two studies.",
        "- **Calendar and gaps.** Monday–Sunday NSE weeks group observed weekend sessions with their calendar week. "
        "The bounded period uses independently checked primary NSE sessions. The shared 2026 definition supplies "
        "verified "
        "holidays and scheduled special sessions; earlier dates outside the bounded archive use market-wide observed "
        "sessions. Every expected session needs positive volume and valid OHLC for a signal candle. There is no "
        "stale-final-bar substitution. Missing names and sessions remain in the coverage ledger.",
        "- **Fills and benchmark.** Use the first positive tradable open in the next exchange week and the first such "
        "open in the week after the intended hold. Missing exit quotes, including suspensions or delistings, have "
        "separate statuses with no recovery value assumed, so executable-only means may be biased. MAE and MFE "
        "summaries use complete paths from the entry open through the exit open, excluding later exit-day prices. "
        "The benchmark is the Nifty 500 price index open on each actual stock fill date. Relative returns are a "
        "price-index comparison and use a different distribution basis from adjusted stocks. Index opening levels "
        "are reference values, which can differ from executable fund prices.",
        "- **Uncertainty.** Paired differences compare filled trades in shared signal weeks, weighted by target fills. "
        "Moving blocks keep all stocks for each selected signal week and use hold length plus one week. The 95% "
        "intervals are descriptive and unadjusted for multiple comparisons. Removing the five most frequent target "
        "stocks provides a concentration sensitivity. Sparse comparisons omit intervals.",
        "- **Costs.** Net means apply proportional fees and slippage per side before tax. "
        f"The displayed scenario uses {costs[0][0]:g} bps fees and {costs[0][1]:g} bps slippage per side.",
        "",
    ]
    for result in results:
        lines.extend(
            [
                f"## {result['label'].title()} study",
                "",
                f"Signal closes span **{result['start']} to {result['end']}**. The price cutoff is "
                f"{result['price_cutoff']}. Last complete exchange week in the price window ends at the "
                f"{result['latest_complete_week']} session. Usable candidate price dates span "
                f"{result['raw_price_start']} to {result['raw_price_end']}.",
                "",
                "| Year | Signal weeks | Member stock-weeks | Usable candles | Common-ready | Max snapshot age |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in result["coverage"].itertuples():
            lines.append(
                f"| {row.year} | {row.weeks} | {row.member_weeks:,} | {row.eligible_weeks:,} | "
                f"{row.common_ready:,} | {row.max_snapshot_age_days} |"
            )
        lines.extend(
            [
                "",
                "### Eight-week returns",
                "",
                "| Segment | Cohort | Signals / controls | Round trips | Gross mean | Net mean | Median | Win rate |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        summary = result["summary"]
        for row in summary.loc[summary.horizon.eq(8) & summary["sample"].eq("variant_ready")].to_dict("records"):
            lines.append(
                f"| {row['segment']} | {row['cohort']} | {row['observations']:,} | {row['round_trips']:,} | "
                f"{pct(row['mean'])} | {pct(row[f'mean_{primary_cost}'])} | "
                f"{pct(row['median'])} | {pct(row['win_rate'])} |"
            )
        lines.extend(
            [
                "",
                "### Same-week eight-week differences on the common-ready sample",
                "",
                "| Segment | Target | Reference | Matched weeks | Difference | 95% interval | Excluding top five |",
                "| --- | --- | --- | ---: | ---: | --- | ---: |",
            ]
        )
        comparisons = result["comparisons"]
        rows = comparisons.loc[comparisons.horizon.eq(8) & comparisons["sample"].eq("common_ready")]
        for row in rows.itertuples():
            interval = f"{pct(row.ci_lower)} to {pct(row.ci_upper)}" if pd.notna(row.ci_lower) else "Sparse"
            lines.append(
                f"| {row.segment} | {row.target} | {row.reference} | {row.matched_weeks} | "
                f"{pct(row.matched_delta)} | {interval} | {pct(row.excluding_top5_delta)} |"
            )
        lines.extend(
            [
                "",
                f"[Full metrics]({result['label']}/summary.csv) · "
                f"[Matched comparisons]({result['label']}/comparisons.csv) · "
                f"[Signal ledger]({result['label']}/signal_ledger.csv) · "
                f"[Execution exclusions]({result['label']}/execution_coverage.csv) · "
                f"[Membership coverage]({result['label']}/membership_and_candle_coverage.csv)",
                "",
            ]
        )
    lines.extend(
        [
            "## Research decision",
            "",
        ]
    )
    bounded = results[0]["comparisons"]
    primary = bounded.loc[bounded.horizon.eq(8) & bounded["sample"].eq("common_ready")]
    hammer = primary.loc[primary.target.eq("A") & primary.reference.eq("green")].iloc[0]
    oversold = primary.loc[primary.target.eq("B") & primary.reference.eq("oversold_nonhammer")].iloc[0]
    if not (hammer.ci_lower > 0 or oversold.ci_lower > 0):
        lines.extend(
            [
                "**Monitor the oversold setup as a research hypothesis.** The bounded study provides "
                "insufficient evidence to advance the hammer family to portfolio simulation.",
                "",
            ]
        )
    for row, name in [
        (hammer, "Strict hammers versus green weeks"),
        (oversold, "Oversold hammers versus oversold non-hammers"),
    ]:
        if pd.notna(row.ci_lower):
            lines.append(
                f"- {name} had a mean difference of {row.matched_delta * 100:+.2f} percentage points, "
                f"with a 95% interval of {row.ci_lower * 100:+.2f} to {row.ci_upper * 100:+.2f} points "
                f"across {row.matched_weeks} matched weeks."
            )
    e_exclusions = primary.loc[primary.reference.eq("D_excluded_by_E"), "reference_fills"].sum()
    if e_exclusions == 0:
        lines.append("- The prior-decline filter removed zero filled D trades at eight weeks in the bounded study.")
    lines.extend(
        [
            "",
            "An evaluation with wider coverage requires effective Nifty 500 membership events, "
            "corporate-action-complete OHLCV and identity history, and a frozen holdout.",
            "",
            "[Run manifest](manifest.json)",
        ]
    )
    (destination / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_cost(value):
    try:
        fees, slippage = map(float, value.split(","))
        if 0 <= fees < 10000 and 0 <= slippage < 10000:
            return fees, slippage
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("Use fee_bps,slippage_bps with each number between 0 and 10000 (exclusive)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Unique directory name beneath outputs/weekly_hammer_reversal")
    parser.add_argument("--as-of", required=True, help="Last available completed NSE session date")
    parser.add_argument("--cost", type=parse_cost, action="append", help="Per-side fees,slippage in basis points")
    args = parser.parse_args()
    if not args.run_id.isascii() or not args.run_id.replace("-", "").replace("_", "").isalnum():
        parser.error("Use letters, digits, hyphens or underscores in --run-id")
    cutoff = pd.Timestamp(args.as_of).normalize()
    costs = tuple(dict.fromkeys(args.cost or COSTS))
    snapshot = load_snapshot(cutoff)
    source_files = (
        PRICE_PATH,
        MEMBERSHIP_PATH,
        ROOT / "data/nse_session_calendar.json",
        MEMBERSHIP,
        MEMBERSHIP_SCOPE,
        PRICE,
        ACTIONS,
        CALENDAR,
        EXCHANGE,
        EVENTS,
        VALIDATION,
        LOCAL_EVENTS,
        LOCAL_ACTIONS,
    )
    before = {path: file_token(path) for path in source_files}
    result_base = OUTPUT / args.run_id
    result_base.mkdir(parents=True, exist_ok=False)
    runner = Path(__file__).resolve()
    reconciliation = json.loads(CALENDAR.read_text(encoding="utf-8"))
    events = pd.read_csv(EVENTS)
    local_events = pd.read_csv(LOCAL_EVENTS)
    validation = pd.read_csv(VALIDATION)
    manifest = dict(
        strategy="weekly_hammer_reversal",
        version=1,
        live_enabled=False,
        git_head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        code_sha256={str(p.relative_to(ROOT)): sha256(p) for p in [runner, ROOT / "weekly_hammer_reversal.py"]},
        price_revision=snapshot.price_revision,
        membership_revision=snapshot.constituent_revision,
        shared_snapshot_cutoff=str(cutoff.date()),
        calendar=calendar_provenance(),
        bounded_calendar_reconciliation={
            key: reconciliation[key]
            for key in [
                "expected_sessions",
                "primary_observed_sessions",
                "missing_primary_sessions",
                "local_missing_sessions",
            ]
        },
        bounded_membership_event_rows=len(events),
        local_nifty500_event_rows=int(local_events.INDEX_NAME.eq("NIFTY 500").sum()),
        local_nifty500_snapshot_discrepancies=int(validation.INDEX_NAME.eq("NIFTY 500").sum()),
        input_sha256={str(path.relative_to(ROOT)): sha256(path) for path in source_files},
        start_target="2016-01-01",
        segments=SEGMENTS,
        primary_holding_weeks=8,
        horizons=HORIZONS,
        week_rule=WEEK,
        signal_geometry=asdict(HammerGeometry()),
        rsi="14, arithmetic Wilder seed; flat-window RSI 50; EMA span 14 seeded by first RSI, 14 RSI values required",
        thresholds=dict(B="RSI <= 35", C="RSI < EMA", D="RVOL20 >= 1.5", E="drawdown26 <= -0.15"),
        missing_policy="Reset price features at gaps or unresolved actions and volume at share-count events",
        fill_policy="First tradable open in designated week; no entry or exit roll, missing fills and censors logged",
        cost_bps_per_side=costs,
        inference={
            "resamples": RESAMPLES,
            "seed": 20261120,
            "block_weeks": "horizon+1",
            "minimum_paired_weeks": "max(12, 2*block_weeks)",
        },
        python=platform.python_version(),
        pandas=pd.__version__,
        numpy=np.__version__,
        platform=platform.platform(),
        rerun="python -m scripts.run_weekly_hammer_reversal " + shlex.join(sys.argv[1:]).replace(args.run_id, "NEW_ID"),
    )
    write_manifest_exclusive(result_base / "manifest.json", manifest)
    results = []
    for mode in ("bounded", "snapshots"):
        destination = result_base / mode
        destination.mkdir()
        print(f"Running {mode} study", flush=True)
        results.append(study(mode, snapshot, destination, costs))
    if any(file_token(path) != token for path, token in before.items()):
        raise RuntimeError("Input files changed during the study. Use a new run ID and retry.")
    write_manifest_exclusive(
        result_base / "run_status.json",
        {
            "status": "complete",
            "studies": [
                {k: v for k, v in r.items() if k not in {"summary", "coverage", "comparisons"}} for r in results
            ],
            "holdout_status": "No reliable effective membership or observed Nifty 500 snapshots from 2025 onward.",
        },
    )
    report(results, result_base, costs)
    print(result_base)


if __name__ == "__main__":
    main()
