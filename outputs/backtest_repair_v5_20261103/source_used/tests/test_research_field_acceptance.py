"""Field-scoped shared-source consumption, including actual portfolio execution."""

import copy
import dataclasses
import hashlib

import numpy as np
import pandas as pd
import pytest

import data_backtest
from backtest_engine import BacktestConfig, run_backtest
from backtest_provenance import frame_hash
from market_data import MarketSnapshot
from research_acceptance import consumed_fields, field_digest, profile_digest


def inputs():
    days = pd.bdate_range("2014-01-02", periods=360)
    rows = []
    for symbol, speed in [("A", 0.003), ("B", 0.002)]:
        close = 100 * np.cumprod(1 + speed + 0.001 * np.sin(np.arange(len(days)) / 5))
        rows.append(
            pd.DataFrame(
                dict(
                    symbol=symbol,
                    date=days,
                    Open=close,
                    High=close * 1.1,
                    Low=close * 0.9,
                    Close=close,
                    Volume=1000000.0,
                    series_type="equity",
                )
            )
        )
    prices = pd.concat(rows, ignore_index=True)
    membership = pd.DataFrame(dict(INDEX_NAME=["NIFTY500"] * 2, TIME_STAMP=[days[0]] * 2, SYMBOL=["A", "B"]))
    cfg = BacktestConfig(
        m=1,
        n=2,
        rebalance_freq="monthly",
        sort_method="Average of 3/6/9/12 months",
        start_date=str(days[270].date()),
        end_date=str(days[-1].date()),
        min_history_days=252,
        compositions_df=membership,
        index_names=["Nifty 500"],
        market_calendar=days,
        corporate_actions=[],
    )
    snapshot = MarketSnapshot(prices, membership, "prices", "members", days[-1])
    cert = dict(
        accepted=True,
        reviewer="synthetic fixture review",
        economics_validated=True,
        complete_membership=True,
        unresolved_consumed_events=[],
        unresolved_consumed_seams=[],
        source_revisions=["prices", "members"],
        start=cfg.start_date,
        end=cfg.end_date,
        history_start=str(days[0].date()),
        index_keys=["NIFTY500"],
        fields=["Close", "Volume"],
        profile_sha256=[profile_digest(cfg)],
        membership_sha256=frame_hash(membership),
        calendar_sha256=hashlib.sha256(days.asi8.tobytes()).hexdigest(),
        price_field_hashes={
            s: field_digest(f, ["Close", "Volume"], days[0], days[-1]) for s, f in snapshot.ohlcv().items()
        },
    )
    reviewed = dataclasses.replace(snapshot, price_metadata={"research_field_scopes": [cert]})
    return snapshot, reviewed, cfg, cert


def test_close_volume_profile_has_identical_fills_and_nav_with_uncertified_high_low_masked():
    source, reviewed, cfg, _ = inputs()
    unmasked = run_backtest(source.ohlcv(), {}, cfg)
    frames = data_backtest.load_ohlcv_for_backtest(snapshot=reviewed).symbol_data
    assert frames["A"][["Open", "High", "Low"]].isna().all().all()
    assert frames["A"].Close.notna().all()
    result = run_backtest(frames, {}, cfg)
    assert "error" not in result
    pd.testing.assert_frame_equal(result["nav"], unmasked["nav"])
    assert result["execution_ledgers"] == unmasked["execution_ledgers"]
    assert result["run_manifest"]["reviewed_field_scope_sha256"]
    assert source.prices.High.notna().all()  # Opposing raw observations stay retained.


@pytest.mark.parametrize("change", ["high_filter", "history750", "component", "stage2"])
def test_shared_certificate_rejects_unsupported_profiles_before_execution(change):
    _, reviewed, cfg, _ = inputs()
    modifications = {
        "high_filter": dict(pct_from_52w_high=25),
        "history750": dict(min_history_days=750),
        "component": dict(index_names=["Nifty 50"]),
        "stage2": dict(stage2_entry_filter=True),
    }
    with pytest.raises(ValueError, match="uncertified price fields|profile|component"):
        run_backtest(reviewed.ohlcv(), {}, dataclasses.replace(cfg, **modifications[change]))


@pytest.mark.parametrize(
    "change",
    ["revision", "price", "membership", "calendar", "member_missing", "scope_missing", "tradability", "unresolved"],
)
def test_scope_requires_exact_revision_prices_membership_calendar_and_closed_economics(change):
    _, reviewed, cfg, cert = inputs()
    frames = reviewed.ohlcv()
    if change == "revision":
        frames["A"].attrs["source_revisions"] = ("changed", "members")
    if change == "price":
        frames["A"].iloc[10, frames["A"].columns.get_loc("Close")] += 1
    if change == "membership":
        cfg = dataclasses.replace(cfg, compositions_df=cfg.compositions_df.iloc[:1])
    if change == "calendar":
        cfg = dataclasses.replace(cfg, market_calendar=cfg.market_calendar.delete(10))
    if change == "member_missing":
        frames.pop("B")
    if change == "scope_missing":
        frames["B"].attrs.pop("research_field_scopes")
    if change == "tradability":
        frames["B"]["is_tradable"] = False
    if change == "unresolved":
        cert = copy.deepcopy(cert)
        cert["unresolved_consumed_events"] = ["unvalued demerger"]
        frames["A"].attrs["research_field_scopes"] = [cert]
    with pytest.raises(ValueError):
        run_backtest(frames, {}, cfg)


def test_dependency_boundary_distinguishes_source_high_from_close_based_stage2_and_circuit_proxy():
    _, _, cfg, _ = inputs()
    assert consumed_fields(cfg) == {"Close", "Volume"}
    assert consumed_fields(dataclasses.replace(cfg, max_circuits=18, stage2_entry_filter=True)) == {"Close", "Volume"}
    assert consumed_fields(dataclasses.replace(cfg, pct_from_52w_high=25)) == {"Close", "Volume", "High"}
