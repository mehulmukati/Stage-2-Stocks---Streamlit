from datetime import datetime

import pandas as pd
import pytest

import market_data as md
from scripts import recover_market_data as recovery
from scripts import refresh_constituents
from scripts.refresh_market_data import merge_observations, refresh_prices


@pytest.mark.parametrize(
    "schedule,now,target,final",
    [
        ("15 14 * * *", "2026-10-19T14:20:00+00:00", "2026-10-19", False),
        ("15 15 * * *", "2026-10-19T15:20:00+00:00", "2026-10-19", False),
        ("15 17 * * *", "2026-10-20T00:30:00+00:00", "2026-10-19", False),
        ("15 3 * * *", "2026-10-20T03:20:00+00:00", "2026-10-19", False),
        ("15 3 * * *", "2026-10-21T03:20:00+00:00", "2026-10-19", False),
        ("15 9 * * *", "2026-10-20T09:20:00+00:00", "2026-10-19", True),
        ("15 9 * * *", "2026-10-21T12:00:00+00:00", "2026-10-19", True),
        ("15 9 * * *", "2026-10-20T20:00:00+00:00", "2026-10-19", True),
        ("15 14 * * *", "2026-10-17T14:20:00+00:00", "2026-10-16", False),
        ("15 17 * * *", "2026-10-22T02:30:00+00:00", "2026-10-21", False),
    ],
)
def test_recovery_slots_pin_same_session_across_delay_holiday_and_weekend(schedule, now, target, final):
    plan = recovery.recovery_plan(schedule, now=datetime.fromisoformat(now))
    assert plan["target_date"] == target
    assert plan["final_attempt"] is final


def test_recovery_rejects_unrecognized_schedule_and_invalid_explicit_date():
    with pytest.raises(ValueError, match="Unknown"):
        recovery.recovery_plan("0 0 * * *")
    with pytest.raises(ValueError):
        recovery.recovery_plan(target_date="2026-10-19\nforged=value")


def test_automatic_attempts_stop_after_next_afternoon_even_across_weekend():
    final = recovery.recovery_plan("15 9 * * *", now=datetime.fromisoformat("2026-10-17T09:20:00+00:00"))
    assert final["target_date"] == "2026-10-16"
    assert final["attempt_allowed"] is True
    later = recovery.recovery_plan("15 14 * * *", now=datetime.fromisoformat("2026-10-17T14:20:00+00:00"))
    assert later["target_date"] == "2026-10-16"
    assert later["attempt_allowed"] is False
    manual = recovery.recovery_plan(target_date="2026-10-16")
    assert manual["attempt_allowed"] is True


def test_outage_then_recovery_then_skip_without_repeated_source_requests(market, monkeypatch):
    """Exercise real validation and writes while only replacing the provider boundary."""
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    frame, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())
    plan = dict(target_date=target, final_attempt=False)
    baseline = md.PRICE_PATH.read_bytes()
    calls = []
    membership_calls = []
    monkeypatch.setattr(refresh_constituents, "refresh", lambda: membership_calls.append(True))
    upstream_available = False
    quotes = []
    for symbol in ["A", "B", "LOWVOL", "Nifty 50", "Nifty 100", "Nifty 500"]:
        row = frame.iloc[[-1]].copy().assign(symbol=symbol, date=pd.Timestamp(target))
        row.series_type = "equity" if symbol in {"A", "B", "LOWVOL"} else "index_price"
        quotes.append(row)
    healthy = merge_observations(frame, pd.concat(quotes, ignore_index=True))

    def fetch(requests, start, end):
        calls.append((tuple(requests), end))
        identities = set(requests.values())
        response = healthy if upstream_available else frame
        selected = response.apply(lambda row: (row.symbol, row.series_type) in identities, axis=1)
        return response[selected].copy(), None

    monkeypatch.setattr(recovery, "refresh_prices", lambda **kwargs: refresh_prices(fetch=fetch, **kwargs))
    failed = recovery.run_attempt(plan)
    assert failed["status"] == "failed"
    assert failed["target_session_symbol_count"] == 0
    assert "incomplete" in failed["error"]
    assert md.PRICE_PATH.read_bytes() == baseline

    source_calls = len(calls)
    exhausted = recovery.run_attempt(plan | {"attempt_allowed": False, "final_attempt": True})
    assert exhausted["status"] == "failed"
    assert "window exhausted" in exhausted["error"]
    assert len(calls) == source_calls
    assert len(membership_calls) == 1

    upstream_available = True
    accepted = recovery.run_attempt(plan)
    assert accepted["status"] == "recovered"
    assert accepted["target_session_symbol_count"] == 3
    revision = md.source_revisions()
    source_calls = len(calls)
    skipped = recovery.run_attempt(plan | {"final_attempt": True})
    assert skipped["status"] == "already_current"
    assert len(calls) == source_calls
    assert len(membership_calls) == 2
    assert md.source_revisions() == revision


def test_rejected_candidate_keeps_partial_download_evidence_and_accepted_prices(market, monkeypatch):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    monkeypatch.setattr(refresh_constituents, "refresh", lambda: None)
    frame, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())
    baseline = md.PRICE_PATH.read_bytes()
    # One stock is fresh, while another has no provider response at all.
    fresh = frame[frame.symbol.eq("A")].tail(1).assign(date=pd.Timestamp(target))
    response = pd.concat([frame[~frame.symbol.eq("B")], fresh], ignore_index=True)

    def fetch(requests, start, end):
        identities = set(requests.values())
        selected = response.apply(lambda row: (row.symbol, row.series_type) in identities, axis=1)
        return response[selected].copy(), "target session unavailable"

    monkeypatch.setattr(recovery, "refresh_prices", lambda **kwargs: refresh_prices(fetch=fetch, **kwargs))
    report = recovery.run_attempt(dict(target_date=target, final_attempt=True))
    assert report["status"] == "failed"
    assert report["target_session_symbol_count"] == 0
    assert report["candidate_coverage"]["target_session_symbol_count"] == 1
    assert "B.NS" in report["provider_errors"]
    assert "latest returned" in report["provider_errors"]["LOWVOL.NS"]
    assert md.PRICE_PATH.read_bytes() == baseline
    summary = recovery.describe_report(report)
    assert "Accepted target coverage: 0/3" in summary
    assert "Rejected candidate coverage: 1/3" in summary
    assert "B.NS" in summary


def test_final_report_keeps_reason_dates_and_missing_coverage(market, monkeypatch):
    _, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())
    monkeypatch.setattr(refresh_constituents, "refresh", lambda: None)

    def offline(**kwargs):
        raise ConnectionError("provider unavailable")

    monkeypatch.setattr(recovery, "refresh_prices", offline)
    report = recovery.run_attempt(dict(target_date=target, final_attempt=True))
    assert report["status"] == "failed"
    assert report["final_attempt"] is True
    assert report["error"] == "provider unavailable"
    assert report["max_price_date"] == str(days[-1].date())
    assert report["target_session_symbol_count"] == 0
    assert "provider unavailable" in recovery.describe_report(report)


def test_empty_provider_response_keeps_symbol_errors_without_writing(market, monkeypatch):
    from apps.dual_momentum import config

    monkeypatch.setattr(config, "ETF_UNIVERSE", {})
    monkeypatch.setattr(refresh_constituents, "refresh", lambda: None)
    _, _, days = market
    target = str((days[-1] + pd.offsets.BDay()).date())
    baseline = md.PRICE_PATH.read_bytes()
    monkeypatch.setattr(
        recovery,
        "refresh_prices",
        lambda **kwargs: refresh_prices(
            fetch=lambda *args: (pd.DataFrame(columns=md.PRICE_COLUMNS), "provider timed out"), **kwargs
        ),
    )
    report = recovery.run_attempt(dict(target_date=target, final_attempt=False))
    assert report["status"] == "failed"
    assert report["candidate_coverage"]["target_session_symbol_count"] == 0
    assert report["provider_errors"]["A.NS"] == "provider timed out"
    assert md.PRICE_PATH.read_bytes() == baseline
    assert "later scheduled recovery" in recovery.describe_report(report)
