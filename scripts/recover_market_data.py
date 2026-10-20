"""One bounded recovery attempt; independent scheduled slots provide delayed retries."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import IST  # noqa: E402
from scripts.refresh_market_data import (  # noqa: E402
    check_publication,
    completed_session,
    price_coverage,
    refresh_prices,
)

SCHEDULES = {"15 14 * * *", "15 15 * * *", "15 17 * * *", "15 3 * * *"}
FINAL_SCHEDULE = "15 3 * * *"


def recovery_plan(schedule="", now=None, target_date=None):
    """Pin delayed/queued runs to their scheduled slot, including across midnight."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("Recovery time must include a timezone")
    nominal = now.astimezone(timezone.utc)
    if schedule:
        if schedule not in SCHEDULES:
            raise ValueError(f"Unknown recovery schedule: {schedule}")
        minute, hour, *_ = schedule.split()
        slot = nominal.replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)
        nominal = slot if slot <= nominal else slot - timedelta(days=1)
    target = target_date or completed_session(nominal.astimezone(IST))
    # Also validates explicit workflow inputs before they enter logs or outputs.
    target = datetime.strptime(target, "%Y-%m-%d").strftime("%Y-%m-%d")
    deadline = (datetime.strptime(target, "%Y-%m-%d") + timedelta(days=1)).replace(hour=8, minute=45, tzinfo=IST)
    return dict(
        target_date=target,
        final_attempt=schedule == FINAL_SCHEDULE,
        scheduled_at=nominal.isoformat(),
        attempt_allowed=not schedule or nominal <= deadline,
    )


def run_attempt(plan, force=False):
    """Use accepted coverage as the success marker; page reads never fetch upstream."""
    import market_data as md
    from scripts.refresh_constituents import refresh as refresh_constituents

    target = plan["target_date"]
    report = dict(plan)
    try:
        if not force:
            try:
                report.update(check_publication(target))
            except RuntimeError:
                pass
            else:
                return report | {"status": "already_current"}
        if not force and not plan.get("attempt_allowed", True):
            raise RuntimeError("Scheduled recovery window exhausted; no further provider requests for this session")
        refresh_constituents()
        report.update(refresh_prices(target_date=target, require_coverage=True))
        report.update(check_publication(target))
        return report | {"status": "recovered"}
    except Exception as exc:
        report["error"] = str(exc)
        try:
            snapshot = md.load_snapshot()
            report.update(price_coverage(snapshot.prices, snapshot.symbols(as_of=target), target))
        except Exception as source_error:
            report["coverage_error"] = str(source_error)
        return report | {"status": "failed"}


def describe_report(report):
    target = report["target_date"]
    missing = report.get("missing_target_symbols", [])
    return (
        f"## Market data recovery: {target}\n\n"
        f"Status: **{report['status']}**\n\n"
        f"Latest accepted price date: {report.get('max_price_date') or 'unavailable'}\n\n"
        f"Target coverage: {report.get('target_session_symbol_count', 0)}/"
        f"{report.get('current_symbol_count', 0)} current stocks\n\n"
        f"Missing benchmarks: {', '.join(report.get('missing_target_benchmarks', [])) or 'none'}\n\n"
        f"Missing stocks ({len(missing)}): {', '.join(missing[:40]) or 'none'}"
        f"{' (full list in recovery artifact)' if len(missing) > 40 else ''}\n\n"
        f"Failure reason: {report.get('error', 'none')}\n"
    )


def main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()) if os.getenv("GITHUB_EVENT_PATH") else {}
    created = os.getenv("RECOVERY_RUN_CREATED_AT")
    now = datetime.fromisoformat(created.replace("Z", "+00:00")) if created else None
    inputs = event.get("inputs") or {}
    plan = recovery_plan(event.get("schedule", ""), now=now, target_date=inputs.get("target_date") or None)
    plan["final_attempt"] = plan["final_attempt"] or str(inputs.get("final_attempt", "false")).lower() == "true"
    if os.getenv("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
            handle.write(f"target_date={plan['target_date']}\nfinal_attempt={str(plan['final_attempt']).lower()}\n")
    report = run_attempt(plan, force=str(inputs.get("force", "false")).lower() == "true")
    path = Path("outputs/market_refresh_status.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    summary = describe_report(report)
    print(summary)
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
            handle.write(summary)
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
