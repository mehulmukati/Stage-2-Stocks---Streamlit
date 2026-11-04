"""Verify native MoMo's unresolved ownership boundary with synthetic input."""
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
NATIVE = ROOT / "MoMo/backtest/available_observations/run_backtest.py"
spec = importlib.util.spec_from_file_location("native_momo_round6", NATIVE)
momo = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = momo
spec.loader.exec_module(momo)


def probe(event):
    old = event["old"]
    suspension = pd.Timestamp(event["suspension"])
    last = pd.Timestamp(event["last_old_close"])
    listing = pd.Timestamp(event["first_newly_allotted_tradable"])
    days = pd.DatetimeIndex([last - pd.Timedelta(days=1), last, suspension, listing])
    close = pd.DataFrame({old: [100.0, 100.0, float("nan"), float("nan")]}, index=days)
    momo.CFG.update(
        start=str(days[0].date()), capital=10000.0, regime_filter=False, execution_mode="daily",
        execution_candle_labels=["15:30"], holdings=1, normal_weight=.5,
    )
    result = momo.run({
        "C": close,
        "sched": {last: last, suspension: suspension, listing: listing},
        "quotes": {last: {old: {"close": 100.0, "label": "15:30"}}},
        "last": close.notna().astype(int),
        "eligible": close.notna(),
        "score": close,
        "raw": close,
        "high": close,
        "corporate_actions": event["actions"],
    }, taxed=False)
    sell = result["trades"].loc[result["trades"].side.eq("SELL")].iloc[0]
    assert sell.symbol == old and sell.date == suspension and sell.assumed_last_price_exit
    assert sell.price_available_at == last + pd.Timedelta(hours=15, minutes=30)
    assert sell.price_age_days == (suspension - last).days
    return {
        "case": event["name"], "outcome": "native sale of obsolete identity on synthetic suspension",
        "symbol": sell.symbol, "sale_date": str(sell.date.date()),
        "price_available_at_IST": str(sell.price_available_at), "price_age_days": sell.price_age_days,
        "assumed": bool(sell.assumed_last_price_exit),
    }


if __name__ == "__main__":
    events = json.loads((ROOT / "outputs/backtest_repair_v5_20261103/ownership_replay.json").read_text())["events"]
    print(json.dumps({
        "fixture_only": True, "native_source_sha256": hashlib.sha256(NATIVE.read_bytes()).hexdigest(),
        "ownership": [probe(event) for event in events],
        "native_muhurat_daily_timestamp_IST": str(momo.candle_available_at("2019-10-27", "daily")),
    }, indent=2))
