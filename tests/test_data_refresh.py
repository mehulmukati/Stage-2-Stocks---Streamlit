import pandas as pd
import pytest

from scripts.refresh_backtest_parquet import _validate_current_tradability as validate_backtest_tradability
from scripts.refresh_screener_parquet import _validate_current_tradability as validate_screener_tradability


def _refresh_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2026-07-13", periods=6)
    rows = []
    for symbol in ["CURRENT", "STALE"]:
        for position, day in enumerate(dates):
            rows.append(
                {
                    "symbol": symbol,
                    "date": day,
                    "Close": 100.0,
                    "High": 101.0,
                    "Volume": 1_000_000 if symbol == "CURRENT" or position < 2 else 0,
                }
            )
    return pd.DataFrame(rows)


def test_refresh_validators_reject_stale_current_constituent():
    frame = _refresh_frame()

    with pytest.raises(RuntimeError, match="STALE"):
        validate_backtest_tradability(frame, current_symbols={"CURRENT", "STALE"})
    with pytest.raises(RuntimeError, match="STALE"):
        validate_screener_tradability(frame, ["CURRENT", "STALE"])


def test_refresh_validators_accept_recent_positive_volume_bars():
    frame = _refresh_frame()

    validate_backtest_tradability(frame, current_symbols={"CURRENT"})
    validate_screener_tradability(frame, ["CURRENT"])
