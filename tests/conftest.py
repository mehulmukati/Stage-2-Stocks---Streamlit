import pandas as pd


def make_ohlcv(
    n: int,
    close: list | None = None,
    volume: int = 1_000_000,
    start: str = "2020-01-01",
) -> pd.DataFrame:
    """Return a minimal OHLCV DataFrame with n business-day rows."""
    dates = pd.bdate_range(start, periods=n)
    c = pd.Series(close if close is not None else [100.0] * n, index=dates, dtype=float)
    return pd.DataFrame(
        {
            "Open": c * 0.99,
            "High": c * 1.01,
            "Low": c * 0.99,
            "Close": c,
            "Volume": float(volume),
        }
    )


from datetime import datetime, timezone

import numpy as np
import pytest

import data
import data_backtest as db
import market_data as md


@pytest.fixture
def market(tmp_path, monkeypatch):
    prices = tmp_path / "data" / "screener_ohlcv.parquet"
    membership = tmp_path / "data" / "constituents.parquet"
    monkeypatch.setattr(md, "PRICE_PATH", prices)
    monkeypatch.setattr(md, "MEMBERSHIP_PATH", membership)
    days = pd.bdate_range("2025-01-01", periods=350)
    rows = []
    for symbol, volume in [("A", 200_000), ("B", 200_000), ("LOWVOL", 70_000)]:
        close = 100 * np.exp(np.arange(len(days)) * 0.002 + np.sin(np.arange(len(days))) * 0.01)
        rows.append(
            pd.DataFrame(
                dict(
                    symbol=symbol,
                    date=days,
                    Open=close,
                    High=close * 1.01,
                    Low=close * 0.99,
                    Close=close,
                    Volume=volume,
                    series_type="equity",
                )
            )
        )
    frame = pd.concat(rows, ignore_index=True)
    history = pd.DataFrame(
        dict(
            INDEX_NAME=["Nifty 50", "Nifty 50", "Nifty 50"],
            TIME_STAMP=[days[0], days[0], days[-10]],
            SYMBOL=["A", "LOWVOL", "B"],
        )
    )
    # Complete new snapshot includes A/B/LOWVOL, not only newly added symbols.
    history = pd.concat(
        [
            history,
            pd.DataFrame(
                dict(
                    INDEX_NAME=["Nifty 50", "Nifty 50"],
                    TIME_STAMP=[days[-10], days[-10]],
                    SYMBOL=["A", "LOWVOL"],
                )
            ),
        ],
        ignore_index=True,
    )
    md.write_source(frame, prices)
    md.write_source(history, membership, {"verified_at": datetime.now(timezone.utc).isoformat()})
    monkeypatch.setattr(data, "_get_target_key", lambda: str(days[-1].date()))
    monkeypatch.setattr(db, "_get_target_key", lambda: str(days[-1].date()))
    monkeypatch.setattr(data, "load_nse_holidays", lambda: frozenset())
    monkeypatch.setattr(db, "load_nse_holidays", lambda: frozenset())
    data._score_cache.update(stage2={"date": None, "data": None}, momentum={"date": None, "data": None})
    return frame, history, days
