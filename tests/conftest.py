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
