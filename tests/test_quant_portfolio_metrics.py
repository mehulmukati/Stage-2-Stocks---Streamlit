import numpy as np
import pandas as pd

from quant_portfolio_metrics import slice_and_rebase_nav


def test_slice_and_rebase_nav_uses_selected_sessions_and_independent_bases():
    dates = pd.bdate_range("2025-01-01", periods=6)
    nav = pd.DataFrame(
        {
            "Quant Portfolio": [100.0, 102.0, 104.0, 106.0, 108.0, 110.0],
            "Late Benchmark": [np.nan, np.nan, 200.0, 210.0, 220.0, 230.0],
        },
        index=dates,
    )

    period, rebased = slice_and_rebase_nav(nav, "2025-01-02", "2025-01-07")

    assert list(period.index) == list(dates[1:5])
    assert rebased.loc[dates[1], "Quant Portfolio"] == 100.0
    assert np.isnan(rebased.loc[dates[1], "Late Benchmark"])
    assert rebased.loc[dates[2], "Late Benchmark"] == 100.0
    assert rebased.loc[dates[4], "Quant Portfolio"] == 108.0 / 102.0 * 100.0


def test_slice_and_rebase_nav_uses_next_observed_session_for_weekend_start():
    dates = pd.bdate_range("2025-01-03", periods=4)
    nav = pd.DataFrame({"Portfolio": [95.0, 100.0, 105.0, 110.0]}, index=dates)

    period, rebased = slice_and_rebase_nav(nav, "2025-01-04", "2025-01-08")

    assert period.index[0] == pd.Timestamp("2025-01-06")
    assert rebased.iloc[0, 0] == 100.0
