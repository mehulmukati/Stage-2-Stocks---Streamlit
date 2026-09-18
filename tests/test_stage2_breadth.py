import pandas as pd

from stage2_breadth import aggregate_breadth, build_breadth_score_history


def test_aggregate_breadth_counts_each_phase_and_percentages():
    scores = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-02"] * 4),
            "symbol": ["A", "B", "C", "D"],
            "Score": [0, 2, 4, 6],
            "Phase": ["Not Stage 2", "Early/Weak Stage 2", "Likely Stage 2", "Strong Stage 2"],
        }
    )
    daily = aggregate_breadth(scores)
    row = daily.iloc[0]
    assert (row["Eligible"], row["Not Stage 2"], row["Early"], row["Likely"], row["Strong"]) == (4, 1, 1, 1, 1)
    assert row["Stage 2"] == 3
    assert row["Stage 2 %"] == 75.0


def test_aggregate_breadth_uses_latest_composition_per_index():
    scores = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-02-03"] * 3),
            "symbol": ["A", "B", "C"],
            "Score": [2, 4, 6],
            "Phase": ["Early/Weak Stage 2", "Likely Stage 2", "Strong Stage 2"],
        }
    )
    compositions = pd.DataFrame(
        {
            "INDEX_NAME": ["NIFTY 50", "NIFTY 50", "NIFTY 50"],
            "TIME_STAMP": pd.to_datetime(["2025-01-01", "2025-02-01", "2025-02-01"]),
            "SYMBOL": ["A", "B", "C"],
        }
    )
    daily = aggregate_breadth(scores, ["Nifty 50"], compositions)
    assert daily.iloc[0]["Eligible"] == 2
    assert daily.iloc[0]["Strong"] == 1


def test_build_breadth_score_history_requires_mature_history():
    dates = pd.bdate_range("2024-01-01", periods=260)
    ohlcv = pd.DataFrame(
        {"symbol": "A", "date": dates, "Close": [100 + i * 0.1 for i in range(260)], "Volume": 1_000_000}
    )
    result = build_breadth_score_history(ohlcv)
    assert not result.empty
    assert result["date"].min() == dates[250]
    assert set(result["Phase"]).issubset({"Not Stage 2", "Early/Weak Stage 2", "Likely Stage 2", "Strong Stage 2"})
