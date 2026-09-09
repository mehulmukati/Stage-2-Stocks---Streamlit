import pandas as pd

from strategy_replay import ExecutionPolicy, resolve_signal_executions


def test_daily_policy_uses_following_observation_date_and_open():
    dates = pd.to_datetime(["2025-01-03", "2025-01-06"])
    signals = pd.DataFrame(
        {"Open": [10.0, 11.0], "Close": [10.0, 11.0], "Signal": ["BUY", ""]},
        index=dates,
    )

    executions = resolve_signal_executions(signals, ExecutionPolicy(execution_date_column=None))

    assert executions.iloc[0]["Signal_Date"] == pd.Timestamp("2025-01-03")
    assert executions.iloc[0]["Execution_Date"] == pd.Timestamp("2025-01-06")
    assert executions.iloc[0]["Execution_Price"] == 11.0


def test_aggregated_policy_uses_following_bars_actual_open_date():
    week_ends = pd.to_datetime(["2025-01-03", "2025-01-10"])
    signals = pd.DataFrame(
        {
            "Open": [10.0, 11.0],
            "Close": [10.0, 12.0],
            "Execution_Date": pd.to_datetime(["2024-12-30", "2025-01-06"]),
            "Signal": ["BUY", ""],
        },
        index=week_ends,
    )

    executions = resolve_signal_executions(
        signals,
        ExecutionPolicy(execution_date_column="Execution_Date"),
    )

    assert executions.iloc[0]["Execution_Date"] == pd.Timestamp("2025-01-06")
    assert executions.iloc[0]["Execution_Price"] == 11.0
