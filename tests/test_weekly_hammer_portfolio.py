import pandas as pd

from quant_portfolio_engine import QuantPortfolioConfig, run_quant_portfolio


def test_hammer_fills_only_selected_members_and_exits_after_missing_week():
    dates = pd.bdate_range("2024-01-01", "2024-02-09")
    normal = pd.DataFrame({"Open": 100, "High": 101, "Low": 99, "Close": 100, "Volume": 500_000}, index=dates)
    hammer = normal.copy()
    hammer.loc["2024-01-01", "Low"] = 95
    hammer.loc["2024-01-05", ["Close", "High"]] = [101, 102]
    missing_exit = hammer.drop(pd.bdate_range("2024-01-15", "2024-01-19"))
    membership = pd.DataFrame(
        {
            "INDEX_NAME": ["NIFTY50", "NIFTY50", "NIFTY50", "NIFTY100"],
            "TIME_STAMP": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-08", "2024-01-01"]),
            "SYMBOL": ["AAA", "BBB", "CCC", "OUT"],
        }
    )
    config = QuantPortfolioConfig(
        strategy="weekly_hammer",
        strategy_settings={"variant": "A", "holding_weeks": 1},
        index_names=["Nifty 50"],
        compositions_df=membership,
        market_calendar=dates,
        max_holdings=1,
        min_history_days=1,
        minimum_median_volume=1,
        transaction_cost_pct=0,
    )
    result = run_quant_portfolio({"BBB": hammer, "AAA": missing_exit, "CCC": hammer, "OUT": hammer}, {}, config)
    decisions = result["decision_log"]
    assert decisions.loc[decisions.Event.eq("BUY")].set_index("Symbol").Outcome.to_dict() == {
        "AAA": "EXECUTED",
        "BBB": "SKIPPED_FULL",
        "CCC": "SKIPPED_UNIVERSE",
        "OUT": "SKIPPED_UNIVERSE",
    }
    exits = decisions.loc[decisions.Event.eq("EXIT")]
    assert exits.Outcome.tolist() == ["SKIPPED_NO_PRICE", "EXECUTED"]
    trade = result["trades"].iloc[0]
    assert trade.Entry_Date == pd.Timestamp("2024-01-08")
    assert trade.Exit_Date == pd.Timestamp("2024-01-22")
    assert result["daily"].Holdings.max() == 1
    assert result["nav"]["Quant Portfolio"].iloc[-1] == 100


def test_hammer_slots_are_sized_without_execution_day_close():
    dates = pd.bdate_range("2024-01-01", "2024-02-09")
    normal = pd.DataFrame({"Open": 100, "High": 102, "Low": 99, "Close": 100, "Volume": 500_000}, index=dates)
    first, second = normal.copy(), normal.copy()
    first.loc["2024-01-01", "Low"] = 95
    first.loc["2024-01-05", "Close"] = 101
    first.loc["2024-01-15", ["High", "Close"]] = [205, 200]
    second.loc["2024-01-08", "Low"] = 95
    second.loc["2024-01-12", "Close"] = 101
    membership = pd.DataFrame({"INDEX_NAME": "NIFTY50", "TIME_STAMP": dates[0], "SYMBOL": ["AAA", "BBB"]})
    result = run_quant_portfolio(
        {"AAA": first, "BBB": second},
        {},
        QuantPortfolioConfig(
            strategy="weekly_hammer",
            strategy_settings={"variant": "A"},
            index_names=["Nifty 50"],
            compositions_df=membership,
            market_calendar=dates,
            initial_capital=1_000,
            max_holdings=2,
            min_history_days=1,
            minimum_median_volume=1,
            transaction_cost_pct=0,
        ),
    )
    trades = result["trades"].set_index("Symbol")
    assert trades.at["BBB", "Entry_Date"] == pd.Timestamp("2024-01-15")
    assert trades.at["BBB", "Shares"] == 5
