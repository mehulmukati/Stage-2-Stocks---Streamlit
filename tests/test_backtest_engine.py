import math

import pandas as pd

from backtest_engine import (
    _compute_fy_tax,
    _compute_summary_stats,
    _trading_days,
    _valid_symbols_at_date,
    get_rebalance_dates,
)

from .conftest import make_ohlcv

# ──────────────────────────────────────────────
# _compute_fy_tax
# ──────────────────────────────────────────────


def test_gains_only_stcg():
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 1000, 0, 0, 0, [], [], 0.20, 0.125)
    assert math.isclose(tax, 200.0)
    assert cf_st == []
    assert cf_lt == []


def test_gains_only_ltcg():
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 0, 0, 1000, 0, [], [], 0.20, 0.125)
    assert math.isclose(tax, 125.0)
    assert cf_lt == []


def test_st_loss_offsets_st_gain():
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 1000, 600, 0, 0, [], [], 0.20, 0.125)
    assert math.isclose(tax, 80.0)  # net 400 * 0.20


def test_st_loss_offsets_lt_gain():
    # ST loss 300 exceeds ST gains 100 by 200; overflow offsets LT gain of 500
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 100, 300, 500, 0, [], [], 0.20, 0.125)
    # net_lt after offset = 500 - 200 = 300; tax = 300 * 0.125
    assert math.isclose(tax, 37.5)


def test_lt_loss_offsets_lt_gain_only():
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 0, 0, 500, 200, [], [], 0.20, 0.125)
    assert math.isclose(tax, 37.5)  # net 300 * 0.125


def test_cf_entry_created_on_net_loss():
    # ST loss 1000, ST gain 0, no LT → unabsorbed 1000 goes to CF
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 0, 1000, 0, 0, [], [], 0.20, 0.125)
    assert tax == 0.0
    assert len(cf_st) == 1
    assert cf_st[0] == (2030, 1000.0)  # expiry = 2022 + 8


def test_cf_entry_applied_in_future_fy():
    # CF from previous year offsets current gains
    cf_in = [(2029, 500.0)]  # expiry 2029, amount 500
    tax, cf_st_out, _ = _compute_fy_tax(2023, 600, 0, 0, 0, cf_in, [], 0.20, 0.125)
    # net_st = 600; CF of 500 reduces it to 100; tax = 100 * 0.20
    assert math.isclose(tax, 20.0)
    assert cf_st_out == []  # fully consumed


def test_cf_expiry_respected():
    # CF entry expired before current FY is ignored
    cf_in = [(2021, 1000.0)]  # expiry 2021 < current FY 2022
    tax, cf_st_out, _ = _compute_fy_tax(2022, 500, 0, 0, 0, cf_in, [], 0.20, 0.125)
    assert math.isclose(tax, 100.0)  # no offset; 500 * 0.20
    assert cf_st_out == []  # expired entry discarded


def test_zero_rates():
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 1000, 0, 2000, 0, [], [], 0.0, 0.0)
    assert tax == 0.0


def test_zero_gains_and_losses():
    tax, cf_st, cf_lt = _compute_fy_tax(2022, 0, 0, 0, 0, [], [], 0.20, 0.125)
    assert tax == 0.0
    assert cf_st == []
    assert cf_lt == []


def test_multi_cf_oldest_consumed_first():
    # Two CF buckets; oldest expiry consumed first
    cf_in = [(2025, 300.0), (2024, 200.0)]  # 2024 is older
    tax, cf_st_out, _ = _compute_fy_tax(2022, 400, 0, 0, 0, cf_in, [], 0.20, 0.125)
    # Sorted oldest first: consume 200 (2024) then 200 from 300 (2025)
    # net_st = 400; after 200 consumed → 200; after next 200 consumed → 0
    assert math.isclose(tax, 0.0)
    assert len(cf_st_out) == 1
    assert cf_st_out[0] == (2025, 100.0)  # 100 left from 300 bucket


def test_lt_cf_cannot_offset_st_gains():
    # LT carry-forward should only reduce LT taxable amount, not ST
    cf_lt_in = [(2030, 500.0)]
    tax, _, cf_lt_out = _compute_fy_tax(2022, 1000, 0, 0, 0, [], cf_lt_in, 0.20, 0.125)
    # ST gains = 1000, LT CF cannot touch it; tax = 1000 * 0.20
    assert math.isclose(tax, 200.0)
    assert cf_lt_out == [(2030, 500.0)]  # unchanged


# ──────────────────────────────────────────────
# _valid_symbols_at_date
# ──────────────────────────────────────────────


def _make_comp_df(rows):
    """Helper: rows = list of (index_name, symbol, timestamp_str)."""
    return pd.DataFrame([{"INDEX_NAME": i, "SYMBOL": s, "TIME_STAMP": pd.Timestamp(t)} for i, s, t in rows])


def test_valid_empty_comp_df():
    result = _valid_symbols_at_date(pd.DataFrame(), ["NIFTY50"], pd.Timestamp("2023-01-01"))
    assert result is None


def test_valid_single_index_single_snapshot():
    df = _make_comp_df([("NIFTY50", "A", "2023-01-01"), ("NIFTY50", "B", "2023-01-01"), ("NIFTY50", "C", "2023-01-01")])
    result = _valid_symbols_at_date(df, ["NIFTY50"], pd.Timestamp("2023-06-01"))
    assert result == {"A", "B", "C"}


def test_valid_uses_latest_snapshot():
    df = _make_comp_df(
        [
            ("NIFTY50", "OLD1", "2022-01-01"),
            ("NIFTY50", "OLD2", "2022-01-01"),
            ("NIFTY50", "NEW1", "2023-01-01"),
            ("NIFTY50", "NEW2", "2023-01-01"),
        ]
    )
    result = _valid_symbols_at_date(df, ["NIFTY50"], pd.Timestamp("2023-06-01"))
    assert result == {"NEW1", "NEW2"}


def test_valid_respects_as_of_date():
    df = _make_comp_df([("NIFTY50", "FUTURE", "2024-01-01")])
    result = _valid_symbols_at_date(df, ["NIFTY50"], pd.Timestamp("2023-06-01"))
    assert result is None


def test_valid_multi_index_union():
    df = _make_comp_df(
        [
            ("NIFTY50", "A", "2023-01-01"),
            ("NIFTY50", "B", "2023-01-01"),
            ("NIFTY500", "C", "2023-01-01"),
            ("NIFTY500", "D", "2023-01-01"),
        ]
    )
    result = _valid_symbols_at_date(df, ["NIFTY50", "NIFTY500"], pd.Timestamp("2023-06-01"))
    assert result == {"A", "B", "C", "D"}


def test_valid_missing_index_returns_rest(caplog):
    df = _make_comp_df([("NIFTY50", "A", "2023-01-01")])
    result = _valid_symbols_at_date(df, ["NIFTY50", "NIFTY_MISSING"], pd.Timestamp("2023-06-01"))
    assert result == {"A"}
    assert "NIFTY_MISSING" in caplog.text


def test_valid_empty_index_names():
    df = _make_comp_df([("NIFTY50", "A", "2023-01-01")])
    result = _valid_symbols_at_date(df, [], pd.Timestamp("2023-01-01"))
    assert result is None


# ──────────────────────────────────────────────
# _trading_days
# ──────────────────────────────────────────────


def test_trading_days_empty_dict():
    result = _trading_days({}, pd.Timestamp("2020-01-01"), pd.Timestamp("2020-12-31"))
    assert isinstance(result, pd.DatetimeIndex)
    assert len(result) == 0


def test_trading_days_single_symbol():
    df = make_ohlcv(10, start="2020-01-02")
    start = df.index[0]
    end = df.index[-1]
    result = _trading_days({"X": df}, start, end)
    assert len(result) == 10
    assert result[0] == start
    assert result[-1] == end


def test_trading_days_date_filtering():
    df = make_ohlcv(20, start="2020-01-02")
    # Only the middle 5 days
    start = df.index[7]
    end = df.index[11]
    result = _trading_days({"X": df}, start, end)
    assert result[0] >= start
    assert result[-1] <= end
    assert len(result) == 5


def test_trading_days_multi_symbol_union():
    df1 = make_ohlcv(5, start="2020-01-02")
    df2 = make_ohlcv(5, start="2020-02-03")
    start = df1.index[0]
    end = df2.index[-1]
    result = _trading_days({"A": df1, "B": df2}, start, end)
    assert len(result) == 10


# ──────────────────────────────────────────────
# get_rebalance_dates
# ──────────────────────────────────────────────

_TWO_YEARS = pd.DatetimeIndex(pd.bdate_range("2022-01-01", "2023-12-31"))


def test_rebalance_monthly():
    dates = get_rebalance_dates(_TWO_YEARS, "monthly")
    assert len(dates) == 24
    # Each date is the last trading day of its month
    months = [(d.year, d.month) for d in dates]
    assert len(set(months)) == 24


def test_rebalance_weekly():
    dates = get_rebalance_dates(_TWO_YEARS, "weekly")
    assert len(dates) >= 100  # 2 years ≈ 104 weeks
    # All dates come from the source
    for d in dates:
        assert d in _TWO_YEARS


def test_rebalance_biweekly():
    weekly = get_rebalance_dates(_TWO_YEARS, "weekly")
    biweekly = get_rebalance_dates(_TWO_YEARS, "biweekly")
    assert len(biweekly) == math.ceil(len(weekly) / 2)


def test_rebalance_quarterly():
    dates = get_rebalance_dates(_TWO_YEARS, "quarterly")
    assert len(dates) == 8  # 4 quarters × 2 years
    quarters = [(d.year, d.quarter) for d in dates]
    assert len(set(quarters)) == 8


def test_rebalance_half_yearly():
    dates = get_rebalance_dates(_TWO_YEARS, "half-yearly")
    assert len(dates) == 4  # 2 halves × 2 years


def test_rebalance_empty_trading_days():
    result = get_rebalance_dates(pd.DatetimeIndex([]), "monthly")
    assert result == []


# ──────────────────────────────────────────────
# _compute_summary_stats
# ──────────────────────────────────────────────


def test_summary_flat_nav():
    nav = pd.DataFrame({"Port": [100.0] * 252}, index=pd.bdate_range("2022-01-01", periods=252))
    stats = _compute_summary_stats(nav)
    assert "Port" in stats.index
    row = stats.loc["Port"]
    assert math.isclose(row["CAGR (%)"], 0.0, abs_tol=0.01)
    assert math.isclose(row["Max Drawdown (%)"], 0.0, abs_tol=0.01)
    assert row["Final NAV"] == 100.0


def test_summary_monotonic_rise():
    closes = [100.0 * (1.001**i) for i in range(252)]
    nav = pd.DataFrame({"Port": closes}, index=pd.bdate_range("2022-01-01", periods=252))
    stats = _compute_summary_stats(nav)
    row = stats.loc["Port"]
    assert row["CAGR (%)"] > 0
    assert math.isclose(row["Max Drawdown (%)"], 0.0, abs_tol=0.01)
    assert math.isnan(row["Calmar"])  # DD=0 → Calmar=NaN


def test_summary_single_drawdown():
    # 100 days up, 1 day down 20%, then 151 days recovering
    base = [100.0 + i * 0.1 for i in range(100)]
    low = [base[-1] * 0.80]
    recovery = [low[0] * (1.002**i) for i in range(1, 152)]
    nav = pd.DataFrame({"Port": base + low + recovery}, index=pd.bdate_range("2022-01-01", periods=252))
    stats = _compute_summary_stats(nav)
    row = stats.loc["Port"]
    assert row["Max Drawdown (%)"] <= -15.0  # at least 15% drawdown recorded


def test_summary_short_series_skipped():
    nav = pd.DataFrame({"Port": [100.0]}, index=pd.bdate_range("2022-01-01", periods=1))
    stats = _compute_summary_stats(nav)
    assert "Port" not in stats.index


def test_summary_multi_column():
    idx = pd.bdate_range("2022-01-01", periods=252)
    nav = pd.DataFrame({"A": [100.0] * 252, "B": [100.0] * 252, "C": [100.0] * 252}, index=idx)
    stats = _compute_summary_stats(nav)
    assert set(stats.index) == {"A", "B", "C"}
