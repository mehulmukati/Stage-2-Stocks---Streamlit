import math

import pandas as pd

from backtest_engine import (
    BacktestConfig,
    _apply_weight_cap,
    _compute_fy_tax,
    _compute_summary_stats,
    _compute_weight_variants,
    _drift_weights,
    _trading_days,
    _valid_symbols_at_date,
    get_rebalance_dates,
    rank_universe_at_date,
    run_backtest,
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


def test_valid_matches_display_index_names_to_canonical_names():
    df = _make_comp_df([("NIFTY 50", "A", "2023-01-01"), ("NIFTY 50", "B", "2023-01-01")])
    result = _valid_symbols_at_date(df, ["Nifty 50"], pd.Timestamp("2023-06-01"))
    assert result == {"A", "B"}


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
    assert result == set()


def test_valid_unknown_requested_index_returns_empty_set(caplog):
    df = _make_comp_df([("NIFTY50", "A", "2023-01-01")])
    result = _valid_symbols_at_date(df, ["NIFTY_MISSING"], pd.Timestamp("2023-06-01"))
    assert result == set()
    assert "NIFTY_MISSING" in caplog.text


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


# ──────────────────────────────────────────────
# _drift_weights
# ──────────────────────────────────────────────


def test_drift_weights_empty():
    assert _drift_weights({}, {}, pd.Timestamp("2022-01-03"), pd.Timestamp("2022-01-04")) == {}


def test_drift_weights_equal_prices_unchanged():
    df = make_ohlcv(5, close=[100.0] * 5)
    dates = df.index
    result = _drift_weights({"A": 0.5, "B": 0.5}, {"A": df, "B": df}, dates[0], dates[2])
    assert math.isclose(result["A"], 0.5, rel_tol=1e-6)
    assert math.isclose(result["B"], 0.5, rel_tol=1e-6)


def test_drift_weights_price_rise_shifts_weight():
    df_a = make_ohlcv(5, close=[100.0, 110.0, 120.0, 130.0, 140.0])
    df_b = make_ohlcv(5, close=[100.0] * 5)
    dates = df_a.index
    result = _drift_weights({"A": 0.5, "B": 0.5}, {"A": df_a, "B": df_b}, dates[0], dates[1])
    # A rose from 100→110, B flat → A weight > 0.5
    assert result["A"] > 0.5
    assert result["B"] < 0.5
    assert math.isclose(result["A"] + result["B"], 1.0, rel_tol=1e-6)


def test_drift_weights_missing_date_uses_original():
    df = make_ohlcv(3, close=[100.0, 110.0, 120.0])
    # Use a date not in the index — should fall back to original weight
    missing = pd.Timestamp("2099-01-01")
    result = _drift_weights({"A": 0.6, "B": 0.4}, {"A": df, "B": df}, df.index[0], missing)
    assert math.isclose(result["A"], 0.6, rel_tol=1e-6)
    assert math.isclose(result["B"], 0.4, rel_tol=1e-6)


def test_drift_weights_normalises_to_one():
    df_a = make_ohlcv(5, close=[100.0, 200.0, 200.0, 200.0, 200.0])
    df_b = make_ohlcv(5, close=[100.0, 50.0, 50.0, 50.0, 50.0])
    dates = df_a.index
    result = _drift_weights({"A": 0.5, "B": 0.5}, {"A": df_a, "B": df_b}, dates[0], dates[1])
    assert math.isclose(sum(result.values()), 1.0, rel_tol=1e-6)


# ──────────────────────────────────────────────
# _apply_weight_cap
# ──────────────────────────────────────────────


def test_apply_weight_cap_empty():
    assert _apply_weight_cap({}, 0.3) == {}


def test_apply_weight_cap_gte_one_unchanged():
    w = {"A": 0.4, "B": 0.6}
    assert _apply_weight_cap(w, 1.0) == w


def test_apply_weight_cap_lte_zero_unchanged():
    w = {"A": 0.4, "B": 0.6}
    assert _apply_weight_cap(w, 0.0) == w


def test_apply_weight_cap_no_violation():
    # Uniform weights, cap = 0.5, no symbol exceeds cap
    w = {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}
    result = _apply_weight_cap(w, 0.5)
    for v in result.values():
        assert v <= 0.5 + 1e-9
    assert math.isclose(sum(result.values()), 1.0, rel_tol=1e-6)


def test_apply_weight_cap_trims_one_overweight():
    # A is 80%, others share 20% — cap at 40%
    w = {"A": 0.8, "B": 0.1, "C": 0.1}
    result = _apply_weight_cap(w, 0.4)
    assert result["A"] <= 0.4 + 1e-9
    assert math.isclose(sum(result.values()), 1.0, rel_tol=1e-6)


def test_apply_weight_cap_infeasible_returns_equal():
    # cap = 0.1 with 5 symbols — exactly feasible (0.1 * 5 = 1.0), but with unequal weights
    # cap = 0.05 with 5 symbols — infeasible (0.05 * 5 = 0.25 < 1.0)
    w = {"A": 0.5, "B": 0.2, "C": 0.1, "D": 0.1, "E": 0.1}
    result = _apply_weight_cap(w, 0.05)
    for v in result.values():
        assert math.isclose(v, 0.2, rel_tol=1e-6)


def test_apply_weight_cap_result_sums_to_one():
    w = {"A": 0.6, "B": 0.3, "C": 0.1}
    result = _apply_weight_cap(w, 0.35)
    assert math.isclose(sum(result.values()), 1.0, rel_tol=1e-6)
    for v in result.values():
        assert v <= 0.35 + 1e-9


# ──────────────────────────────────────────────
# _compute_weight_variants
# ──────────────────────────────────────────────


def test_compute_weight_variants_all_new():
    # No prior holdings: all 3 symbols are entries
    full, slot, prop = _compute_weight_variants(
        new_holdings={"A", "B", "C"},
        entries={"A", "B", "C"},
        exits=set(),
        marg_weights={},
        prop_weights={},
        size=3,
    )
    for v in full.values():
        assert math.isclose(v, 1 / 3, rel_tol=1e-6)
    assert math.isclose(sum(slot.values()), 1.0, rel_tol=1e-6)
    assert math.isclose(sum(prop.values()), 1.0, rel_tol=1e-6)


def test_compute_weight_variants_no_change():
    # Same holdings, no entries or exits
    w = {"A": 0.4, "B": 0.35, "C": 0.25}
    full, slot, prop = _compute_weight_variants(
        new_holdings={"A", "B", "C"},
        entries=set(),
        exits=set(),
        marg_weights=w,
        prop_weights=w,
        size=3,
    )
    for v in full.values():
        assert math.isclose(v, 1 / 3, rel_tol=1e-6)
    # Slot and prop incumbents keep their normalised weights
    assert math.isclose(sum(slot.values()), 1.0, rel_tol=1e-6)
    assert math.isclose(sum(prop.values()), 1.0, rel_tol=1e-6)


def test_compute_weight_variants_partial_rotation():
    # D exits, E enters; A/B/C carry over
    marg = {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}
    full, slot, prop = _compute_weight_variants(
        new_holdings={"A", "B", "C", "E"},
        entries={"E"},
        exits={"D"},
        marg_weights=marg,
        prop_weights=marg,
        size=4,
    )
    for v in full.values():
        assert math.isclose(v, 0.25, rel_tol=1e-6)
    assert "E" in slot and "D" not in slot
    assert math.isclose(sum(slot.values()), 1.0, rel_tol=1e-6)
    assert math.isclose(sum(prop.values()), 1.0, rel_tol=1e-6)


def test_compute_weight_variants_full_is_always_equal():
    full, _, _ = _compute_weight_variants(
        new_holdings={"X", "Y"},
        entries={"X"},
        exits={"Z"},
        marg_weights={"Y": 0.7, "Z": 0.3},
        prop_weights={"Y": 0.7, "Z": 0.3},
        size=2,
    )
    for v in full.values():
        assert math.isclose(v, 0.5, rel_tol=1e-6)


# ──────────────────────────────────────────────
# rank_universe_at_date
# ──────────────────────────────────────────────


def test_rank_universe_empty_ohlcv():
    result = rank_universe_at_date({}, pd.Timestamp("2023-01-01"), "Average of 3/6/9/12 months")
    assert result == []


def test_rank_universe_insufficient_history_filtered():
    # Only 100 rows — below min_history_days=750
    df = make_ohlcv(100)
    result = rank_universe_at_date(
        {"A": df},
        as_of=df.index[-1],
        sort_method="Average of 3/6/9/12 months",
        min_history_days=750,
        apply_volume_filter=False,
    )
    assert result == []


def test_rank_universe_valid_symbols_filter():
    rising = [100.0 + i * 0.05 for i in range(800)]
    df = make_ohlcv(800, close=rising)
    all_ohlcv = {"A": df, "B": df, "C": df}
    result = rank_universe_at_date(
        all_ohlcv,
        as_of=df.index[-1],
        sort_method="Average of 3/6/9/12 months",
        valid_symbols={"A", "C"},
        min_history_days=750,
        apply_volume_filter=False,
    )
    assert "B" not in result
    assert set(result).issubset({"A", "C"})


def test_rank_universe_volume_filter_excludes_low_vol():
    from config import MIN_VOLUME

    # Rising prices so Sharpe is non-zero and stocks aren't filtered by ranking
    rising = [100.0 + i * 0.05 for i in range(800)]
    df_low = make_ohlcv(800, close=rising, volume=MIN_VOLUME // 2)
    df_ok = make_ohlcv(800, close=rising, volume=MIN_VOLUME * 10)
    result = rank_universe_at_date(
        {"LOW": df_low, "OK": df_ok},
        as_of=df_ok.index[-1],
        sort_method="Average of 3/6/9/12 months",
        min_history_days=750,
        apply_volume_filter=True,
    )
    assert "LOW" not in result
    assert "OK" in result


# ──────────────────────────────────────────────
# run_backtest critical regressions
# ──────────────────────────────────────────────


def test_run_backtest_rejects_invalid_ohlcv_schema():
    bad = pd.DataFrame({"Close": [100.0, 101.0]}, index=pd.bdate_range("2023-01-01", periods=2))
    cfg = BacktestConfig(
        m=1,
        n=2,
        rebalance_freq="weekly",
        sort_method="3 months",
        start_date="2023-01-01",
        end_date="2023-02-01",
        min_history_days=1,
        apply_volume_filter=False,
    )
    result = run_backtest({"BAD": bad}, {}, cfg)
    assert "error" in result
    assert "missing required OHLCV columns" in result["error"]


def test_run_backtest_taxes_partial_full_rebalance_trims():
    n = 330
    dates = pd.bdate_range("2022-01-03", periods=n)
    # A and B stay in the top-2, but A rises faster. Full rebalance trims A
    # back to equal weight on later rebalances, creating taxable partial sells.
    a = [100.0 * (1.0015**i) * (1 + (0.002 if i % 2 else -0.001)) for i in range(n)]
    b = [100.0 * (1.0007**i) * (1 + (0.001 if i % 3 else -0.001)) for i in range(n)]
    c = [100.0 * (0.9995**i) * (1 + (0.001 if i % 2 else -0.001)) for i in range(n)]
    all_ohlcv = {
        "A": make_ohlcv(n, close=a, start=str(dates[0].date())),
        "B": make_ohlcv(n, close=b, start=str(dates[0].date())),
        "C": make_ohlcv(n, close=c, start=str(dates[0].date())),
    }
    start = str(dates[260].date())
    end = str(dates[-1].date())
    cfg_no_tax = BacktestConfig(
        m=2,
        n=3,
        rebalance_freq="weekly",
        sort_method="3 months",
        start_date=start,
        end_date=end,
        transaction_cost_pct=0.0,
        min_history_days=252,
        apply_volume_filter=False,
    )
    cfg_tax = BacktestConfig(
        m=2,
        n=3,
        rebalance_freq="weekly",
        sort_method="3 months",
        start_date=start,
        end_date=end,
        transaction_cost_pct=0.0,
        min_history_days=252,
        apply_volume_filter=False,
        stcg_rate=0.20,
        ltcg_rate=0.125,
    )

    no_tax = run_backtest(all_ohlcv, {}, cfg_no_tax)
    with_tax = run_backtest(all_ohlcv, {}, cfg_tax)

    assert "error" not in no_tax
    assert "error" not in with_tax
    assert with_tax["stats"].loc["Full Rebalance", "Tax Drag (%)"] > 0
    assert with_tax["stats"].loc["Full Rebalance", "Final NAV"] < no_tax["stats"].loc["Full Rebalance", "Final NAV"]
