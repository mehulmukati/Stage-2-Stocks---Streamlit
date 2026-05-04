import math

import pytest

from momentum_engine import _calculate_avg_sharpe, _calculate_positive_days_pct, _calculate_sharpe, score_momentum

from .conftest import make_ohlcv

# ──────────────────────────────────────────────
# _calculate_sharpe
# ──────────────────────────────────────────────


def test_sharpe_insufficient_history():
    df = make_ohlcv(10)
    assert _calculate_sharpe(df, 63) is None


def test_sharpe_zero_volatility():
    df = make_ohlcv(100, close=[100.0] * 100)
    assert _calculate_sharpe(df, 63) is None


def test_sharpe_period_boundary():
    # Exactly period_days rows with non-flat prices → not None
    close = [100.0 * (1 + 0.001 * i) for i in range(63)]
    df = make_ohlcv(63, close=close)
    result = _calculate_sharpe(df, 63)
    assert result is not None


def test_sharpe_positive_for_rising_prices():
    close = [100.0 * (1 + 0.001 * i) for i in range(100)]
    df = make_ohlcv(100, close=close)
    result = _calculate_sharpe(df, 63)
    assert result is not None
    assert result > 0


def test_sharpe_negative_for_falling_prices():
    close = [100.0 * (1 - 0.001 * i) for i in range(100)]
    df = make_ohlcv(100, close=close)
    result = _calculate_sharpe(df, 63)
    assert result is not None
    assert result < 0


# ──────────────────────────────────────────────
# _calculate_positive_days_pct
# ──────────────────────────────────────────────


def test_pos_days_insufficient_history():
    df = make_ohlcv(10)
    assert _calculate_positive_days_pct(df, 3) is None


def test_pos_days_all_up():
    close = [100.0 + i for i in range(100)]
    df = make_ohlcv(100, close=close)
    result = _calculate_positive_days_pct(df, 3)
    assert result is not None
    assert math.isclose(result, 100.0, abs_tol=1.0)


def test_pos_days_all_down():
    close = [200.0 - i for i in range(100)]
    df = make_ohlcv(100, close=close)
    result = _calculate_positive_days_pct(df, 3)
    assert result is not None
    assert math.isclose(result, 0.0, abs_tol=1.0)


def test_pos_days_alternating():
    # Alternating up/down ≈ 50%
    close = [100.0 + (1 if i % 2 == 0 else -1) * (i * 0.01) for i in range(100)]
    df = make_ohlcv(100, close=close)
    result = _calculate_positive_days_pct(df, 3)
    assert result is not None
    assert 40.0 <= result <= 60.0


# ──────────────────────────────────────────────
# _calculate_avg_sharpe
# ──────────────────────────────────────────────


def test_avg_sharpe_unknown_method():
    assert _calculate_avg_sharpe({"Sharpe_1Y": 1.0}, "bogus_method") is None


def test_avg_sharpe_all_none_values():
    row = {k: None for k in ["Sharpe_1M", "Sharpe_3M", "Sharpe_6M", "Sharpe_9M", "Sharpe_1Y"]}
    assert _calculate_avg_sharpe(row, "1 year") is None


def test_avg_sharpe_partial_none():
    row = {"Sharpe_3M": 1.0, "Sharpe_6M": None}
    result = _calculate_avg_sharpe(row, "Average of 3/6 months")
    assert result is not None
    assert math.isclose(result, 1.0)


def test_avg_sharpe_nan_excluded():
    row = {"Sharpe_3M": 1.0, "Sharpe_6M": float("nan")}
    result = _calculate_avg_sharpe(row, "Average of 3/6 months")
    assert result is not None
    assert math.isclose(result, 1.0)


@pytest.mark.parametrize(
    "method, expected",
    [
        ("1 year", 2.0),
        ("1Y", 2.0),
        ("3 months", 1.0),
        ("3M", 1.0),
        ("6 months", 1.5),
        ("6M", 1.5),
        ("9 months", 2.5),
        ("9M", 2.5),
        ("Average of 3/6 months", 1.25),
        ("Average of 3/6/9/12 months", 1.75),  # avg(1.0, 1.5, 2.5, 2.0)
        ("Average of 1/3/6/12 months", 1.25),  # avg(0.5, 1.0, 1.5, 2.0)
        ("Average of 1/3/12 months", 7 / 6),  # avg(0.5, 1.0, 2.0)
        ("Average of 1/3/6/9/12 months", 1.5),  # avg(0.5, 1.0, 1.5, 2.5, 2.0)
    ],
)
def test_avg_sharpe_all_methods(method, expected):
    row = {
        "Sharpe_1M": 0.5,
        "Sharpe_3M": 1.0,
        "Sharpe_6M": 1.5,
        "Sharpe_9M": 2.5,
        "Sharpe_1Y": 2.0,
    }
    result = _calculate_avg_sharpe(row, method)
    assert result is not None
    assert math.isclose(result, expected, rel_tol=1e-9)


# ──────────────────────────────────────────────
# score_momentum
# ──────────────────────────────────────────────

_MOMENTUM_KEYS = {
    "Close",
    "52w_High",
    "DMA100",
    "DMA200",
    "Vol_Median",
    "1Y_Change",
    "Pct_From_52W_High",
    "Circuit_Count",
    "Sharpe_3M",
    "Sharpe_6M",
    "Sharpe_9M",
    "Sharpe_1Y",
    "Volatility",
    "Pos_Days_3M",
    "Pos_Days_6M",
    "Pos_Days_12M",
}


def test_score_momentum_insufficient_history():
    df = make_ohlcv(100)
    assert score_momentum(df) is None


def test_score_momentum_returns_all_keys():
    close = [100.0 + i * 0.1 for i in range(300)]
    df = make_ohlcv(300, close=close)
    result = score_momentum(df)
    assert result is not None
    assert _MOMENTUM_KEYS.issubset(result.keys())


def test_score_momentum_circuit_count_zero():
    df = make_ohlcv(300, close=[100.0] * 300)
    result = score_momentum(df)
    assert result is not None
    assert result["Circuit_Count"] == 0


def test_score_momentum_circuit_count_detected():
    close = [100.0] * 300
    # Inject a 5% single-day move near the end (within last 252 rows)
    close[280] = close[279] * 1.05  # exactly +5%
    df = make_ohlcv(300, close=close)
    result = score_momentum(df)
    assert result is not None
    assert result["Circuit_Count"] >= 1


def test_score_momentum_close_value():
    close = [100.0 + i * 0.5 for i in range(300)]
    df = make_ohlcv(300, close=close)
    result = score_momentum(df)
    assert result is not None
    assert math.isclose(result["Close"], close[-1], rel_tol=1e-4)


def test_score_momentum_1y_change_none_when_short():
    # Exactly 250 rows — 1Y_Change needs 252 rows, so should be present but None possible
    close = [100.0 + i * 0.1 for i in range(250)]
    df = make_ohlcv(250, close=close)
    result = score_momentum(df)
    assert result is not None
    # 1Y_Change is computed as pct_change(251); with 250 rows it should be None
    assert result["1Y_Change"] is None
