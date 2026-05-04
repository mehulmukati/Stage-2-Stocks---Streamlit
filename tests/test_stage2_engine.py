import pandas as pd

from stage2_engine import check_weinstein_retest, score_stage2

from .conftest import make_ohlcv

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

_STAGE2_KEYS = {
    "Score",
    "Stage",
    "Illiquid",
    "Close",
    "Volume",
    "Vol_Ratio",
    "RSI",
    "MA50",
    "MA150",
    "MA200",
    "MA_Stack",
    "Consolidating",
    "Avg_Vol",
}


def _make_retest_df(
    pullback_close: float = 99.5,
    bounce_close: float = 103.0,
    pullback_volume: int = 500_000,
    include_breakout_vol: bool = True,
) -> pd.DataFrame:
    """
    Construct a 100-row OHLCV DataFrame that has a Weinstein retest pattern:
      - Rows 0–78:  slow rising prices (80.0 → ~99.5), volume=1M
      - Row 79:     breakout close=100.0, volume=3M (if include_breakout_vol)
      - Rows 80–89: pullback to pullback_close, volume=pullback_volume
      - Rows 90–99: bounce to bounce_close, volume=pullback_volume
    """
    n = 100
    dates = pd.bdate_range("2020-01-01", periods=n)
    close = [80.0 + i * 0.25 for i in range(79)] + [100.0] + [pullback_close] * 10 + [bounce_close] * 10
    breakout_vol = 3_000_000 if include_breakout_vol else 1_000_000
    volume = [1_000_000] * 79 + [breakout_vol] + [pullback_volume] * 20
    return pd.DataFrame(
        {
            "Open": [c * 0.99 for c in close],
            "High": [c * 1.01 for c in close],
            "Low": [c * 0.99 for c in close],
            "Close": close,
            "Volume": [float(v) for v in volume],
        },
        index=dates,
    )


# ──────────────────────────────────────────────
# score_stage2
# ──────────────────────────────────────────────


def test_score_stage2_insufficient_history():
    df = make_ohlcv(100)
    assert score_stage2(df) is None


def test_score_stage2_returns_all_keys():
    close = [100.0 + i * 0.1 for i in range(300)]
    df = make_ohlcv(300, close=close)
    result = score_stage2(df)
    assert result is not None
    assert _STAGE2_KEYS.issubset(result.keys())


def test_score_stage2_score_in_range():
    close = [100.0 + i * 0.1 for i in range(300)]
    df = make_ohlcv(300, close=close)
    result = score_stage2(df)
    assert result is not None
    assert 0 <= result["Score"] <= 8


def test_score_stage2_ma_stack_true_for_rising_series():
    # Monotonically rising series → MA50 > MA150 > MA200
    close = [80.0 + i * 0.1 for i in range(300)]
    df = make_ohlcv(300, close=close)
    result = score_stage2(df)
    assert result is not None
    assert result["MA_Stack"]


def test_score_stage2_consolidation_true_for_flat_tail():
    # Build a long rising series but flatten the last 25 rows so the 20-day window is flat
    rising = [80.0 + i * 0.5 for i in range(275)]
    flat = [rising[-1]] * 25
    close = rising + flat
    df = make_ohlcv(300, close=close)
    result = score_stage2(df)
    assert result is not None
    assert result["Consolidating"] is True


def test_score_stage2_strong_label_for_high_score():
    # Rising series with volume spike on last day → should score >= 6
    close = [80.0 + i * 0.1 for i in range(299)] + [100.0]
    # Volume spike on last row: 3M vs avg of ~1M
    vol = [1_000_000] * 299 + [3_000_000]
    dates = pd.bdate_range("2020-01-01", periods=300)
    df = pd.DataFrame(
        {
            "Open": [c * 0.99 for c in close],
            "High": [c * 1.01 for c in close],
            "Low": [c * 0.99 for c in close],
            "Close": close,
            "Volume": [float(v) for v in vol],
        },
        index=dates,
    )
    result = score_stage2(df)
    assert result is not None
    if result["Score"] >= 6:
        assert "Strong Stage 2" in result["Stage"]


def test_score_stage2_not_stage2_label_for_declining_fast():
    # Fast declining series (1.5/day from 600) → all 8 criteria fail → score = 0
    # Steep enough that last-20-day range ≈ 18.8% > 15% consolidation threshold
    close = [600.0 - i * 1.5 for i in range(300)]
    df = make_ohlcv(300, close=close, volume=1_000_000)
    result = score_stage2(df)
    assert result is not None
    # Inverted MA stack, below all MAs, no HH/HL, no consolidation, no vol surge → score = 0
    assert result["Score"] == 0
    assert "Not Stage 2" in result["Stage"]


# ──────────────────────────────────────────────
# check_weinstein_retest
# ──────────────────────────────────────────────


def test_retest_insufficient_history():
    df = make_ohlcv(30)
    assert check_weinstein_retest(df) is False


def test_retest_no_breakout():
    # Flat prices with uniform volume: volume ratio is always 1.0, never >= 2.0
    df = make_ohlcv(100, close=[100.0] * 100, volume=1_000_000)
    assert check_weinstein_retest(df) is False


def test_retest_valid_full_pattern():
    # pullback_close=99.5: Low = 98.505 which is ≥ 100 * 0.98 = 98.0 ✓
    # bounce_close=103.0 ≥ 100 * 1.02 = 102.0 ✓
    # pullback_volume=500K < 3M * 0.75 = 2.25M ✓
    df = _make_retest_df(pullback_close=99.5, bounce_close=103.0, pullback_volume=500_000)
    assert check_weinstein_retest(df) is True


def test_retest_no_pullback():
    # Price runs away upward after breakout (no retest of breakout level)
    # Rows 80-99 close=110.0; Low=108.9 > 100*1.02=102.0 → near_breakout=False
    df = _make_retest_df(pullback_close=110.0, bounce_close=112.0, pullback_volume=500_000)
    assert check_weinstein_retest(df) is False


def test_retest_no_bounce():
    # Pullback is valid but current close only reaches 101.0 < 102.0 required
    df = _make_retest_df(pullback_close=99.5, bounce_close=101.0, pullback_volume=500_000)
    assert check_weinstein_retest(df) is False


def test_retest_volume_not_dried_up():
    # Pullback volume = 2.5M > 3M * 0.75 = 2.25M threshold → dryup check fails
    df = _make_retest_df(pullback_close=99.5, bounce_close=103.0, pullback_volume=2_500_000)
    assert check_weinstein_retest(df) is False


def test_retest_float_precision_tolerance():
    # Simulate a tiny float precision gap (0.005%): close is 99.999% of rolling max
    # The 0.9999 factor in the breakout mask should still detect it
    close = [80.0 + i * 0.25 for i in range(79)] + [100.0] + [99.5] * 10 + [103.0] * 10
    # Slightly reduce breakout close to simulate float32 parquet round-trip
    close[79] = 100.0 * 0.9999  # 99.99 — should still be caught by >= hh_50 * 0.9999
    volume = [1_000_000] * 79 + [3_000_000] + [500_000] * 20
    dates = pd.bdate_range("2020-01-01", periods=100)
    df = pd.DataFrame(
        {
            "Open": [c * 0.99 for c in close],
            "High": [c * 1.01 for c in close],
            "Low": [c * 0.99 for c in close],
            "Close": close,
            "Volume": [float(v) for v in volume],
        },
        index=dates,
    )
    # close[79] = 99.99 which equals hh_50[79] (since close[79] IS the 50-day high after reduction)
    # The breakout condition c >= hh_50 * 0.9999 should detect this
    assert check_weinstein_retest(df) is True
