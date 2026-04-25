import numpy as np
import pandas as pd

from config import (
    BOUNCE_CONFIRMATION,
    CONSOLIDATION_LOOKBACK,
    CONSOLIDATION_RANGE_PCT,
    HH_HL_LOOKBACK,
    MA_RISING_LOOKBACK,
    MIN_VOLUME,
    RETEST_LOOKBACK_DAYS,
    RETEST_TOLERANCE,
    VOL_AVG_PERIOD,
    VOL_DRYUP_RATIO,
)


def _rsi_wilder(series: pd.Series, period: int = 14) -> pd.Series:
    """Compute Wilder's RSI using exponential weighted moving averages."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    # When avg_loss == 0 all gains, RSI should be 100 (not NaN)
    rsi = 100 - (100 / (1 + rs))
    return rsi.where(avg_loss != 0, 100.0)


def compute_rolling_stage2(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised daily Stage 2 score; returns df with Close/MA cols, Score (0-8), and Phase."""
    c, v = df["Close"], df["Volume"].astype(float)
    ma50 = c.rolling(50).mean()
    ma150 = c.rolling(150).mean()
    ma200 = c.rolling(200).mean()
    avg_vol = v.rolling(VOL_AVG_PERIOD).mean()

    # Higher high: close broke above 50-day prior high
    higher_high = (c >= c.rolling(HH_HL_LOOKBACK).max().shift(1)).astype(int)
    # Higher low: recent 20-day low is above the 50-day low from HH_HL_LOOKBACK bars ago
    higher_low = (c.rolling(20).min().shift(1) > c.rolling(HH_HL_LOOKBACK).min().shift(HH_HL_LOOKBACK)).astype(int)

    consol_range = (c.rolling(CONSOLIDATION_LOOKBACK).max() - c.rolling(CONSOLIDATION_LOOKBACK).min()) / c.rolling(
        CONSOLIDATION_LOOKBACK
    ).min()
    consolidation = (consol_range < CONSOLIDATION_RANGE_PCT).astype(int)

    score = (
        (v / avg_vol >= 2.0).astype(int)
        + higher_high
        + higher_low
        + ((c > ma50) & (ma50 > ma50.shift(MA_RISING_LOOKBACK))).astype(int)
        + ((c > ma200) & (ma200 > ma200.shift(MA_RISING_LOOKBACK))).astype(int)
        + (c > ma150).astype(int)
        + ((ma50 > ma150) & (ma150 > ma200)).astype(int)
        + consolidation
    )

    phase = pd.cut(
        score,
        bins=[-1, 1, 3, 5, 8],
        labels=["Not Stage 2", "Early/Weak Stage 2", "Likely Stage 2", "Strong Stage 2"],
    )

    result = pd.DataFrame(
        {"Close": c, "MA50": ma50, "MA150": ma150, "MA200": ma200, "Score": score, "Phase": phase},
        index=df.index,
    )
    return result


def score_stage2(df: pd.DataFrame) -> dict | None:
    """Score a stock on 8 Weinstein Stage 2 criteria; returns metric dict or None if insufficient data."""
    if len(df) < 250:
        return None
    c, v = df["Close"], df["Volume"]
    ma50 = c.rolling(50).mean()
    ma150 = c.rolling(150).mean()
    ma200 = c.rolling(200).mean()
    avg_vol = v.rolling(VOL_AVG_PERIOD).mean()
    rsi = _rsi_wilder(c)

    c1, v1 = c.iloc[-1], v.iloc[-1]
    m50, m150, m200 = ma50.iloc[-1], ma150.iloc[-1], ma200.iloc[-1]
    r = rsi.iloc[-1]
    vr = v1 / avg_vol.iloc[-1] if avg_vol.iloc[-1] > 0 else 0

    if np.isnan([m50, m150, m200, vr, r]).any():
        return None

    score = 0
    if vr >= 2.0:
        score += 1
    if c1 >= c.rolling(HH_HL_LOOKBACK).max().shift(1).iloc[-1]:
        score += 1
    # Higher low: recent 20-day low above the 50-day low from HH_HL_LOOKBACK bars ago
    recent_low = c.rolling(20).min().iloc[-2]
    older_low = c.rolling(HH_HL_LOOKBACK).min().iloc[-HH_HL_LOOKBACK]
    if recent_low > older_low:
        score += 1
    if c1 > m50 and ma50.iloc[-1] > ma50.iloc[-MA_RISING_LOOKBACK]:
        score += 1
    if c1 > m200 and ma200.iloc[-1] > ma200.iloc[-MA_RISING_LOOKBACK]:
        score += 1
    if c1 > m150:
        score += 1
    if m50 > m150 > m200:
        score += 1
    # Consolidation: close range over last N days < X% (base formation check)
    lookback_closes = c.iloc[-CONSOLIDATION_LOOKBACK:]
    consol_range = (lookback_closes.max() - lookback_closes.min()) / lookback_closes.min()
    consolidating = bool(consol_range < CONSOLIDATION_RANGE_PCT)
    if consolidating:
        score += 1

    if score >= 6:
        stage = "🟢 Strong Stage 2"
    elif score >= 4:
        stage = "🟡 Likely Stage 2"
    elif score >= 2:
        stage = "🟠 Early/Weak Stage 2"
    else:
        stage = "⚪ Not Stage 2"

    return {
        "Score": score,
        "Stage": stage,
        "Illiquid": avg_vol.iloc[-1] < MIN_VOLUME,
        "Close": round(c1, 2),
        "Volume": int(v1),
        "Vol_Ratio": round(vr, 2),
        "RSI": round(r, 1),
        "MA50": round(m50, 2),
        "MA150": round(m150, 2),
        "MA200": round(m200, 2),
        "MA_Stack": m50 > m150 > m200,
        "Consolidating": consolidating,
        "Avg_Vol": int(np.floor(avg_vol.iloc[-1])),
    }


def check_weinstein_retest(df: pd.DataFrame) -> bool:
    """Return True if the stock recently broke out on volume and has since retested
    that breakout level with volume contraction and a confirmed bounce."""
    if len(df) < RETEST_LOOKBACK_DAYS + 50:
        return False

    c = df["Close"]
    v = df["Volume"]
    avg_vol = v.rolling(VOL_AVG_PERIOD).mean()

    # 1. Find the most recent 50-day closing-high breakout with volume confirmation
    hh_50 = c.rolling(50).max()
    breakout_mask = (c == hh_50) & (v / avg_vol >= 2.0)

    # Shift by 1 — we want a pullback *after* the breakout day, not on it
    breakout_mask = breakout_mask.shift(1).fillna(False)

    recent_breakouts = df.index[breakout_mask & (df.index >= df.index[-RETEST_LOOKBACK_DAYS])]
    if len(recent_breakouts) == 0:
        return False

    last_breakout_idx = recent_breakouts[-1]
    breakout_level = c.loc[last_breakout_idx]
    breakout_vol = v.loc[last_breakout_idx]

    # 2. Check that price pulled back to within ±RETEST_TOLERANCE of the breakout level
    pullback_period = df.loc[last_breakout_idx:].iloc[1:]  # exclude breakout day itself
    if pullback_period.empty:
        return False

    pullback_low = pullback_period["Low"].min()
    near_breakout = pullback_low <= breakout_level * (1 + RETEST_TOLERANCE) and pullback_low >= breakout_level * (
        1 - RETEST_TOLERANCE
    )
    if not near_breakout:
        return False

    # 3. Current close has bounced ≥ BOUNCE_CONFIRMATION above the breakout level
    if c.iloc[-1] < breakout_level * (1 + BOUNCE_CONFIRMATION):
        return False

    # 4. Volume dried up during the pullback (avg < VOL_DRYUP_RATIO × breakout-day vol)
    if pullback_period["Volume"].mean() > breakout_vol * VOL_DRYUP_RATIO:
        return False

    return True
