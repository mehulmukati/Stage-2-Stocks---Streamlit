import numpy as np
import pandas as pd

from config import (HH_HL_LOOKBACK, MA_RISING_LOOKBACK, MIN_VOLUME,
                    VOL_AVG_PERIOD)


def _rsi_wilder(series: pd.Series, period: int = 14) -> pd.Series:
    """Compute Wilder's RSI using exponential weighted moving averages."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


import streamlit as st


@st.cache_data(ttl=3600)
def compute_rolling_stage2(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised daily Stage 2 score; returns df with Close/MA cols, Score (0-7), and Phase."""
    c, h, l, v = df["Close"], df["High"], df["Low"], df["Volume"].astype(float)
    ma50 = c.rolling(50).mean()
    ma150 = c.rolling(150).mean()
    ma200 = c.rolling(200).mean()
    avg_vol = v.rolling(VOL_AVG_PERIOD).mean()

    score = (
        (v / avg_vol >= 2.0).astype(int)
        + (c >= c.rolling(HH_HL_LOOKBACK).max().shift(1)).astype(int)
        + (c >= c.rolling(HH_HL_LOOKBACK).min().shift(1)).astype(int)
        + ((c > ma50) & (ma50 > ma50.shift(MA_RISING_LOOKBACK))).astype(int)
        + ((c > ma200) & (ma200 > ma200.shift(MA_RISING_LOOKBACK))).astype(int)
        + (c > ma150).astype(int)
        + ((ma50 > ma150) & (ma150 > ma200)).astype(int)
    )

    phase = pd.cut(
        score,
        bins=[-1, 1, 3, 5, 7],
        labels=["Not Stage 2", "Early/Weak Stage 2", "Likely Stage 2", "Strong Stage 2"],
    )

    result = pd.DataFrame(
        {"Close": c, "MA50": ma50, "MA150": ma150, "MA200": ma200, "Score": score, "Phase": phase},
        index=df.index,
    )
    return result


def score_stage2(df: pd.DataFrame) -> dict | None:
    """Score a stock on 7 Weinstein Stage 2 criteria; returns metric dict or None if insufficient data."""
    if len(df) < 250:
        return None
    c, h, l, v = df["Close"], df["High"], df["Low"], df["Volume"]
    ma50 = c.rolling(50).mean()
    ma150 = c.rolling(150).mean()
    ma200 = c.rolling(200).mean()
    avg_vol = v.rolling(VOL_AVG_PERIOD).mean()
    rsi = _rsi_wilder(c)

    c1, h1, l1, v1 = c.iloc[-1], h.iloc[-1], l.iloc[-1], v.iloc[-1]
    m50, m150, m200 = ma50.iloc[-1], ma150.iloc[-1], ma200.iloc[-1]
    r = rsi.iloc[-1]
    vr = v1 / avg_vol.iloc[-1] if avg_vol.iloc[-1] > 0 else 0

    if np.isnan([m50, m150, m200, vr, r]).any():
        return None

    score = 0
    if vr >= 2.0:
        score += 1
    if h1 >= h.rolling(HH_HL_LOOKBACK).max().shift(1).iloc[-1]:
        score += 1
    if l1 >= l.rolling(HH_HL_LOOKBACK).min().shift(1).iloc[-1]:
        score += 1
    if c1 > m50 and ma50.iloc[-1] > ma50.iloc[-MA_RISING_LOOKBACK]:
        score += 1
    if c1 > m200 and ma200.iloc[-1] > ma200.iloc[-MA_RISING_LOOKBACK]:
        score += 1
    if c1 > m150:
        score += 1
    if m50 > m150 > m200:
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
        "Avg_Vol": int(np.floor(avg_vol.iloc[-1])),
    }
