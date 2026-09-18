import os
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
HISTORY_PERIOD = "2y"
HISTORY_DAYS = 750  # calendar days ≈ 2y; screener requests last 550 of these for scoring

# ── Screener parquet paths (relative to repo root) ──────────────────────────
_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
SCREENER_OHLCV_PARQUET = os.path.join(_DATA_DIR, "screener_ohlcv.parquet")
BACKTEST_HISTORY_PARQUET = os.path.join(_DATA_DIR, "backtest_history.parquet")
STAGE2_CACHE_PARQUET = os.path.join(_DATA_DIR, "stage2_cache.parquet")
# Historical per-symbol Stage 2 classifications used by the breadth dashboard.
# Unlike STAGE2_CACHE_PARQUET, this is derived from the full OHLCV history.
STAGE2_BREADTH_CACHE_PARQUET = os.path.join(_DATA_DIR, "stage2_breadth_scores.parquet")
STAGE2_BREADTH_CACHE_META = os.path.join(_DATA_DIR, "stage2_breadth_scores.meta.json")
INDEX_OVERLAY_CACHE_PARQUET = os.path.join(_DATA_DIR, "index_overlay_prices.parquet")
MOMENTUM_CACHE_PARQUET = os.path.join(_DATA_DIR, "momentum_cache.parquet")
MIN_VOLUME = 100_000
VOL_AVG_PERIOD = 10
HH_HL_LOOKBACK = 50
MA_RISING_LOOKBACK = 50
CONSOLIDATION_LOOKBACK = 20  # days to measure base flatness
CONSOLIDATION_RANGE_PCT = 0.15  # max close-to-close range (as % of period low) to qualify as a base

CIRCUIT_LEVELS = [5.0, 10.0, 20.0]
CIRCUIT_TOLERANCE = 0.1

# Weinstein Retest Detection
RETEST_LOOKBACK_DAYS = 20  # days to look back for the initial breakout
RETEST_TOLERANCE = 0.02  # ±2% proximity band around the breakout level
BOUNCE_CONFIRMATION = 0.02  # current close must be ≥2% above breakout level
VOL_DRYUP_RATIO = 0.75  # pullback avg vol must be < 75% of breakout-day vol
