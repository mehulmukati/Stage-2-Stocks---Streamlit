from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
HISTORY_PERIOD = "5y"
MIN_VOLUME = 100_000
VOL_AVG_PERIOD = 10
HH_HL_LOOKBACK = 50
MA_RISING_LOOKBACK = 50

CIRCUIT_LEVELS = [5.0, 10.0, 20.0]
CIRCUIT_TOLERANCE = 0.1

# Weinstein Retest Detection
RETEST_LOOKBACK_DAYS = 20   # days to look back for the initial breakout
RETEST_TOLERANCE = 0.02     # ±2% proximity band around the breakout level
BOUNCE_CONFIRMATION = 0.02  # current close must be ≥2% above breakout level
VOL_DRYUP_RATIO = 0.75      # pullback avg vol must be < 75% of breakout-day vol

_MOMENTUM_TTL = 3600  # seconds before in-memory momentum data is considered stale
