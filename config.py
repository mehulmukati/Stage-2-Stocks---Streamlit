from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
HISTORY_PERIOD = "2y"
MIN_VOLUME = 100_000
VOL_AVG_PERIOD = 10
HH_HL_LOOKBACK = 50
MA_RISING_LOOKBACK = 50

CIRCUIT_LEVELS = [5.0, 10.0, 20.0]
CIRCUIT_TOLERANCE = 0.1

_MOMENTUM_TTL = 3600  # seconds before in-memory momentum data is considered stale
