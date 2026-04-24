"""
Database layer — PostgreSQL via Neon (psycopg v3), direct per-call connections.
Requires DATABASE_URL env var pointing to a postgresql:// connection string.

No connection pool is used: Neon is a serverless database that suspends after
inactivity and can take several seconds to cold-start.  A pool's background
keep-alive threads fight this behaviour and produce spurious timeout errors.
Direct connections are opened on demand, with a retry loop that absorbs
cold-start latency transparently.
"""

import io
import os
import re
import time
from contextlib import contextmanager

import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()


def _build_conninfo() -> str:
    """
    Return the PostgreSQL connection string from DATABASE_URL.

    - Neon hosts (ep-* pattern): injects the required endpoint option.
    - Supabase hosts (supabase.com): injects sslmode=require, which the
      Session Pooler mandates but psycopg doesn't enable by default.
    """
    url = os.environ["DATABASE_URL"]

    # Neon: inject endpoint parameter
    m = re.search(r"@(ep-[^.]+)\.", url)
    if m and "options=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}options=endpoint%3D{m.group(1)}"

    # Supabase: require SSL (Session Pooler closes non-SSL connections)
    if "supabase.com" in url and "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"

    return url


_PERMANENT_ERRORS = (
    "quota",
    "does not exist",
    "password authentication failed",
    "pg_hba.conf",
    "role",
    "database",
    "getaddrinfo",          # DNS resolution failure — retrying won't help
    "resolve host",         # same class of error, different wording
    "name or service",      # POSIX variant: "Name or service not known"
    "tenant or user not",   # Supabase pooler: wrong username format
    "no pg_hba.conf",       # auth config mismatch
)


def _is_transient(exc: Exception) -> bool:
    """Return True only for errors that are worth retrying (cold-start, network blip)."""
    msg = str(exc).lower()
    return not any(kw in msg for kw in _PERMANENT_ERRORS)


def _masked_url() -> str:
    """Return DATABASE_URL with the password replaced by *** for safe logging."""
    url = os.environ.get("DATABASE_URL", "")
    return re.sub(r"(:)[^:@]+(@)", r"\1***\2", url)


@contextmanager
def _get_conn():
    """
    Open a direct psycopg connection, yield it, then close it.

    Retries up to 3 times (with 10 s / 20 s back-off) for transient failures
    such as Neon cold-starts.  Permanent errors (quota exceeded, bad credentials,
    unknown database, etc.) are re-raised immediately without retrying.
    """
    conn: psycopg.Connection | None = None
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            conn = psycopg.connect(_build_conninfo(), connect_timeout=30)
            break
        except Exception as exc:
            last_exc = exc
            if not _is_transient(exc) or attempt == 2:
                raise RuntimeError(
                    f"DB connection failed [{_masked_url()}]: {exc}"
                ) from exc
            time.sleep(10 * (attempt + 1))   # 10 s, then 20 s
    try:
        yield conn  # type: ignore[misc]
    finally:
        conn.close()  # type: ignore[union-attr]


# ──────────────────────────────────────────────
# SCHEMA INIT
# ──────────────────────────────────────────────
def init_db():
    """Create ohlcv, index_ohlcv, and stage2_cache tables (and indexes) if they do not already exist."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                symbol  TEXT    NOT NULL,
                date    TEXT    NOT NULL,
                open    FLOAT,
                high    FLOAT,
                low     FLOAT,
                close   FLOAT,
                volume  BIGINT,
                PRIMARY KEY (symbol, date)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ohlcv_date_idx ON ohlcv (date)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_ohlcv (
                symbol  TEXT    NOT NULL,
                date    TEXT    NOT NULL,
                close   FLOAT,
                PRIMARY KEY (symbol, date)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS index_ohlcv_date_idx ON index_ohlcv (date)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stage2_cache (
                cache_date  DATE      PRIMARY KEY,
                results     JSONB     NOT NULL,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()


# ──────────────────────────────────────────────
# OHLCV — WRITE
# ──────────────────────────────────────────────
def upsert_ohlcv(records: list[dict]):
    """Bulk-upsert OHLCV rows, updating price/volume fields on duplicate (symbol, date) keys."""
    if not records:
        return
    rows = [
        (
            r["symbol"],
            str(r["date"]),
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume"],
        )
        for r in records
    ]
    sql = """
        INSERT INTO ohlcv (symbol, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open=excluded.open, high=excluded.high, low=excluded.low,
            close=excluded.close, volume=excluded.volume
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows, returning=False)
        conn.commit()


# ──────────────────────────────────────────────
# OHLCV — READ
# ──────────────────────────────────────────────
def get_latest_ohlcv_date() -> tuple[str | None, str | None]:
    """
    Returns (global_max, conservative_min) where:
      global_max       = MAX(date) across all rows  → used to check if DB was synced recently
      conservative_min = MIN of per-symbol MAX dates → used as fetch-start to fill any gaps
    """
    with _get_conn() as conn:
        gmax = conn.execute("SELECT MAX(date) FROM ohlcv").fetchone()[0]
        gmin = conn.execute("""
            SELECT MIN(max_date) FROM (
                SELECT symbol, MAX(date) AS max_date FROM ohlcv GROUP BY symbol
            ) t
        """).fetchone()[0]
        return gmax, gmin


def get_earliest_ohlcv_date() -> str | None:
    """Return the earliest date present in the ohlcv table, or None if the table is empty."""
    with _get_conn() as conn:
        result = conn.execute("SELECT MIN(date) FROM ohlcv").fetchone()[0]
    return str(result) if result else None


def load_ohlcv_all(period_days: int = 550) -> dict[str, pd.DataFrame]:
    """Load the last period_days of OHLCV data; returns a symbol→DataFrame dict with proper column names."""
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT symbol, date, open, high, low, close, volume
            FROM ohlcv
            WHERE date::date >= NOW() - make_interval(days => %s)
            ORDER BY symbol, date
            """,
            (int(period_days),),
        ).fetchall()

    df = pd.DataFrame(
        rows, columns=["symbol", "date", "open", "high", "low", "close", "volume"]
    )
    if df.empty:
        return {}

    result: dict[str, pd.DataFrame] = {}
    for symbol, grp in df.groupby("symbol"):
        sub = grp.drop(columns="symbol").copy()
        sub["date"] = pd.to_datetime(sub["date"])
        sub = sub.set_index("date")
        sub.columns = ["Open", "High", "Low", "Close", "Volume"]
        sub["Volume"] = sub["Volume"].astype("Int64")
        result[symbol] = sub
    return result


def load_ohlcv_symbol(symbol: str, period_days: int = 750) -> pd.DataFrame:
    """Load OHLCV history for a single symbol from DB; returns empty DataFrame if not found."""
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol = %s AND date::date >= NOW() - make_interval(days => %s)
            ORDER BY date
            """,
            (symbol, int(period_days)),
        ).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "Open", "High", "Low", "Close", "Volume"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df["Volume"] = df["Volume"].astype("Int64")
    return df


# ──────────────────────────────────────────────
# STAGE 2 CACHE — WRITE / READ
# ──────────────────────────────────────────────
def save_stage2_cache(cache_date: str, df: pd.DataFrame):
    """Persist scored Stage 2 results for cache_date as JSON; overwrites any existing entry."""
    payload = df.to_json(orient="records")
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO stage2_cache (cache_date, results)
            VALUES (%s, %s)
            ON CONFLICT (cache_date) DO UPDATE SET results=EXCLUDED.results, created_at=NOW()
        """,
            (cache_date, payload),
        )
        conn.commit()


def _jsonb_to_df(value) -> pd.DataFrame:
    """Convert a psycopg JSONB value (already a Python object) or raw JSON string to a DataFrame."""
    if isinstance(value, str):
        return pd.read_json(io.StringIO(value), orient="records")
    return pd.DataFrame(value)


def load_stage2_cache(cache_date: str) -> pd.DataFrame | None:
    """Return cached Stage 2 results for a specific trading date, or None if not found."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT results FROM stage2_cache WHERE cache_date = %s", (cache_date,)
        ).fetchone()
    if row:
        return _jsonb_to_df(row[0])
    return None


# ──────────────────────────────────────────────
# INDEX OHLCV (benchmarks)
# ──────────────────────────────────────────────
def upsert_index_ohlcv(records: list[dict]):
    """Bulk-upsert benchmark index close prices (symbol, date, close)."""
    if not records:
        return
    rows = [(r["symbol"], str(r["date"]), r["close"]) for r in records]
    sql = """
        INSERT INTO index_ohlcv (symbol, date, close)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET close=excluded.close
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows, returning=False)
        conn.commit()


def load_index_ohlcv(symbol: str) -> pd.Series:
    """Return a date-indexed close price Series for a benchmark index symbol."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT date, close FROM index_ohlcv WHERE symbol = %s ORDER BY date",
            (symbol,),
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows, columns=["date", "close"])
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")["close"]


def get_latest_index_date(symbol: str) -> str | None:
    """Return the most recent date stored for a benchmark index symbol."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(date) FROM index_ohlcv WHERE symbol = %s", (symbol,)
        ).fetchone()
    return row[0] if row else None


def load_latest_stage2_cache() -> tuple[pd.DataFrame | None, str | None]:
    """Return the most recent Stage 2 cache entry as (DataFrame, date_str) fallback when today's data is unavailable."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT cache_date, results FROM stage2_cache ORDER BY cache_date DESC LIMIT 1"
        ).fetchone()
    if row:
        date_str = row[0] if isinstance(row[0], str) else row[0].strftime("%Y-%m-%d")
        return _jsonb_to_df(row[1]), date_str
    return None, None
