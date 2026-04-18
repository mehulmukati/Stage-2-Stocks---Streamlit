"""
Database layer — SQLite locally, PostgreSQL (Neon/Render) in production.
Auto-detects from DATABASE_URL prefix:
  sqlite:///path/to/file.db  → SQLite (local dev, no network needed)
  postgresql://...            → PostgreSQL via psycopg v3 (cloud deploy)
"""
import os
import io
import re
import sqlite3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# BACKEND DETECTION
# ──────────────────────────────────────────────
def _db_url() -> str:
    url = os.environ.get("DATABASE_URL", "sqlite:///daily_cache/stocks.db")
    return url

def _is_sqlite() -> bool:
    return _db_url().startswith("sqlite")

def _sqlite_path() -> str:
    url = _db_url()
    # sqlite:///relative/path  or  sqlite:////absolute/path
    path = re.sub(r"^sqlite:///", "", url)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    return path

def _get_pg_conn():
    import time, psycopg
    url = _db_url()
    m = re.search(r"@(ep-[^.]+)\.", url)
    if m and "options=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}options=endpoint%3D{m.group(1)}"
    last_exc = None
    for attempt in range(3):
        try:
            return psycopg.connect(url, connect_timeout=30)
        except psycopg.OperationalError as e:
            last_exc = e
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
    raise last_exc


# ──────────────────────────────────────────────
# SCHEMA INIT
# ──────────────────────────────────────────────
def init_db():
    ddl_ohlcv = """
        CREATE TABLE IF NOT EXISTS ohlcv (
            symbol  TEXT    NOT NULL,
            date    TEXT    NOT NULL,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL,
            volume  INTEGER,
            PRIMARY KEY (symbol, date)
        )
    """
    ddl_idx = "CREATE INDEX IF NOT EXISTS ohlcv_date_idx ON ohlcv (date)"
    ddl_cache = """
        CREATE TABLE IF NOT EXISTS stage2_cache (
            cache_date  TEXT PRIMARY KEY,
            results     TEXT NOT NULL,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """
    if _is_sqlite():
        with sqlite3.connect(_sqlite_path()) as conn:
            conn.execute(ddl_ohlcv)
            conn.execute(ddl_idx)
            conn.execute(ddl_cache)
            conn.commit()
    else:
        # PostgreSQL uses JSONB and TIMESTAMP — adjust DDL
        with _get_pg_conn() as conn:
            conn.execute(ddl_ohlcv.replace("REAL", "FLOAT").replace("INTEGER", "BIGINT"))
            conn.execute(ddl_idx)
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
    if not records:
        return
    rows = [
        (r["symbol"], str(r["date"]), r["open"], r["high"], r["low"], r["close"], r["volume"])
        for r in records
    ]
    sql = """
        INSERT INTO ohlcv (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open=excluded.open, high=excluded.high, low=excluded.low,
            close=excluded.close, volume=excluded.volume
    """
    if _is_sqlite():
        with sqlite3.connect(_sqlite_path()) as conn:
            conn.executemany(sql, rows)
            conn.commit()
    else:
        import psycopg
        pg_sql = sql.replace("?", "%s")
        with _get_pg_conn() as conn:
            conn.executemany(pg_sql, rows)
            conn.commit()


# ──────────────────────────────────────────────
# OHLCV — READ
# ──────────────────────────────────────────────
def get_latest_ohlcv_date() -> tuple[str | None, str | None]:
    """
    Returns (global_max, conservative_min) where:
      global_max      = MAX(date) across all rows  → used to check if DB was synced recently
      conservative_min = MIN of per-symbol MAX dates → used as fetch-start to fill any gaps
    """
    sql_max = "SELECT MAX(date) FROM ohlcv"
    sql_min = """
        SELECT MIN(max_date) FROM (
            SELECT symbol, MAX(date) AS max_date FROM ohlcv GROUP BY symbol
        ) t
    """
    if _is_sqlite():
        with sqlite3.connect(_sqlite_path()) as conn:
            gmax = conn.execute(sql_max).fetchone()[0]
            gmin = conn.execute(sql_min).fetchone()[0]
            return gmax, gmin
    else:
        with _get_pg_conn() as conn:
            gmax = conn.execute(sql_max).fetchone()[0]
            gmin = conn.execute(sql_min).fetchone()[0]
            fmt = lambda d: d.strftime("%Y-%m-%d") if d else None
            return fmt(gmax), fmt(gmin)


def load_ohlcv_all(period_days: int = 550) -> dict[str, pd.DataFrame]:
    if _is_sqlite():
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%d")
        with sqlite3.connect(_sqlite_path()) as conn:
            df = pd.read_sql(
                "SELECT * FROM ohlcv WHERE date >= ? ORDER BY symbol, date",
                conn, params=(cutoff,)
            )
    else:
        with _get_pg_conn() as conn:
            rows = conn.execute(f"""
                SELECT symbol, date, open, high, low, close, volume
                FROM ohlcv
                WHERE date >= NOW() - INTERVAL '{period_days} days'
                ORDER BY symbol, date
            """).fetchall()
            df = pd.DataFrame(rows, columns=["symbol","date","open","high","low","close","volume"])

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


# ──────────────────────────────────────────────
# STAGE 2 CACHE — WRITE / READ
# ──────────────────────────────────────────────
def save_stage2_cache(cache_date: str, df: pd.DataFrame):
    payload = df.to_json(orient="records")
    sql_sqlite = """
        INSERT INTO stage2_cache (cache_date, results)
        VALUES (?, ?)
        ON CONFLICT (cache_date) DO UPDATE SET results=excluded.results, created_at=datetime('now')
    """
    sql_pg = """
        INSERT INTO stage2_cache (cache_date, results)
        VALUES (%s, %s)
        ON CONFLICT (cache_date) DO UPDATE SET results=EXCLUDED.results, created_at=NOW()
    """
    if _is_sqlite():
        with sqlite3.connect(_sqlite_path()) as conn:
            conn.execute(sql_sqlite, (cache_date, payload))
            conn.commit()
    else:
        with _get_pg_conn() as conn:
            conn.execute(sql_pg, (cache_date, payload))
            conn.commit()


def load_stage2_cache(cache_date: str) -> pd.DataFrame | None:
    if _is_sqlite():
        with sqlite3.connect(_sqlite_path()) as conn:
            row = conn.execute(
                "SELECT results FROM stage2_cache WHERE cache_date = ?", (cache_date,)
            ).fetchone()
    else:
        with _get_pg_conn() as conn:
            row = conn.execute(
                "SELECT results FROM stage2_cache WHERE cache_date = %s", (cache_date,)
            ).fetchone()
    if row:
        return pd.read_json(io.StringIO(row[0]), orient="records")
    return None


def load_latest_stage2_cache() -> tuple[pd.DataFrame | None, str | None]:
    if _is_sqlite():
        with sqlite3.connect(_sqlite_path()) as conn:
            row = conn.execute(
                "SELECT cache_date, results FROM stage2_cache ORDER BY cache_date DESC LIMIT 1"
            ).fetchone()
    else:
        with _get_pg_conn() as conn:
            row = conn.execute(
                "SELECT cache_date, results FROM stage2_cache ORDER BY cache_date DESC LIMIT 1"
            ).fetchone()
    if row:
        date_str = row[0] if isinstance(row[0], str) else row[0].strftime("%Y-%m-%d")
        return pd.read_json(io.StringIO(row[1]), orient="records"), date_str
    return None, None
