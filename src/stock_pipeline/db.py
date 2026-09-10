"""PostgreSQL access layer for stock quotes.

The pipeline only upserts into stock_quotes: one row per (symbol,
trade_date), refreshed by each hourly run. The batch runs as a single
transaction, so a mid-batch failure leaves the table untouched and re-runs
never duplicate rows. Connection details come from environment variables.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import psycopg
from psycopg.rows import dict_row

from .config import PipelineConfig
from .validation import Quote

logger = logging.getLogger(__name__)

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS stock_quotes (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol         TEXT        NOT NULL,
    trade_date     DATE        NOT NULL,
    current        NUMERIC(12,4),
    change         NUMERIC(12,4),
    percent_change NUMERIC(8,4),
    high           NUMERIC(12,4),
    low            NUMERIC(12,4),
    open           NUMERIC(12,4),
    prev_close     NUMERIC(12,4),
    source         TEXT        NOT NULL DEFAULT 'finnhub',
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (symbol, trade_date)
);
"""

# One INSERT per quote, batched via executemany in one transaction. NULLs in
# the value columns are intentional: partial quotes keep what they have
# (see validation.py).
_UPSERT_SQL = """
INSERT INTO stock_quotes
    (symbol, trade_date, current, change, percent_change,
     high, low, open, prev_close, fetched_at)
VALUES (%(symbol)s, %(trade_date)s, %(current)s, %(change)s,
        %(percent_change)s, %(high)s, %(low)s, %(open)s,
        %(prev_close)s, %(fetched_at)s)
ON CONFLICT (symbol, trade_date) DO UPDATE SET
    current        = EXCLUDED.current,
    change         = EXCLUDED.change,
    percent_change = EXCLUDED.percent_change,
    high           = EXCLUDED.high,
    low            = EXCLUDED.low,
    open           = EXCLUDED.open,
    prev_close     = EXCLUDED.prev_close,
    fetched_at     = EXCLUDED.fetched_at
"""


def connect(config: PipelineConfig) -> psycopg.Connection:
    """Open a connection to the stock database using env-var credentials."""
    return psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password,
        connect_timeout=10,
        row_factory=dict_row,
    )


def ensure_table(conn: psycopg.Connection) -> None:
    """Create stock_quotes on a fresh database; init.sql normally handles
    this at container boot."""
    with conn.transaction():
        conn.execute(_TABLE_DDL)
    conn.commit()


def upsert_quotes(
    conn: psycopg.Connection, quotes: list[Quote]
) -> tuple[int, int]:
    """Upsert quotes in one transaction; returns (inserted, updated).

    Existing (symbol, trade_date) rows are refreshed, new ones inserted; a
    mid-batch failure writes nothing. OperationalError (connection lost)
    and DatabaseError bubble up for the op-level retry to handle.
    """
    if not quotes:
        return 0, 0

    now = datetime.now(UTC)
    today = now.date()
    symbols = [q.symbol for q in quotes]
    rows = [
        {
            "symbol": q.symbol,
            "trade_date": today,
            "current": q.current,
            "change": q.change,
            "percent_change": q.percent_change,
            "high": q.high,
            "low": q.low,
            "open": q.open,
            "prev_close": q.prev_close,
            "fetched_at": now,
        }
        for q in quotes
    ]

    # Single transaction: either every quote lands or none does.
    with conn.transaction():
        # Which of today's rows already exist (for the insert/refresh split
        # in the return value; the upsert below is correct either way)
        existing = {
            row["symbol"]
            for row in conn.execute(
                "SELECT symbol FROM stock_quotes "
                "WHERE trade_date = %s AND symbol = ANY(%s)",
                (today, symbols),
            ).fetchall()
        }
        inserted = sum(1 for s in symbols if s not in existing)
        updated = len(symbols) - inserted

        # psycopg 3 puts executemany on the cursor, not the connection.
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, rows)

    logger.info(
        "Upserted %d quote(s) for %s: %d new, %d refreshed",
        len(quotes),
        today,
        inserted,
        updated,
    )
    return inserted, updated
