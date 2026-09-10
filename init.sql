-- Runs once, automatically, on first boot of the postgres container
-- (mounted at /docker-entrypoint-initdb.d/).
--
-- The Dagster instance uses the default `postgres` database (chosen via
-- POSTGRES_DB in docker-compose.yml). This script creates the separate
-- `stockdata` database for pipeline data, plus the table the pipeline
-- upserts into. db.py re-runs the CREATE TABLE IF NOT EXISTS as a safety
-- net, so a dropped table self-heals on the next pipeline run.

CREATE DATABASE stockdata;

\connect stockdata

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

-- Helpful index for the common "latest quotes" dashboard-style query.
CREATE INDEX IF NOT EXISTS idx_stock_quotes_fetched_at
    ON stock_quotes (fetched_at DESC);
