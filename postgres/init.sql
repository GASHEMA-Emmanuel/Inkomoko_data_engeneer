-- =============================================================================
-- PostgreSQL initialisation script (OLTP layer)
-- Runs automatically on first container start via /docker-entrypoint-initdb.d
-- =============================================================================
-- This file:
--   1. Creates a dedicated "airflow" metadata database (so Airflow and the
--      application data live in the same Postgres server but separate DBs).
--   2. Creates the "raw" schema that holds landing tables for ingested data.
--   3. Creates users / posts / comments tables matching the JSONPlaceholder API.
--   4. Creates a sync_watermark table used by the replication layer for
--      incremental loads into ClickHouse.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Airflow metadata database (created in the default POSTGRES_DB connection)
-- ---------------------------------------------------------------------------
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- ---------------------------------------------------------------------------
-- 2. Application schema
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------------
-- 3. Source tables (flattened from the JSONPlaceholder API)
-- ---------------------------------------------------------------------------

-- /users
CREATE TABLE IF NOT EXISTS raw.users (
    id            INTEGER       PRIMARY KEY,
    name          TEXT,
    username      TEXT,
    email         TEXT,
    phone         TEXT,
    website       TEXT,
    company_name  TEXT,
    city          TEXT,
    zipcode       TEXT,
    lat           NUMERIC(9,6),
    lng           NUMERIC(9,6),
    ingested_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- /posts
CREATE TABLE IF NOT EXISTS raw.posts (
    id            INTEGER       PRIMARY KEY,
    user_id       INTEGER,
    title         TEXT,
    body          TEXT,
    ingested_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- /comments
CREATE TABLE IF NOT EXISTS raw.comments (
    id            INTEGER       PRIMARY KEY,
    post_id       INTEGER,
    name          TEXT,
    email         TEXT,
    body          TEXT,
    ingested_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Helpful indexes for the replication layer's incremental reads
CREATE INDEX IF NOT EXISTS idx_users_ingested_at    ON raw.users (ingested_at);
CREATE INDEX IF NOT EXISTS idx_posts_ingested_at    ON raw.posts (ingested_at);
CREATE INDEX IF NOT EXISTS idx_comments_ingested_at ON raw.comments (ingested_at);

-- ---------------------------------------------------------------------------
-- 4. Replication watermark table (drives incremental loads into ClickHouse)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.sync_watermark (
    table_name        TEXT PRIMARY KEY,
    last_ingested_at  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO raw.sync_watermark (table_name) VALUES
    ('users'), ('posts'), ('comments')
ON CONFLICT (table_name) DO NOTHING;
