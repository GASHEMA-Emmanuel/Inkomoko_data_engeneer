-- =============================================================================
-- ClickHouse initialisation DDL (OLAP layer)
-- Runs automatically on first container start via /docker-entrypoint-initdb.d
-- =============================================================================
-- The "raw" database is created by the CLICKHOUSE_DB env var. Here we create:
--   * the raw landing tables that the replication layer writes into
--   * the "analytics" database that dbt builds staging + mart models into
--
-- Engine choice: ReplacingMergeTree(ingested_at)
--   - MergeTree family = columnar, sorted, highly compressible storage that
--     ClickHouse is optimised for (great for analytical scans/aggregations).
--   - ReplacingMergeTree collapses rows with the same sorting key, keeping the
--     row with the largest "version" column (ingested_at). This makes our
--     incremental loads idempotent: re-loading the same id simply replaces it.
--   - ORDER BY (id) is both the sorting key and the de-duplication key.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;

-- ----- raw.users -----
CREATE TABLE IF NOT EXISTS raw.users
(
    id            UInt32,
    name          String,
    username      String,
    email         String,
    phone         String,
    website       String,
    company_name  String,
    city          String,
    zipcode       String,
    lat           Float64,
    lng           Float64,
    ingested_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (id);

-- ----- raw.posts -----
CREATE TABLE IF NOT EXISTS raw.posts
(
    id            UInt32,
    user_id       UInt32,
    title         String,
    body          String,
    ingested_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (id);

-- ----- raw.comments -----
CREATE TABLE IF NOT EXISTS raw.comments
(
    id            UInt32,
    post_id       UInt32,
    name          String,
    email         String,
    body          String,
    ingested_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (id);
