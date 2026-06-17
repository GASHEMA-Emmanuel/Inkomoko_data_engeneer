-- =============================================================================
-- Validation queries: prove data moved through every stage.
-- Run the Postgres block in DBeaver against the "inkomoko" DB, and the
-- ClickHouse blocks against the ClickHouse connection.
-- Expected counts assume the static JSONPlaceholder dataset:
--   users = 10, posts = 100, comments = 500
-- =============================================================================

-- ----------------------------------------------------------------------------
-- 1. PostgreSQL (OLTP) row counts        -- connect to DB: inkomoko
-- ----------------------------------------------------------------------------
SELECT 'users'    AS table_name, count(*) AS rows FROM raw.users
UNION ALL
SELECT 'posts'    AS table_name, count(*) FROM raw.posts
UNION ALL
SELECT 'comments' AS table_name, count(*) FROM raw.comments;
-- Expected:
--   users     | 10
--   posts     | 100
--   comments  | 500

-- Watermark state (shows incremental replication progress)
SELECT * FROM raw.sync_watermark ORDER BY table_name;


-- ----------------------------------------------------------------------------
-- 2. ClickHouse (OLAP) raw landing counts   -- run on ClickHouse
--    FINAL collapses ReplacingMergeTree duplicates for an exact logical count.
-- ----------------------------------------------------------------------------
SELECT 'users'    AS table_name, count() AS rows FROM raw.users    FINAL
UNION ALL
SELECT 'posts'    AS table_name, count() FROM raw.posts    FINAL
UNION ALL
SELECT 'comments' AS table_name, count() FROM raw.comments FINAL;
-- Expected: matches Postgres (10 / 100 / 500)


-- ----------------------------------------------------------------------------
-- 3. dbt staging models   -- run on ClickHouse, database: analytics
-- ----------------------------------------------------------------------------
SELECT 'stg_users'    AS model, count() AS rows FROM analytics.stg_users
UNION ALL
SELECT 'stg_posts'    AS model, count() FROM analytics.stg_posts
UNION ALL
SELECT 'stg_comments' AS model, count() FROM analytics.stg_comments;
-- Expected: 10 / 100 / 500


-- ----------------------------------------------------------------------------
-- 4. dbt marts (analytics-ready)   -- run on ClickHouse, database: analytics
-- ----------------------------------------------------------------------------
SELECT count() AS user_rows FROM analytics.mart_user_post_summary;     -- expect 10
SELECT count() AS post_rows FROM analytics.mart_comment_statistics;    -- expect 100

-- Sample analytics output: top users by comments received
SELECT user_id, username, total_posts, total_comments_received, unique_commenters
FROM analytics.mart_user_post_summary
ORDER BY total_comments_received DESC
LIMIT 5;

-- Sample analytics output: most-commented posts
SELECT post_id, author_username, total_comments, avg_comment_length
FROM analytics.mart_comment_statistics
ORDER BY total_comments DESC
LIMIT 5;
