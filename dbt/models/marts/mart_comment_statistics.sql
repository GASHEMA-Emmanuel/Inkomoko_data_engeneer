-- Mart: comment statistics (analytics-ready, one row per post)
-- Per-post engagement metrics joined to the post's author. Materialised as a
-- MergeTree table ordered by post_id.
--
-- The joined logic lives in a `final` CTE; the outer `select * from final`
-- exposes plain, unqualified column names so ClickHouse can resolve the
-- ORDER BY (post_id) sorting key unambiguously at table-creation time.

{{ config(order_by='(post_id)') }}

with posts as (
    select * from {{ ref('stg_posts') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

comment_metrics as (
    select
        post_id,
        count(*)                       as total_comments,
        uniqExact(commenter_email)     as unique_commenter_emails,
        avg(body_length)               as avg_comment_length,
        min(body_length)               as min_comment_length,
        max(body_length)               as max_comment_length
    from comments
    group by post_id
),

final as (
    select
        p.post_id                                        as post_id,
        p.user_id                                        as user_id,
        u.username                                       as author_username,
        p.title                                          as post_title,
        coalesce(cm.total_comments, 0)                   as total_comments,
        coalesce(cm.unique_commenter_emails, 0)          as unique_commenter_emails,
        round(coalesce(cm.avg_comment_length, 0), 2)     as avg_comment_length,
        coalesce(cm.min_comment_length, 0)               as min_comment_length,
        coalesce(cm.max_comment_length, 0)               as max_comment_length,
        now()                                            as built_at
    from posts p
    left join comment_metrics cm on p.post_id = cm.post_id
    left join users u            on p.user_id = u.user_id
)

select * from final