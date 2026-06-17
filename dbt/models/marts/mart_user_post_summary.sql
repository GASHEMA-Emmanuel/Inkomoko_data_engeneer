-- Mart: user post summary (analytics-ready, one row per user)
-- Combines users + their posts + comments received on those posts into a single
-- denormalised, ML/BI-friendly table. Materialised as a MergeTree table ordered
-- by user_id for fast lookups and range scans in ClickHouse.
--
-- The joined logic lives in a `final` CTE; the outer `select * from final`
-- exposes plain, unqualified column names so ClickHouse can resolve the
-- ORDER BY (user_id) sorting key unambiguously at table-creation time.

{{ config(order_by='(user_id)') }}

with users as (
    select * from {{ ref('stg_users') }}
),

posts as (
    select * from {{ ref('stg_posts') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

post_metrics as (
    select
        user_id,
        count(*)                         as total_posts,
        avg(title_length)                as avg_post_title_length,
        avg(body_length)                 as avg_post_body_length,
        sum(body_length)                 as total_post_body_length
    from posts
    group by user_id
),

-- comments received on each user's posts (join comments -> posts -> author)
comments_received as (
    select
        p.user_id                        as user_id,
        count(*)                         as total_comments_received,
        uniqExact(c.commenter_email)     as unique_commenters
    from comments c
    inner join posts p on c.post_id = p.post_id
    group by p.user_id
),

final as (
    select
        u.user_id                                    as user_id,
        u.username                                   as username,
        u.full_name                                  as full_name,
        u.email                                      as email,
        u.company_name                               as company_name,
        u.city                                       as city,
        coalesce(pm.total_posts, 0)                  as total_posts,
        coalesce(cr.total_comments_received, 0)      as total_comments_received,
        coalesce(cr.unique_commenters, 0)            as unique_commenters,
        round(coalesce(pm.avg_post_title_length, 0), 2) as avg_post_title_length,
        round(coalesce(pm.avg_post_body_length, 0), 2)  as avg_post_body_length,
        coalesce(pm.total_post_body_length, 0)       as total_post_body_length,
        now()                                        as built_at
    from users u
    left join post_metrics pm      on u.user_id = pm.user_id
    left join comments_received cr on u.user_id = cr.user_id
)

select * from final