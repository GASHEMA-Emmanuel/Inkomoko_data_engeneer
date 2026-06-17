-- Staging: posts
-- De-duplicate by latest ingested_at per id, standardise text, and derive a
-- couple of cheap analytical helpers (lengths) used by the marts.

with deduped as (

    select
        id,
        user_id,
        title,
        body,
        ingested_at
    from {{ source('raw', 'posts') }}
    order by ingested_at desc
    limit 1 by id

)

select
    cast(id as UInt32)               as post_id,
    cast(user_id as UInt32)          as user_id,
    trim(title)                      as title,
    trim(body)                       as body,
    length(trim(title))              as title_length,
    length(trim(body))               as body_length,
    ingested_at
from deduped
