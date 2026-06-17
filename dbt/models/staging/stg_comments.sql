-- Staging: comments
-- De-duplicate by latest ingested_at per id, normalise email, derive body length.

with deduped as (

    select
        id,
        post_id,
        name,
        email,
        body,
        ingested_at
    from {{ source('raw', 'comments') }}
    order by ingested_at desc
    limit 1 by id

)

select
    cast(id as UInt32)               as comment_id,
    cast(post_id as UInt32)          as post_id,
    name                             as commenter_name,
    lower(trim(email))               as commenter_email,
    trim(body)                       as body,
    length(trim(body))               as body_length,
    ingested_at
from deduped
