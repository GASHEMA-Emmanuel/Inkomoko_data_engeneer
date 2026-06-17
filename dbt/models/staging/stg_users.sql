-- Staging: users
-- * De-duplicates the raw landing table by keeping the most recently ingested
--   row per id (ReplacingMergeTree can hold un-merged duplicates; "LIMIT 1 BY id"
--   after ordering by ingested_at DESC gives us a deterministic single row).
-- * Standardises types and trims/normalises text.

with deduped as (

    select
        id,
        name,
        username,
        lower(trim(email))            as email,
        phone,
        website,
        company_name,
        city,
        zipcode,
        lat,
        lng,
        ingested_at
    from {{ source('raw', 'users') }}
    order by ingested_at desc
    limit 1 by id

)

select
    cast(id as UInt32)                as user_id,
    name                             as full_name,
    username,
    email,
    phone,
    website,
    company_name,
    city,
    zipcode,
    lat,
    lng,
    ingested_at
from deduped
