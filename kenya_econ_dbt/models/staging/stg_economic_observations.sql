with source as (
    select
        source,
        indicator_code,
        indicator_name,
        geography,
        period_start,
        period_end,
        frequency,
        value,
        unit,
        currency,
        source_published_at,
        source_url,
        source_record_hash,
        raw_payload,
        ingested_at,
        run_id
    from {{ source('raw', 'economic_observations') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by source, indicator_code, geography, period_start, period_end
            order by coalesce(source_published_at, ingested_at) desc, ingested_at desc
        ) as revision_rank
    from source
)

select
    *,
    revision_rank = 1 as is_latest_revision
from ranked
