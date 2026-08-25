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
        src.*,
        row_number() over (
            partition by
                src.source,
                src.indicator_code,
                src.geography,
                src.period_start,
                src.period_end
            order by
                coalesce(src.source_published_at, src.ingested_at) desc,
                src.ingested_at desc
        ) as revision_rank
    from source as src
)

select
    *,
    revision_rank = 1 as is_latest_revision
from ranked
