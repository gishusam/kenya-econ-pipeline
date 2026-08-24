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
    ingested_at,
    run_id
from {{ ref('stg_economic_observations') }}
where is_latest_revision
