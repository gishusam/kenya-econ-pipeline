select
    source,
    indicator_code,
    indicator_name,
    geography,
    period_end as observation_date,
    value,
    unit,
    currency,
    source_url,
    ingested_at,
    run_id
from {{ ref('latest_indicators') }}
