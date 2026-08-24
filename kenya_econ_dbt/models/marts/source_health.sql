with observations as (
    select
        source,
        max(period_end) as latest_observation_date,
        max(ingested_at) as last_data_ingested_at
    from {{ ref('indicator_history') }}
    group by source
),

checks as (
    select
        source,
        status as last_check_status,
        checked_at as last_checked_at,
        error_message as last_error
    from {{ source('metadata', 'source_runs') }}
    qualify row_number() over (partition by source order by checked_at desc) = 1
),

joined as (
    select
        coalesce(o.source, c.source) as source,
        o.latest_observation_date,
        o.last_data_ingested_at,
        c.last_check_status,
        c.last_checked_at,
        c.last_error,
        date_diff(current_date(), o.latest_observation_date, day) as age_days,
        case coalesce(o.source, c.source)
            when 'CBK' then 4
            when 'KNBS' then 45
            when 'WORLD_BANK' then 400
            else 30
        end as expected_max_age_days
    from observations o
    full outer join checks c using (source)
)

select
    *,
    case
        when last_check_status = 'failed' then 'DEGRADED'
        when latest_observation_date is null then 'STALE'
        when age_days > expected_max_age_days then 'STALE'
        else 'CURRENT'
    end as freshness_status
from joined
