select *
from {{ ref('indicator_history') }}
qualify row_number() over (
    partition by source, indicator_code, geography
    order by period_end desc, ingested_at desc
) = 1
