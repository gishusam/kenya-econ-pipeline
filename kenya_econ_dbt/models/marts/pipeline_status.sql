{{ config(
    materialized="view",
    grant_access_to=[
        {
            'project': env_var(
                'GCP_PROJECT_ID',
                'kenya-econ-dev'
            ),
            'dataset': 'metadata'
        }
    ]
) }}

select
    run_id,
    started_at,
    completed_at,
    status,
    sources_succeeded,
    sources_failed,
    rows_inserted,
    dbt_status,
    git_sha,
    error_message
from {{ source('metadata', 'pipeline_runs') }}
where completed_at is not null
qualify row_number() over (order by completed_at desc) = 1
