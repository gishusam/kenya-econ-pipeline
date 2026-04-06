with source as (
    select *
    from {{ source('staging', 'stg_worldbank_inflation') }}
    where is_current = true
),

renamed as (
    select
        country_code,
        country_name,
        year,
        inflation_pct,
        loaded_at
    from source
    where inflation_pct is not null
)

select * from renamed