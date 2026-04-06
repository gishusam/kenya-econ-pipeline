with source as (
    select *
    from {{ source('staging', 'stg_worldbank_gdp') }}
    where is_current = true
),

renamed as (
    select
        country_code,
        country_name,
        year,
        value_usd,
        round(value_usd / 1e9, 2)  as value_usd_billions,
        loaded_at
    from source
    where value_usd is not null
)

select * from renamed