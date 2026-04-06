with source as (
    select *
    from {{ source('staging', 'stg_fx_rates') }}
),

enriched as (
    select
        base_currency,
        target_currency,
        rate                                     as kes_per_usd,
        usd_per_kes,
        round(usd_per_kes * 100, 6)              as usd_per_100_kes,
        rate_timestamp,
        date_trunc('day', rate_timestamp)::date  as rate_date,
        loaded_at
    from source
    where target_currency = 'KES'
)

select * from enriched