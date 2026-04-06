with fx as (
    select * from {{ ref('stg_fx') }}
),

daily as (
    select
        rate_date,
        base_currency,
        target_currency,
        round(min(kes_per_usd), 4)  as rate_min,
        round(max(kes_per_usd), 4)  as rate_max,
        round(avg(kes_per_usd), 4)  as rate_avg,
        round(avg(usd_per_kes), 8)  as usd_per_kes_avg,
        count(*)                     as snapshot_count
    from fx
    group by rate_date, base_currency, target_currency
)

select * from daily
order by rate_date desc