with macro as (
    select * from {{ ref('kenya_macro') }}
),

base as (
    select
        kes_per_usd
    from macro
    where inflation_pct is not null
    order by year asc
    limit 1
),

indexed as (
    select
        m.year,
        m.gdp_usd_billions,
        m.gdp_kes_billions,
        m.inflation_pct,
        m.kes_per_usd,
        m.gdp_growth_pct,

        round(
            100 / (1 + (m.inflation_pct / 100))
        , 2)                                    as inflation_index,

        round(
            (b.kes_per_usd / nullif(m.kes_per_usd, 0)) * 100
        , 2)                                    as fx_strength_index,

        round(
            (100 / (1 + (m.inflation_pct / 100)))
            * (b.kes_per_usd / nullif(m.kes_per_usd, 0))
        , 2)                                    as purchasing_power_index

    from macro m
    cross join base b
    where m.inflation_pct is not null
)

select * from indexed
order by year desc