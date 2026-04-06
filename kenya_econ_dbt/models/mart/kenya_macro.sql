with gdp as (
    select * from {{ ref('stg_gdp') }}
),

inflation as (
    select * from {{ ref('stg_inflation') }}
),

fx_latest as (
    select
        kes_per_usd,
        usd_per_kes
    from {{ ref('stg_fx') }}
    order by rate_timestamp desc
    limit 1
),

joined as (
    select
        g.year,
        g.country_code,
        g.country_name,
        g.value_usd_billions                        as gdp_usd_billions,

        round(
            g.value_usd_billions * fx.kes_per_usd, 2
        )                                           as gdp_kes_billions,

        i.inflation_pct,
        fx.kes_per_usd,
        fx.usd_per_kes,

        round(
            (g.value_usd_billions
                - lag(g.value_usd_billions)
                    over (order by g.year))
            / nullif(
                lag(g.value_usd_billions)
                    over (order by g.year), 0
            ) * 100,
        2)                                          as gdp_growth_pct

    from gdp g
    left join inflation i
        on g.year = i.year
        and g.country_code = i.country_code
    cross join fx_latest fx
)

select * from joined
order by year desc