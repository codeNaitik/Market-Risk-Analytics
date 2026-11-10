-- Grain: one row per calendar day, every macro series as a column (forward-filled).
with wide as (
    select
        calendar_date,
        max(case when series_id = 'FEDFUNDS'           then value end) as fed_funds_rate,
        max(case when series_id = 'DGS2'               then value end) as us_2y_yield,
        max(case when series_id = 'DGS10'              then value end) as us_10y_yield,
        max(case when series_id = 'CPIAUCSL'           then value end) as us_cpi_index,
        max(case when series_id = 'UNRATE'             then value end) as us_unemployment_rate,
        max(case when series_id = 'VIXCLS'             then value end) as vix,
        max(case when series_id = 'V39079'             then value end) as boc_policy_rate,
        max(case when series_id = 'FXUSDCAD'           then value end) as usd_cad,
        max(case when series_id = 'BD.CDN.10YR.DQ.YLD' then value end) as cad_10y_yield
    from {{ ref('int_macro_daily_filled') }}
    group by calendar_date
)

select
    cast(extract(year from calendar_date) * 10000 + extract(month from calendar_date) * 100
         + extract(day from calendar_date) as int)                  as date_key,
    *,
    us_10y_yield - us_2y_yield                                        as us_2s10s_spread,   -- < 0 = inverted curve
    cad_10y_yield - us_10y_yield                                      as cad_us_10y_spread,
    100 * (us_cpi_index / lag(us_cpi_index, 365) over (order by calendar_date) - 1) as us_cpi_yoy_pct
from wide
