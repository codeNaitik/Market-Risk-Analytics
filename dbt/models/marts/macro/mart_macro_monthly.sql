-- Grain: month. Month-end macro levels, their monthly change, and sector returns side by side.
with month_end as (
    select *
    from (
        select m.*,
               cast(date_trunc('month', calendar_date) as date) as month_start_date,
               row_number() over (partition by date_trunc('month', calendar_date) order by calendar_date desc) as rn
        from {{ ref('fct_macro_daily') }} m
    ) x
    where rn = 1
),

changes as (
    select
        month_start_date,
        boc_policy_rate,
        fed_funds_rate,
        us_10y_yield,
        cad_10y_yield,
        usd_cad,
        vix,
        us_2s10s_spread,
        boc_policy_rate - lag(boc_policy_rate) over (order by month_start_date)  as boc_rate_change,
        fed_funds_rate  - lag(fed_funds_rate)  over (order by month_start_date)  as fed_rate_change,
        us_10y_yield    - lag(us_10y_yield)    over (order by month_start_date)  as us_10y_change,
        cad_10y_yield   - lag(cad_10y_yield)   over (order by month_start_date)  as cad_10y_change,
        usd_cad / lag(usd_cad) over (order by month_start_date) - 1              as usd_cad_pct_change
    from month_end
)

select
    s.sector,
    s.month_start_date,
    s.avg_monthly_return       as sector_return,
    c.boc_policy_rate,
    c.fed_funds_rate,
    c.us_10y_yield,
    c.cad_10y_yield,
    c.usd_cad,
    c.vix,
    c.us_2s10s_spread,
    c.boc_rate_change,
    c.fed_rate_change,
    c.us_10y_change,
    c.cad_10y_change,
    c.usd_cad_pct_change
from {{ ref('mart_sector_performance') }} s
join changes c on c.month_start_date = s.month_start_date
