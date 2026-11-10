-- Q: Each month, which stock led its sector, and by how much did it beat the sector average?
-- Skills: CTEs, window aggregates, RANK(), QUALIFY (DuckDB + Databricks SQL both support it)
with monthly as (
    select
        f.ticker,
        c.sector,
        cast(date_trunc('month', f.trade_date) as date)  as month_start,
        exp(sum(f.log_return)) - 1                       as monthly_return
    from {{ ref('fct_daily_prices') }} f
    join {{ ref('dim_company') }} c on c.ticker = f.ticker
    where not c.is_benchmark and f.log_return is not null
    group by f.ticker, c.sector, cast(date_trunc('month', f.trade_date) as date)
)

select
    month_start,
    sector,
    ticker                                                         as sector_leader,
    round(100 * monthly_return, 2)                                 as leader_return_pct,
    round(100 * avg(monthly_return) over (partition by sector, month_start), 2)  as sector_avg_pct,
    round(100 * (monthly_return - avg(monthly_return) over (partition by sector, month_start)), 2) as outperformance_pct
from monthly
qualify rank() over (partition by sector, month_start order by monthly_return desc) = 1
   and count(*) over (partition by sector, month_start) > 1      -- skip one-stock sectors
order by month_start desc, outperformance_pct desc
