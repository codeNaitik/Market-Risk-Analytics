-- Grain: sector x month. Equal-weighted average of member stocks' monthly returns.
with monthly as (
    select
        r.ticker,
        cast(date_trunc('month', r.trade_date) as date)   as month_start_date,
        exp(sum(r.log_return)) - 1                        as monthly_return,
        count(*)                                          as trading_days
    from {{ ref('int_daily_returns') }} r
    group by r.ticker, cast(date_trunc('month', r.trade_date) as date)
)

select
    c.sector,
    m.month_start_date,
    avg(m.monthly_return)        as avg_monthly_return,
    min(m.monthly_return)        as worst_member_return,
    max(m.monthly_return)        as best_member_return,
    count(distinct m.ticker)     as n_members
from monthly m
join {{ ref('company_universe') }} c on c.ticker = m.ticker
where not c.is_benchmark
group by c.sector, m.month_start_date
