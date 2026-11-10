-- Q: Which companies are growing revenue fastest year over year, and which growth
--    quartile does each fall into within the universe?
-- Skills: LAG over quarters, NTILE, NULLIF to avoid divide-by-zero
with growth as (
    select
        ticker,
        period_end,
        revenue,
        lag(revenue, 4) over (partition by ticker order by period_end)    as revenue_same_q_last_year
    from {{ ref('fct_financials_quarterly') }}
),

latest as (
    select *, row_number() over (partition by ticker order by period_end desc) as rn
    from growth
    where revenue_same_q_last_year is not null
)

select
    l.ticker,
    c.sector,
    l.period_end                                                                   as quarter_end,
    round(l.revenue / 1e9, 2)                                                       as revenue_bn,
    round(100 * (l.revenue / nullif(l.revenue_same_q_last_year, 0) - 1), 1)        as yoy_growth_pct,
    ntile(4) over (order by l.revenue / nullif(l.revenue_same_q_last_year, 0) desc) as growth_quartile   -- 1 = fastest
from latest l
join {{ ref('dim_company') }} c on c.ticker = l.ticker
where l.rn = 1
order by yoy_growth_pct desc
