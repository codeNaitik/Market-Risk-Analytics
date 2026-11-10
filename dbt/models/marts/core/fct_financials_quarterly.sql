-- Grain: one row per ticker per fiscal quarter (Q4 derived from the 10-K).
-- Flow metrics are pivoted wide; balance-sheet metrics are the instant at quarter end.
with flows as (
    select
        ticker,
        period_end,
        max(period_start)                                                    as period_start,
        max(case when metric = 'revenue'             then value end)         as revenue,
        max(case when metric = 'net_income'          then value end)         as net_income,
        max(case when metric = 'operating_cash_flow' then value end)         as operating_cash_flow,
        max(case when metric = 'eps_diluted'         then value end)         as eps_diluted,
        max(case when is_restated then 1 else 0 end) = 1                     as has_restatement,
        max(case when quarter_source = 'derived_from_10k' then 1 else 0 end) = 1 as is_derived_q4
    from {{ ref('int_quarterly_financials') }}
    group by ticker, period_end
),

balance as (
    select
        ticker,
        period_end,
        max(case when metric = 'total_assets'        then value end) as total_assets,
        max(case when metric = 'total_liabilities'   then value end) as total_liabilities,
        max(case when metric = 'stockholders_equity' then value end) as stockholders_equity,
        max(case when metric = 'long_term_debt'      then value end) as long_term_debt
    from {{ ref('stg_financials') }}
    where period_type = 'instant'
    group by ticker, period_end
),

joined as (
    select f.*, b.total_assets, b.total_liabilities, b.stockholders_equity, b.long_term_debt
    from flows f
    left join balance b on b.ticker = f.ticker and b.period_end = f.period_end
)

select
    *,
    cast(extract(year from period_end) as int)                as calendar_year,
    cast(extract(quarter from period_end) as int)             as calendar_quarter,
    net_income / nullif(revenue, 0)                           as net_margin,
    long_term_debt / nullif(stockholders_equity, 0)           as debt_to_equity,
    -- trailing twelve months (needs 4 consecutive quarters)
    case when count(revenue) over w4 = 4 then sum(revenue) over w4 end               as revenue_ttm,
    case when count(net_income) over w4 = 4 then sum(net_income) over w4 end         as net_income_ttm,
    case when count(eps_diluted) over w4 = 4 then sum(eps_diluted) over w4 end       as eps_ttm
from joined
window w4 as (partition by ticker order by period_end rows between 3 preceding and current row)
