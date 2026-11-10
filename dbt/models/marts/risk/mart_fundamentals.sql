-- Screener table: latest trailing-twelve-month fundamentals + valuation per company.
-- Always uses each company's MOST RECENT quarter and flags stale/incomplete data.
with fin as (
    select
        *,
        lag(revenue_ttm, 4) over (partition by ticker order by period_end)   as revenue_ttm_prior_year,
        row_number() over (partition by ticker order by period_end desc)     as recency
    from {{ ref('fct_financials_quarterly') }}
),

latest_fin as (
    select * from fin where recency = 1
),

latest_price as (
    select ticker, close_price, trade_date
    from (
        select ticker, close_price, trade_date,
               row_number() over (partition by ticker order by trade_date desc) as rn
        from {{ ref('stg_daily_prices') }}
    ) x
    where rn = 1
)

select
    c.ticker,
    c.company_name,
    c.sector,
    f.period_end                                               as latest_quarter_end,
    f.revenue_ttm,
    f.net_income_ttm,
    f.eps_ttm,
    f.net_income_ttm / nullif(f.revenue_ttm, 0)                as net_margin_ttm,
    f.revenue_ttm / nullif(f.revenue_ttm_prior_year, 0) - 1    as revenue_growth_yoy,
    f.debt_to_equity,
    f.total_assets,
    p.close_price                                              as latest_close,
    p.trade_date                                               as price_date,
    case when f.eps_ttm > 0 then p.close_price / f.eps_ttm end as pe_ratio_ttm,
    f.has_restatement,
    p.trade_date - f.period_end > 200                          as is_stale,
    case
        when p.trade_date - f.period_end > 200 then 'stale: no filing in 200+ days'
        when f.revenue_ttm is null             then 'revenue not reported under mapped XBRL tags'
        when f.net_income_ttm is null          then 'net income missing for a recent quarter'
        else 'ok'
    end                                                        as data_quality_note
from latest_fin f
join {{ ref('dim_company') }} c on c.ticker = f.ticker
left join latest_price p on p.ticker = f.ticker
