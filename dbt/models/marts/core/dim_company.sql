with financial_names as (
    select ticker, max(entity_name) as entity_name, max(cik) as cik
    from {{ ref('stg_financials') }}
    group by ticker
),

coverage as (
    select ticker, min(trade_date) as first_trade_date, max(trade_date) as last_trade_date,
           count(*) as trading_days_loaded
    from {{ ref('stg_daily_prices') }}
    group by ticker
)

select
    u.ticker,
    coalesce(f.entity_name, u.ticker)     as company_name,
    f.cik,
    u.sector,
    u.exchange,
    u.country,
    u.currency,
    u.is_benchmark,
    w.weight is not null                  as is_portfolio_holding,
    coalesce(w.weight, 0)                 as portfolio_weight,
    c.first_trade_date,
    c.last_trade_date,
    c.trading_days_loaded
from {{ ref('company_universe') }} u
left join financial_names f on f.ticker = u.ticker
left join coverage c on c.ticker = u.ticker
left join {{ ref('portfolio_weights') }} w on w.ticker = u.ticker
