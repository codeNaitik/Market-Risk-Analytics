-- Grain: one row per ticker per trading day.
select
    {{ dbt.concat(["p.ticker", "'|'", "cast(p.trade_date as string)"]) }}  as price_id,
    cast(extract(year from p.trade_date) * 10000 + extract(month from p.trade_date) * 100
         + extract(day from p.trade_date) as int)                          as date_key,
    p.ticker,
    p.trade_date,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.adj_close_price,
    p.volume,
    p.close_price * p.volume                                               as traded_value,
    r.daily_return,
    r.log_return,
    p.is_corporate_action,
    p.data_source
from {{ ref('stg_daily_prices') }} p
left join {{ ref('int_daily_returns') }} r
    on r.ticker = p.ticker and r.trade_date = p.trade_date
