-- Daily simple and log returns on split/dividend-adjusted prices.
-- Log returns are additive over time, which makes rolling and cumulative maths easy.
with prices as (
    select
        ticker,
        trade_date,
        adj_close_price,
        lag(adj_close_price) over (partition by ticker order by trade_date) as prev_adj_close,
        lag(trade_date)      over (partition by ticker order by trade_date) as prev_trade_date
    from {{ ref('stg_daily_prices') }}
)

select
    ticker,
    trade_date,
    prev_trade_date,
    adj_close_price,
    adj_close_price / prev_adj_close - 1   as daily_return,
    ln(adj_close_price / prev_adj_close)   as log_return
from prices
where prev_adj_close is not null
