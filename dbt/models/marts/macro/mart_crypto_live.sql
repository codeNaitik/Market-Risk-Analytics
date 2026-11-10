{{ config(enabled = var('enable_streaming')) }}

-- Latest 24h of 1-minute bars with rolling 15-minute stats for the live dashboard page.
select
    product_id,
    bar_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    trade_count,
    close_price / lag(close_price) over (partition by product_id order by bar_start) - 1  as minute_return,
    avg(close_price) over (partition by product_id order by bar_start
                           rows between 14 preceding and current row)                      as sma_15m,
    sum(volume) over (partition by product_id order by bar_start
                      rows between 14 preceding and current row)                           as volume_15m
from {{ ref('stg_crypto_bars_1m') }}
where bar_start >= (select max(bar_start) from {{ ref('stg_crypto_bars_1m') }}) - interval 1 day
