{{ config(enabled = var('enable_streaming')) }}

select
    product_id,
    bar_start,
    bar_end,
    cast(open as double)   as open_price,
    cast(high as double)   as high_price,
    cast(low as double)    as low_price,
    cast(close as double)  as close_price,
    cast(volume as double) as volume,
    cast(vwap as double)   as vwap,
    trade_count
from {{ source('silver', 'crypto_bars_1m') }}
