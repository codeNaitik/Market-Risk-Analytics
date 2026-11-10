-- One row per ticker per trading day, straight from silver with clean names.
select
    ticker,
    cast(trade_date as date)          as trade_date,
    open                              as open_price,
    high                              as high_price,
    low                               as low_price,
    close                             as close_price,
    adj_close                         as adj_close_price,
    volume,
    is_corporate_action,
    source                            as data_source,
    _processed_at
from {{ source('silver', 'daily_prices') }}
