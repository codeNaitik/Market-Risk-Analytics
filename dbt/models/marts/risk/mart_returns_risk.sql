-- Grain: ticker x trading day. Rolling risk metrics per security.
--   * volatility over 1M / 3M / 1Y windows (annualised)
--   * beta vs the benchmark over 1Y: cov(r_stock, r_mkt) / var(r_mkt)
--   * cumulative return and drawdown from the running peak
{% set td = var('trading_days') %}

with r as (
    select * from {{ ref('int_daily_returns') }}
),

bench as (
    select r.trade_date, r.daily_return as benchmark_return
    from r
    join {{ ref('company_universe') }} c on c.ticker = r.ticker and c.is_benchmark
),

joined as (
    select r.*, b.benchmark_return
    from r
    left join bench b on b.trade_date = r.trade_date
),

rolling as (
    select
        ticker,
        trade_date,
        adj_close_price,
        daily_return,
        log_return,
        benchmark_return,
        {{ annualize_vol('stddev_samp(daily_return) over (partition by ticker order by trade_date rows between 20 preceding and current row)') }}  as vol_1m_ann,
        {{ annualize_vol('stddev_samp(daily_return) over (partition by ticker order by trade_date rows between 62 preceding and current row)') }}  as vol_3m_ann,
        {{ annualize_vol('stddev_samp(daily_return) over w1y') }}                                                                                       as vol_1y_ann,
        exp(sum(log_return) over w1y) - 1                                                    as return_1y,
        covar_samp(daily_return, benchmark_return) over w1y
            / nullif(var_samp(benchmark_return) over w1y, 0)                                 as beta_1y,
        count(*) over w1y                                                                    as obs_in_1y_window,
        exp(sum(log_return) over (partition by ticker order by trade_date
                                  rows between unbounded preceding and current row)) - 1    as cumulative_return
    from joined
    window w1y as (partition by ticker order by trade_date rows between {{ td - 1 }} preceding and current row)
),

drawdown as (
    select
        *,
        max(1 + cumulative_return) over (partition by ticker order by trade_date
                                         rows between unbounded preceding and current row) as running_peak
    from rolling
)

select
    ticker,
    trade_date,
    adj_close_price,
    daily_return,
    benchmark_return,
    vol_1m_ann,
    vol_3m_ann,
    case when obs_in_1y_window >= {{ td }} then vol_1y_ann end                as vol_1y_ann,
    case when obs_in_1y_window >= {{ td }} then return_1y end                 as return_1y,
    case when obs_in_1y_window >= {{ td }} then beta_1y end                   as beta_1y,
    case when obs_in_1y_window >= {{ td }}
         then (return_1y - {{ var('risk_free_rate') }}) / nullif(vol_1y_ann, 0) end as sharpe_1y,
    cumulative_return,
    (1 + cumulative_return) / running_peak - 1                                as drawdown
from drawdown
