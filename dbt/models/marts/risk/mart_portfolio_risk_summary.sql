-- One row per look-back window with headline risk metrics for the dashboard cards.
--   Historical VaR (1-day): the loss not exceeded on 95% / 99% of days in the window.
--   Expected shortfall (CVaR 95): average loss on the days beyond VaR 95.
{% set td = var('trading_days') %}

with daily as (
    select *, row_number() over (order by trade_date desc) as days_back
    from {{ ref('mart_portfolio_daily') }}
),

windows as (
    select 'full_period' as window_name, d.* from daily d
    union all
    select 'last_1y', d.* from daily d where days_back <= {{ td }}
    union all
    select 'last_3m', d.* from daily d where days_back <= 63
),

-- drawdown measured from the peak *inside* each window, not the all-time peak
windows_dd as (
    select
        *,
        portfolio_value_cad / max(portfolio_value_cad) over (
            partition by window_name order by trade_date
            rows between unbounded preceding and current row) - 1 as window_drawdown
    from windows
),

var_levels as (
    select
        window_name,
        percentile_cont(0.05) within group (order by portfolio_return) as var95_return,
        percentile_cont(0.01) within group (order by portfolio_return) as var99_return
    from windows
    group by window_name
),

stats as (
    select
        w.window_name,
        min(w.trade_date)                                                      as window_start,
        max(w.trade_date)                                                      as window_end,
        count(*)                                                               as trading_days,
        exp(sum(ln(1 + w.portfolio_return)) * {{ td }} / count(*)) - 1         as annualized_return,
        {{ annualize_vol('stddev_samp(w.portfolio_return)') }}                 as annualized_vol,
        min(w.window_drawdown)                                                 as max_drawdown,
        min(w.portfolio_return)                                                as worst_day_return,
        max(w.portfolio_return)                                                as best_day_return,
        covar_samp(w.portfolio_return, w.benchmark_return)
            / nullif(var_samp(w.benchmark_return), 0)                          as beta,
        avg(case when w.portfolio_return <= v.var95_return then w.portfolio_return end) as cvar95_return,
        max(v.var95_return)                                                    as var95_return,
        max(v.var99_return)                                                    as var99_return
    from windows_dd w
    join var_levels v on v.window_name = w.window_name
    group by w.window_name
),

latest as (
    select portfolio_value_cad from daily where days_back = 1
)

select
    s.*,
    (s.annualized_return - {{ var('risk_free_rate') }}) / nullif(s.annualized_vol, 0) as sharpe_ratio,
    -s.var95_return  * l.portfolio_value_cad                                           as var95_cad,
    -s.var99_return  * l.portfolio_value_cad                                           as var99_cad,
    -s.cvar95_return * l.portfolio_value_cad                                           as cvar95_cad,
    l.portfolio_value_cad                                                              as current_value_cad
from stats s
cross join latest l
