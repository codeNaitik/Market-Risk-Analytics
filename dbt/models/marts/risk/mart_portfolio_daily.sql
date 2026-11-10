-- Grain: one row per trading day for the sample portfolio.
-- Assumption: constant target weights (daily rebalanced). A holding that did not trade
-- on a given day (e.g. TSX holiday) contributes a 0% return that day.
with weights as (
    select ticker, weight, notional_cad from {{ ref('portfolio_weights') }}
),

days as (
    select distinct r.trade_date
    from {{ ref('int_daily_returns') }} r
    join weights w on w.ticker = r.ticker
),

holding_returns as (
    select d.trade_date, w.ticker, w.weight, w.notional_cad, coalesce(r.daily_return, 0) as daily_return
    from days d
    cross join weights w
    left join {{ ref('int_daily_returns') }} r on r.ticker = w.ticker and r.trade_date = d.trade_date
),

portfolio as (
    select
        trade_date,
        max(notional_cad)               as notional_cad,
        sum(weight * daily_return)      as portfolio_return,
        count(*)                        as n_holdings
    from holding_returns
    group by trade_date
),

bench as (
    select r.trade_date, r.daily_return as benchmark_return
    from {{ ref('int_daily_returns') }} r
    join {{ ref('company_universe') }} c on c.ticker = r.ticker and c.is_benchmark
),

indexed as (
    select
        p.trade_date,
        p.portfolio_return,
        b.benchmark_return,
        p.notional_cad * exp(sum(ln(1 + p.portfolio_return)) over (order by p.trade_date
                             rows between unbounded preceding and current row))           as portfolio_value_cad,
        exp(sum(ln(1 + coalesce(b.benchmark_return, 0))) over (order by p.trade_date
                             rows between unbounded preceding and current row)) - 1       as benchmark_cumulative_return,
        {{ annualize_vol('stddev_samp(p.portfolio_return) over (order by p.trade_date rows between 62 preceding and current row)') }} as portfolio_vol_3m_ann,
        p.notional_cad
    from portfolio p
    left join bench b on b.trade_date = p.trade_date
)

select
    trade_date,
    portfolio_return,
    benchmark_return,
    portfolio_return - coalesce(benchmark_return, 0)                         as active_return,
    portfolio_value_cad,
    portfolio_value_cad / notional_cad - 1                                    as portfolio_cumulative_return,
    benchmark_cumulative_return,
    portfolio_vol_3m_ann,
    portfolio_value_cad / max(portfolio_value_cad) over (order by trade_date
        rows between unbounded preceding and current row) - 1                 as drawdown,
    portfolio_return * lag(portfolio_value_cad) over (order by trade_date)    as daily_pnl_cad
from indexed
