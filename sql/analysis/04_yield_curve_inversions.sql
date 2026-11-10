-- Q: When was the US yield curve inverted (10Y below 2Y), for how long, and how did the
--    portfolio do during each inversion?
-- Skills: gaps-and-islands with ROW_NUMBER difference, join facts across subject areas
with daily as (
    select calendar_date, us_2s10s_spread, us_2s10s_spread < 0 as is_inverted
    from {{ ref('fct_macro_daily') }}
    where us_2s10s_spread is not null
),

islands as (
    select
        *,
        row_number() over (order by calendar_date)
          - row_number() over (partition by is_inverted order by calendar_date) as grp
    from daily
),

inversions as (
    select
        min(calendar_date)             as start_date,
        max(calendar_date)             as end_date,
        count(*)                       as calendar_days,
        min(us_2s10s_spread)           as deepest_spread
    from islands
    where is_inverted
    group by grp
    having count(*) >= 5               -- ignore single-day blips
)

select
    i.start_date,
    i.end_date,
    i.calendar_days,
    round(i.deepest_spread, 2)                                       as deepest_spread_pct_pts,
    round(100 * (exp(sum(ln(1 + p.portfolio_return))) - 1), 2)       as portfolio_return_pct,
    round(100 * (exp(sum(ln(1 + p.benchmark_return))) - 1), 2)       as benchmark_return_pct
from inversions i
left join {{ ref('mart_portfolio_daily') }} p
    on p.trade_date between i.start_date and i.end_date
group by i.start_date, i.end_date, i.calendar_days, i.deepest_spread
order by i.start_date
