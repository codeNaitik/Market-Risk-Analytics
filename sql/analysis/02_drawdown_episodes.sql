-- Q: List every portfolio drawdown episode deeper than 5%: when it started, bottomed,
--    recovered, and how many trading days the recovery took.
-- Skills: gaps-and-islands (running sum of "new peak" flags), conditional aggregates
with flagged as (
    select
        trade_date,
        drawdown,
        case when drawdown = 0 then 1 else 0 end                        as is_new_peak
    from {{ ref('mart_portfolio_daily') }}
),

episodes as (
    select
        *,
        sum(is_new_peak) over (order by trade_date rows between unbounded preceding and current row) as episode_id
    from flagged
),

summary as (
    select
        episode_id,
        min(trade_date)                                                  as peak_date,
        min(drawdown)                                                    as max_drawdown,
        min(case when drawdown = 0 then null else trade_date end)        as first_down_day,
        max(trade_date)                                                  as last_day_below_peak,
        count(*) - 1                                                     as trading_days_underwater
    from episodes
    group by episode_id
),

troughs as (
    select e.episode_id, min(e.trade_date) as trough_date
    from episodes e
    join summary s on s.episode_id = e.episode_id and e.drawdown = s.max_drawdown
    group by e.episode_id
),

recoveries as (
    select episode_id - 1 as episode_id, min(trade_date) as recovery_date   -- next episode starts at the new peak
    from episodes
    group by episode_id
)

select
    s.peak_date,
    t.trough_date,
    r.recovery_date,                                                 -- null = not yet recovered
    round(100 * s.max_drawdown, 2)                                   as max_drawdown_pct,
    s.trading_days_underwater,
    r.recovery_date is not null                                      as has_recovered
from summary s
join troughs t on t.episode_id = s.episode_id
left join recoveries r on r.episode_id = s.episode_id
where s.max_drawdown < -0.05
order by s.max_drawdown
