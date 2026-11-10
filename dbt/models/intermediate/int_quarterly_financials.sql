-- Quarterly flow metrics with Q4 derived.
-- Companies file 10-Qs for Q1-Q3 only; Q4 is reported inside the annual 10-K.
-- So Q4 = annual value - (Q1 + Q2 + Q3) for the quarters inside that fiscal year.
with flows as (
    select * from {{ ref('stg_financials') }}
    where metric in ('revenue', 'net_income', 'operating_cash_flow', 'eps_diluted')
),

quarters as (
    select ticker, metric, period_start, period_end, value, is_restated, 'reported' as quarter_source
    from flows where period_type = 'quarter'
),

annual as (
    select ticker, metric, period_start, period_end, value, is_restated
    from flows where period_type = 'annual'
),

q1_q3 as (
    select a.ticker, a.metric, a.period_end as fy_end,
           sum(q.value) as q1_q3_value,
           count(*)     as n_quarters,
           max(q.period_end) as q3_end
    from annual a
    join quarters q
      on q.ticker = a.ticker and q.metric = a.metric
     and q.period_end > a.period_start and q.period_end < a.period_end
    group by a.ticker, a.metric, a.period_end
),

derived_q4 as (
    select
        a.ticker, a.metric,
        cast(q.q3_end + interval 1 day as date)   as period_start,
        a.period_end,
        -- EPS is not additive across quarters (share counts change) but this is the
        -- standard approximation analysts use for Q4 EPS when only FY EPS is filed.
        a.value - q.q1_q3_value                   as value,
        a.is_restated,
        'derived_from_10k'                        as quarter_source
    from annual a
    join q1_q3 q
      on q.ticker = a.ticker and q.metric = a.metric and q.fy_end = a.period_end
    where q.n_quarters = 3
)

select * from quarters
union all
select * from derived_q4
