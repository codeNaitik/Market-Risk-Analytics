-- Every macro series on a continuous daily calendar, forward-filled.
-- Monthly series (CPI, unemployment) carry their last value until the next release,
-- so daily price data can be joined to "the rate in force that day".
with bounds as (
    select min(obs_date) as start_date, max(obs_date) as end_date
    from {{ ref('stg_macro_observations') }}
),

spine as (
    {{ day_spine('(select start_date from bounds)', '(select end_date from bounds)') }}
),

series as (
    select distinct series_id from {{ ref('stg_macro_observations') }}
),

grid as (
    select s.series_id, d.calendar_date
    from series s
    cross join spine d
)

select
    g.series_id,
    g.calendar_date,
    o.value                                                            as reported_value,
    {{ ffill('o.value', 'g.series_id', 'g.calendar_date') }}           as value
from grid g
left join {{ ref('stg_macro_observations') }} o
    on o.series_id = g.series_id
   and o.obs_date = g.calendar_date
