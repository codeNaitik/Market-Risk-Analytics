-- Calendar dimension covering every date that appears in prices or macro data.
with bounds as (
    select min(d) as start_date, max(d) as end_date from (
        select min(trade_date) as d from {{ ref('stg_daily_prices') }}
        union all select max(trade_date) from {{ ref('stg_daily_prices') }}
        union all select min(obs_date) from {{ ref('stg_macro_observations') }}
        union all select max(obs_date) from {{ ref('stg_macro_observations') }}
    ) x
),

spine as (
    {{ day_spine('(select start_date from bounds)', '(select end_date from bounds)') }}
),

us_trading_days as (   -- a day is a US trading day if the benchmark index traded
    select distinct p.trade_date
    from {{ ref('stg_daily_prices') }} p
    join {{ ref('company_universe') }} c on c.ticker = p.ticker and c.is_benchmark
)

select
    cast(extract(year from s.calendar_date) * 10000
         + extract(month from s.calendar_date) * 100
         + extract(day from s.calendar_date) as int)          as date_key,
    s.calendar_date,
    cast(extract(year from s.calendar_date) as int)          as year,
    cast(extract(quarter from s.calendar_date) as int)       as quarter,
    cast(extract(month from s.calendar_date) as int)         as month,
    case cast(extract(month from s.calendar_date) as int)
        when 1 then 'Jan' when 2 then 'Feb' when 3 then 'Mar' when 4 then 'Apr'
        when 5 then 'May' when 6 then 'Jun' when 7 then 'Jul' when 8 then 'Aug'
        when 9 then 'Sep' when 10 then 'Oct' when 11 then 'Nov' else 'Dec' end
                                                            as month_short_name,
    cast(extract(year from s.calendar_date) * 100
         + extract(month from s.calendar_date) as int)       as year_month,
    cast(date_trunc('month', s.calendar_date) as date)       as month_start_date,
    {{ iso_dow('s.calendar_date') }}                         as iso_day_of_week,
    {{ iso_dow('s.calendar_date') }} >= 6                    as is_weekend,
    t.trade_date is not null                                 as is_us_trading_day
from spine s
left join us_trading_days t on t.trade_date = s.calendar_date
