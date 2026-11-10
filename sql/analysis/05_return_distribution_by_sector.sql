-- Q: How fat are each sector's tails? Compare median, 5th/95th percentile daily returns,
--    and how often a stock moved more than 3 standard deviations.
-- Skills: PERCENTILE_CONT WITHIN GROUP, window stddev, conditional counts
with r as (
    select
        f.ticker,
        c.sector,
        f.daily_return,
        stddev_samp(f.daily_return) over (partition by f.ticker) as ticker_sd
    from {{ ref('fct_daily_prices') }} f
    join {{ ref('dim_company') }} c on c.ticker = f.ticker
    where f.daily_return is not null and not c.is_benchmark
)

select
    sector,
    count(*)                                                                         as observations,
    round(100 * percentile_cont(0.05) within group (order by daily_return), 3)      as p05_pct,
    round(100 * percentile_cont(0.50) within group (order by daily_return), 3)      as median_pct,
    round(100 * percentile_cont(0.95) within group (order by daily_return), 3)      as p95_pct,
    sum(case when abs(daily_return) > 3 * ticker_sd then 1 else 0 end)               as three_sigma_days,
    -- a normal distribution expects ~0.27% of days beyond 3 sigma
    round(100.0 * sum(case when abs(daily_return) > 3 * ticker_sd then 1 else 0 end) / count(*), 3) as pct_three_sigma_days
from r
group by sector
order by pct_three_sigma_days desc
