-- Query optimisation notes (run these against the Databricks SQL Warehouse and compare the
-- query profiles; record bytes scanned and duration in docs/performance.md).
--
-- 1) PARTITION PRUNING
--    silver.daily_prices is partitioned by trade_year. A filter on the partition column
--    lets the engine skip whole folders:
--      SLOW : where year(trade_date) = 2025        -- function on column, prunes nothing
--      FAST : where trade_year = 2025              -- reads 1 partition
--
-- 2) DATA SKIPPING / Z-ORDER (Delta)
--      OPTIMIZE silver.daily_prices ZORDER BY (ticker);
--    co-locates rows for the same ticker so "where ticker = 'AAPL'" skips most files.
--
-- 3) PRE-AGGREGATE FOR DASHBOARDS
--    Power BI visuals hit gold.mart_* tables (thousands of rows), never fct_daily_prices
--    with a GROUP BY at query time. That is why mart_sector_performance exists.
--
-- 4) AVOID SELECT * ON WIDE FACTS IN DIRECTQUERY MODE
--    Parquet/Delta are columnar: selecting 5 columns reads ~5 columns' worth of bytes.

-- Example to profile: monthly volume by ticker for one year
select ticker, cast(date_trunc('month', trade_date) as date) as month_start, sum(volume) as volume
from {{ ref('fct_daily_prices') }}
where trade_date >= date '2025-01-01' and trade_date < date '2026-01-01'
group by 1, 2
order by 1, 2
