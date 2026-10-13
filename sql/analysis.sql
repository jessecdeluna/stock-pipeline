-- Example questions the warehouse can answer once a few weeks of history build up.
-- Run with:  duckdb data/stocks.duckdb < sql/analysis.sql

-- 1. Which tickers show up on the most-actives list most often?
SELECT ticker, count(*) AS days_on_list, round(avg(rank), 1) AS avg_rank
FROM daily_quotes
GROUP BY ticker
ORDER BY days_on_list DESC, avg_rank
LIMIT 15;

-- 2. Day-over-day change in open price for each ticker
SELECT snapshot_date, ticker, open_price,
       round(100 * (open_price / lag(open_price) OVER w - 1), 2) AS pct_change
FROM daily_quotes
WINDOW w AS (PARTITION BY ticker ORDER BY snapshot_date)
ORDER BY ticker, snapshot_date;

-- 3. Unusual activity: today's volume vs. the 3-month average
SELECT ticker, volume, avg_volume, round(volume::DOUBLE / avg_volume, 2) AS volume_ratio
FROM latest_quotes
WHERE avg_volume > 0
ORDER BY volume_ratio DESC;

-- 4. Pipeline health: how often did we fall back or fail?
SELECT status, source, count(*) AS runs
FROM pipeline_runs
GROUP BY ALL
ORDER BY runs DESC;
