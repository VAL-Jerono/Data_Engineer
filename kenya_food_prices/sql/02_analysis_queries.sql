-- =============================================================
-- Kenya Food Prices Data Engineering 
-- Script: 02_analysis_queries.sql — 8 Analytical SQL Queries
-- =============================================================

-- ---------------------------------------------------------------
-- QUERY 1: Latest prices for a specific market (Nairobi)
-- Tests: SELECT / WHERE / ORDER BY
-- ---------------------------------------------------------------
SELECT
    date,
    market,
    commodity,
    unit,
    price,
    currency
FROM raw.raw_food_prices
WHERE market ILIKE '%nairobi%'
  AND price IS NOT NULL
ORDER BY date DESC, commodity ASC
LIMIT 50;

-- ---------------------------------------------------------------
-- QUERY 2: Average, min, max price per commodity (all-time)
-- Tests: Aggregates — AVG, MIN, MAX, COUNT
-- ---------------------------------------------------------------
SELECT
    commodity,
    unit,
    currency,
    ROUND(AVG(price), 2)    AS avg_price,
    MIN(price)              AS min_price,
    MAX(price)              AS max_price,
    COUNT(*)                AS observations
FROM raw.raw_food_prices
WHERE price IS NOT NULL
GROUP BY commodity, unit, currency
ORDER BY avg_price DESC;

-- ---------------------------------------------------------------
-- QUERY 3: Commodities traded in more than 10 distinct markets
-- Tests: GROUP BY + HAVING
-- ---------------------------------------------------------------
SELECT
    commodity,
    COUNT(DISTINCT market) AS market_count,
    ROUND(AVG(price), 2)   AS avg_price
FROM raw.raw_food_prices
WHERE price IS NOT NULL
GROUP BY commodity
HAVING COUNT(DISTINCT market) > 10
ORDER BY market_count DESC;

-- ---------------------------------------------------------------
-- QUERY 4: Average price by county and year
-- Tests: GROUP BY + HAVING + date handling
-- ---------------------------------------------------------------
SELECT
    admin1                          AS county,
    EXTRACT(YEAR FROM date)::INT    AS year,
    ROUND(AVG(price), 2)            AS avg_price,
    COUNT(*)                        AS records
FROM raw.raw_food_prices
WHERE price IS NOT NULL
  AND admin1 IS NOT NULL
GROUP BY admin1, EXTRACT(YEAR FROM date)
HAVING COUNT(*) >= 5
ORDER BY county, year;

-- ---------------------------------------------------------------
-- QUERY 5: Filter recent 12 months of data
-- Tests: Date handling — EXTRACT, date arithmetic
-- ---------------------------------------------------------------
SELECT
    DATE_TRUNC('month', date)   AS month,
    commodity,
    market,
    ROUND(AVG(price), 2)        AS avg_monthly_price
FROM raw.raw_food_prices
WHERE date >= NOW() - INTERVAL '12 months'
  AND price IS NOT NULL
GROUP BY DATE_TRUNC('month', date), commodity, market
ORDER BY month DESC, commodity;

-- ---------------------------------------------------------------
-- QUERY 6: Year-over-year maize price comparison
-- Tests: Subquery + date extraction + joining aggregates
-- ---------------------------------------------------------------
WITH yearly_maize AS (
    SELECT
        EXTRACT(YEAR FROM date)::INT AS year,
        ROUND(AVG(price), 2)         AS avg_price,
        COUNT(*)                     AS num_records
    FROM raw.raw_food_prices
    WHERE commodity ILIKE '%maize%'
      AND price IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM date)
)
SELECT
    year,
    avg_price,
    num_records,
    LAG(avg_price) OVER (ORDER BY year)                  AS prev_year_price,
    ROUND(
        (avg_price - LAG(avg_price) OVER (ORDER BY year))
        / NULLIF(LAG(avg_price) OVER (ORDER BY year), 0) * 100,
    2)                                                    AS yoy_pct_change
FROM yearly_maize
ORDER BY year;

-- ---------------------------------------------------------------
-- QUERY 7: Price volatility index per commodity
-- Tests: STDDEV, ROUND, advanced aggregates
-- ---------------------------------------------------------------
SELECT
    commodity,
    ROUND(AVG(price), 2)    AS avg_price,
    ROUND(STDDEV(price), 2) AS price_stddev,
    ROUND(
        STDDEV(price) / NULLIF(AVG(price), 0) * 100,
    2)                      AS coefficient_of_variation_pct,
    COUNT(*)                AS observations
FROM raw.raw_food_prices
WHERE price IS NOT NULL
GROUP BY commodity
HAVING COUNT(*) >= 20
ORDER BY coefficient_of_variation_pct DESC
LIMIT 20;

-- ---------------------------------------------------------------
-- QUERY 8: Data quality audit — missing values & duplicates
-- Tests: NULL handling, COUNT FILTER, data profiling
-- ---------------------------------------------------------------
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(*) FILTER (WHERE price IS NULL)               AS missing_price,
    COUNT(*) FILTER (WHERE market IS NULL)              AS missing_market,
    COUNT(*) FILTER (WHERE commodity IS NULL)           AS missing_commodity,
    COUNT(*) FILTER (WHERE date IS NULL)                AS missing_date,
    COUNT(DISTINCT market)                              AS unique_markets,
    COUNT(DISTINCT commodity)                           AS unique_commodities,
    COUNT(DISTINCT admin1)                              AS unique_counties,
    MIN(date)                                           AS earliest_date,
    MAX(date)                                           AS latest_date
FROM raw.raw_food_prices;
