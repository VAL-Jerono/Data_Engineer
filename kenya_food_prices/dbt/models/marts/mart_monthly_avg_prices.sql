-- dbt model: mart_monthly_avg_prices.sql
-- Layer: marts
-- Author: Rene Bosire | Everything Data Bootcamp
-- Description: Monthly average prices per county and commodity.
--              Used directly by Grafana/Metabase dashboards.

{{
  config(
    materialized = 'table',
    schema       = 'marts',
    indexes = [
      {'columns': ['year', 'month']},
      {'columns': ['county']},
      {'columns': ['commodity']},
    ]
  )
}}

SELECT
    year,
    month,
    DATE_TRUNC('month', date)           AS month_start,
    county,
    commodity,
    unit,
    ROUND(AVG(price_kes),    2)         AS avg_price_kes,
    ROUND(AVG(price_usd),    2)         AS avg_price_usd,
    ROUND(AVG(price_per_kg), 2)         AS avg_price_per_kg,
    ROUND(MIN(price_kes),    2)         AS min_price_kes,
    ROUND(MAX(price_kes),    2)         AS max_price_kes,
    ROUND(STDDEV(price_kes), 2)         AS stddev_price_kes,
    COUNT(*)                            AS num_observations,
    COUNT(DISTINCT market)              AS num_markets

FROM {{ ref('stg_food_prices') }}

GROUP BY 1, 2, 3, 4, 5, 6

ORDER BY year DESC, month DESC, county, commodity
