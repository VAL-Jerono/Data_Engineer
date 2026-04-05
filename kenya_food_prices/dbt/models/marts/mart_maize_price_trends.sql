-- dbt model: mart_maize_price_trends.sql
-- Layer: marts
-- Author: Rene Bosire | Everything Data Bootcamp
-- Description: Yearly maize price trend with YoY change.
--              Powers the main trend line dashboard.

{{
  config(
    materialized = 'table',
    schema       = 'marts'
  )
}}

WITH maize_monthly AS (
    SELECT
        year,
        month,
        month_start,
        county,
        avg_price_kes,
        num_observations
    FROM {{ ref('mart_monthly_avg_prices') }}
    WHERE LOWER(commodity) LIKE '%maize%'
),

with_lag AS (
    SELECT
        *,
        LAG(avg_price_kes) OVER (
            PARTITION BY county
            ORDER BY year, month
        ) AS prev_month_price
    FROM maize_monthly
)

SELECT
    year,
    month,
    month_start,
    county,
    ROUND(avg_price_kes,  2)            AS maize_price_kes,
    ROUND(prev_month_price, 2)          AS prev_month_price_kes,
    ROUND(
        (avg_price_kes - prev_month_price)
        / NULLIF(prev_month_price, 0) * 100,
    2)                                  AS mom_pct_change,
    num_observations

FROM with_lag
ORDER BY county, year, month
