-- dbt model: stg_food_prices.sql
-- Layer: staging
-- Author: Rene Bosire | Everything Data Bootcamp
-- Description: Selects clean, valid rows from staging table
--              and prepares them for the mart layer.

{{
  config(
    materialized = 'view',
    schema       = 'analytics'
  )
}}

SELECT
    price_date                              AS date,
    year,
    month,
    COALESCE(county,   'Unknown')           AS county,
    COALESCE(district, 'Unknown')           AS district,
    market,
    COALESCE(category, 'Uncategorized')     AS category,
    commodity,
    unit,
    pricetype,
    currency,
    price_kes,
    price_usd,
    price_per_kg,
    is_valid_price

FROM {{ source('staging', 'stg_food_prices') }}

WHERE is_valid_price = TRUE
  AND price_date IS NOT NULL
  AND market     IS NOT NULL
  AND commodity  IS NOT NULL
