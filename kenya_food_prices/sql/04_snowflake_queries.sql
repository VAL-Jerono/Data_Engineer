-- =============================================================
-- Kenya Food Prices Data Engineering Capstone
-- Author: Rene Bosire | Everything Data Bootcamp
-- Script: snowflake_queries.sql
-- Purpose: Demonstrates Snowflake-specific SQL features
--          and differences vs PostgreSQL
-- =============================================================

-- ── Setup ──────────────────────────────────────────────────────
USE ROLE    SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE  KENYA_FOOD_PRICES;
USE SCHEMA    PUBLIC;

-- ── Create staging table (Snowflake syntax) ────────────────────
CREATE TABLE IF NOT EXISTS STG_FOOD_PRICES (
    DATE            DATE,
    YEAR            INT,
    MONTH           INT,
    COUNTY          VARCHAR(100),
    DISTRICT        VARCHAR(100),
    MARKET          VARCHAR(150),
    CATEGORY        VARCHAR(100),
    COMMODITY       VARCHAR(150),
    UNIT            VARCHAR(50),
    PRICETYPE       VARCHAR(50),
    CURRENCY        VARCHAR(10),
    PRICE_KES       FLOAT,
    PRICE_USD       FLOAT,
    PRICE_PER_KG    FLOAT,
    IS_VALID_PRICE  BOOLEAN,
    LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── DIFFERENCE 1: Snowflake uses VARIANT for semi-structured ───
-- (no equivalent in PostgreSQL without jsonb extension)
CREATE TABLE IF NOT EXISTS RAW_METADATA (
    FILE_NAME   VARCHAR,
    LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STATS       VARIANT        -- JSON blob: row counts, quality scores, etc.
);

-- ── DIFFERENCE 2: Snowflake Time Travel ────────────────────────
-- Query how the table looked 1 hour ago — no PostgreSQL equivalent
SELECT COUNT(*) FROM STG_FOOD_PRICES
AT (OFFSET => -3600);  -- 1 hour ago in seconds

-- ── DIFFERENCE 3: FLATTEN / LATERAL JOIN on VARIANT ───────────
-- Snowflake natively handles JSON without casting
SELECT
    FILE_NAME,
    LOADED_AT,
    f.VALUE::STRING AS quality_note
FROM RAW_METADATA,
LATERAL FLATTEN(INPUT => STATS:quality_notes) f;

-- ── DIFFERENCE 4: RESULT_SCAN — reuse last query result ────────
-- Execute any query, then inspect it without re-running
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ── DIFFERENCE 5: ILIKE is native in Snowflake (case-insensitive LIKE)
SELECT DISTINCT COMMODITY
FROM STG_FOOD_PRICES
WHERE COMMODITY ILIKE '%maize%';   -- same as PostgreSQL ILIKE

-- ── Key aggregate query: avg price by county + commodity ───────
SELECT
    COUNTY,
    COMMODITY,
    YEAR,
    ROUND(AVG(PRICE_KES), 2)    AS AVG_PRICE_KES,
    COUNT(*)                    AS RECORDS
FROM STG_FOOD_PRICES
WHERE IS_VALID_PRICE = TRUE
GROUP BY COUNTY, COMMODITY, YEAR
ORDER BY YEAR DESC, COUNTY, COMMODITY;

-- ── Maize trend — Snowflake window function ────────────────────
SELECT
    YEAR,
    MONTH,
    COUNTY,
    ROUND(AVG(PRICE_KES), 2)                    AS AVG_MAIZE_PRICE,
    LAG(ROUND(AVG(PRICE_KES), 2))
        OVER (PARTITION BY COUNTY ORDER BY YEAR, MONTH) AS PREV_PRICE,
    ROUND(
        (AVG(PRICE_KES) - LAG(AVG(PRICE_KES))
            OVER (PARTITION BY COUNTY ORDER BY YEAR, MONTH))
        / NULLIFZERO(LAG(AVG(PRICE_KES))
            OVER (PARTITION BY COUNTY ORDER BY YEAR, MONTH)) * 100,
    2)                                          AS MOM_PCT_CHANGE
FROM STG_FOOD_PRICES
WHERE COMMODITY ILIKE '%maize%'
  AND IS_VALID_PRICE = TRUE
GROUP BY YEAR, MONTH, COUNTY
ORDER BY COUNTY, YEAR, MONTH;

-- ── Suspend warehouse when done to save credits ────────────────
ALTER WAREHOUSE COMPUTE_WH SUSPEND;
