-- =============================================================
-- Kenya Food Prices Data Engineering 
-- Script: 01_setup.sql — Database & Raw Staging Table
-- =============================================================

-- Create the database (run as superuser outside this script)
-- CREATE DATABASE kenya_food_prices;

-- Connect to the database before running below
-- \c kenya_food_prices;

-- ----------------------------------------------------------------
-- Drop & recreate schema for clean runs
-- ----------------------------------------------------------------
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS warehouse CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA warehouse;

-- ----------------------------------------------------------------
-- RAW staging table — mirrors the CSV closely, plus source lineage
-- ----------------------------------------------------------------
CREATE TABLE raw.raw_food_prices (
    id              SERIAL PRIMARY KEY,
    date            DATE,
    admin1          VARCHAR(100),   -- County / Region
    admin2          VARCHAR(100),   -- Sub-county / District
    market          VARCHAR(150),
    market_id       INTEGER,
    latitude        NUMERIC(10, 6),
    longitude       NUMERIC(10, 6),
    category        VARCHAR(100),
    commodity       VARCHAR(150),
    commodity_id    INTEGER,
    unit            VARCHAR(50),
    priceflag       VARCHAR(50),
    pricetype       VARCHAR(50),
    currency        VARCHAR(10),
    price           NUMERIC(12, 4),
    usdprice        NUMERIC(12, 4),
    source_file     VARCHAR(255),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- ----------------------------------------------------------------
-- Indexes for common query patterns
-- ----------------------------------------------------------------
CREATE INDEX idx_raw_date      ON raw.raw_food_prices (date);
CREATE INDEX idx_raw_market    ON raw.raw_food_prices (market);
CREATE INDEX idx_raw_commodity ON raw.raw_food_prices (commodity);
CREATE INDEX idx_raw_admin1    ON raw.raw_food_prices (admin1);

COMMENT ON TABLE raw.raw_food_prices IS
    'Raw ingestion of WFP Kenya food price data. No transformations applied.';

SELECT 'raw.raw_food_prices created successfully.' AS status;
