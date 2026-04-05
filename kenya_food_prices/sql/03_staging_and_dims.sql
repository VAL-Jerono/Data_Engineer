-- =============================================================
-- Kenya Food Prices Data Engineering Capstone
-- Author: Rene Bosire | Everything Data Bootcamp
-- Script: 03_staging_and_dims.sql — Cleaned Staging + Dimensions
-- =============================================================

-- ---------------------------------------------------------------
-- Cleaned staging table
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS staging.stg_food_prices;

CREATE TABLE staging.stg_food_prices (
    id              SERIAL PRIMARY KEY,
    price_date      DATE            NOT NULL,
    year            INT             NOT NULL,
    month           INT             NOT NULL,
    county          VARCHAR(100),
    district        VARCHAR(100),
    market          VARCHAR(150)    NOT NULL,
    source_market_id INTEGER,
    latitude        NUMERIC(10, 6),
    longitude       NUMERIC(10, 6),
    category        VARCHAR(100),
    commodity       VARCHAR(150)    NOT NULL,
    source_commodity_id INTEGER,
    unit            VARCHAR(50),
    priceflag       VARCHAR(50),
    pricetype       VARCHAR(50),
    currency        VARCHAR(10),
    price_kes       NUMERIC(12, 4),
    price_usd       NUMERIC(12, 4),
    price_per_kg    NUMERIC(12, 4),
    source_file     VARCHAR(255),
    is_valid_price  BOOLEAN         DEFAULT TRUE,
    loaded_at       TIMESTAMP       DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_staging_food_prices_business_key
    ON staging.stg_food_prices (price_date, market, commodity, unit, pricetype);

-- ---------------------------------------------------------------
-- Dimension: Markets
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.dim_market CASCADE;

CREATE TABLE warehouse.dim_market (
    market_id       SERIAL PRIMARY KEY,
    market_name     VARCHAR(150)    NOT NULL,
    district        VARCHAR(100),
    county          VARCHAR(100),
    source_market_id INTEGER,
    latitude        NUMERIC(10, 6),
    longitude       NUMERIC(10, 6),
    created_at      TIMESTAMP       DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_dim_market_business_key
    ON warehouse.dim_market (market_name, district, county);

-- ---------------------------------------------------------------
-- Dimension: Commodities
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.dim_commodity CASCADE;

CREATE TABLE warehouse.dim_commodity (
    commodity_id    SERIAL PRIMARY KEY,
    commodity_name  VARCHAR(150)    NOT NULL,
    category        VARCHAR(100),
    unit            VARCHAR(50),
    source_commodity_id INTEGER,
    created_at      TIMESTAMP       DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_dim_commodity_business_key
    ON warehouse.dim_commodity (commodity_name, unit);

-- ---------------------------------------------------------------
-- Dimension: Date
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.dim_date CASCADE;

CREATE TABLE warehouse.dim_date (
    date_id         INT PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE            NOT NULL UNIQUE,
    year            INT             NOT NULL,
    quarter         INT             NOT NULL,
    month           INT             NOT NULL,
    month_name      VARCHAR(15)     NOT NULL,
    day_of_month    INT             NOT NULL,
    is_year_start   BOOLEAN         DEFAULT FALSE,
    is_year_end     BOOLEAN         DEFAULT FALSE
);

-- Populate dim_date for the data range (2006–2026)
INSERT INTO warehouse.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(YEAR  FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TO_CHAR(d, 'Month'),
    EXTRACT(DAY   FROM d)::INT,
    EXTRACT(MONTH FROM d) = 1  AND EXTRACT(DAY FROM d) = 1,
    EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) = 31
FROM generate_series('2006-01-01'::DATE, '2026-12-31'::DATE, '1 day') AS d;

-- ---------------------------------------------------------------
-- Star schema: Fact table
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS warehouse.fact_prices CASCADE;

CREATE TABLE warehouse.fact_prices (
    fact_id         BIGSERIAL PRIMARY KEY,
    date_id         INT             REFERENCES warehouse.dim_date(date_id),
    market_id       INT             REFERENCES warehouse.dim_market(market_id),
    commodity_id    INT             REFERENCES warehouse.dim_commodity(commodity_id),
    price_kes       NUMERIC(12, 4),
    price_usd       NUMERIC(12, 4),
    price_per_kg    NUMERIC(12, 4),
    pricetype       VARCHAR(50),
    is_valid_price  BOOLEAN,
    loaded_at       TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX idx_fact_date      ON warehouse.fact_prices (date_id);
CREATE INDEX idx_fact_market    ON warehouse.fact_prices (market_id);
CREATE INDEX idx_fact_commodity ON warehouse.fact_prices (commodity_id);
CREATE UNIQUE INDEX uq_fact_prices_business_key
    ON warehouse.fact_prices (date_id, market_id, commodity_id, pricetype);

SELECT 'Staging tables and star schema created successfully.' AS status;
