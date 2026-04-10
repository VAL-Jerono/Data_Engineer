#!/bin/bash
# =============================================================
# Kenya Food Prices — PostgreSQL Docker Init Script
# Author: Rene Bosire | Everything Data Bootcamp
# Runs automatically when the postgres container starts fresh.
# Creates the app database, user, and base schemas.
# =============================================================

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- ── Application database ─────────────────────────────────
    CREATE DATABASE kenya_food_prices;

    -- ── Airflow database ─────────────────────────────────────
    CREATE DATABASE airflow;

    CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

    -- ── Metabase internal database ───────────────────────────
    CREATE DATABASE metabase;

    -- ── Application user ─────────────────────────────────────
    CREATE USER kenya WITH ENCRYPTED PASSWORD 'kenya123';
    GRANT ALL PRIVILEGES ON DATABASE kenya_food_prices TO kenya;

EOSQL

# Connect to the app DB and create schemas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "kenya_food_prices" <<-EOSQL

    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS warehouse;
    CREATE SCHEMA IF NOT EXISTS marts;

    GRANT ALL ON SCHEMA raw,       staging, warehouse, marts TO kenya;
    GRANT ALL ON ALL TABLES    IN SCHEMA raw, staging, warehouse, marts TO kenya;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA raw, staging, warehouse, marts TO kenya;

    ALTER DEFAULT PRIVILEGES IN SCHEMA raw, staging, warehouse, marts
        GRANT ALL ON TABLES    TO kenya;
    ALTER DEFAULT PRIVILEGES IN SCHEMA raw, staging, warehouse, marts
        GRANT ALL ON SEQUENCES TO kenya;

EOSQL

echo "✅  PostgreSQL init complete — databases and schemas ready."
