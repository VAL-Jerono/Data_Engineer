# Kenya Food Prices Data Engineering Capstone

**Author:** Rene Bosire  
**Bootcamp:** Everything Data, Data Engineering Track  
**Submission target:** End of March 2026

## Project Summary

This capstone documents a local-first data engineering pipeline for the WFP Kenya food prices dataset. The repository covers the full submission brief from raw PostgreSQL setup to cleaned ETL, dbt marts, and dashboard support.

The dataset already lives in this repository as [wfp_food_prices_ken.csv](wfp_food_prices_ken.csv), so extraction is treated as a local-file step by default. The code still supports downloading the file from a remote URL if needed, but the capstone workflow does not depend on re-extraction.

## What This Repo Answers

This project is organized around the two monthly deliverables in the brief:

- Month 1: basic SQL exploration and staging of Kenyan food prices.
- Month 2: cleaned and enriched ETL, warehouse modeling, dbt validation, and presentation-ready visuals.

The code and SQL scripts are written to answer the core questions behind the dataset:

- How do prices vary by market, county, commodity, and month?
- Which commodities are widely traded across markets?
- How can raw CSV data be cleaned, quality-checked, staged, and modeled into a star schema?
- Which marts are suitable for dashboards and demo queries?

## Dataset Snapshot

Based on the local CSV in the repo:

- Rows: 18,837
- Date range: 2006-01-15 to 2026-03-15
- Markets: 226
- Commodities: 51
- Distinct units: 14

## End-to-End Flow

```text
Local CSV
   |
   v
extract.py  ->  clean.py  ->  quality.py
   |
   v
PostgreSQL raw.raw_food_prices
   |
   v
load.py -> staging.stg_food_prices
   |
   v
warehouse.dim_date / dim_market / dim_commodity / fact_prices
   |
   +--> dbt staging + marts
   +--> Grafana / Metabase
   +--> optional Snowflake mirror
```

## Repository Map

- [airflow/dags/kenya_food_prices_dag.py](airflow/dags/kenya_food_prices_dag.py) orchestrates the full monthly pipeline.
- [python/extract.py](python/extract.py) loads from a local CSV first, then falls back to download.
- [python/clean.py](python/clean.py) standardizes names, parses dates, derives fields, and flags invalid rows.
- [python/quality.py](python/quality.py) runs critical and warning-level data checks.
- [python/load.py](python/load.py) creates database objects, loads raw and staging data, populates dimensions, and builds the fact table.
- [python/pipeline.py](python/pipeline.py) ties the full ETL together as a single executable entry point.
- [sql/01_setup.sql](sql/01_setup.sql) sets up the raw schema and raw staging table.
- [sql/02_analysis_queries.sql](sql/02_analysis_queries.sql) contains the exploratory SQL used for Month 1.
- [sql/03_staging_and_dims.sql](sql/03_staging_and_dims.sql) creates the cleaned staging table and star schema.
- [sql/04_snowflake_queries.sql](sql/04_snowflake_queries.sql) shows the Snowflake-specific comparison queries.
- [dbt/models/staging/stg_food_prices.sql](dbt/models/staging/stg_food_prices.sql) builds the staging model.
- [dbt/models/marts/mart_monthly_avg_prices.sql](dbt/models/marts/mart_monthly_avg_prices.sql) builds the monthly average mart.
- [dbt/models/marts/mart_maize_price_trends.sql](dbt/models/marts/mart_maize_price_trends.sql) builds the maize trend mart.
- [visuals/grafana_dashboard.json](visuals/grafana_dashboard.json) provisions the dashboard structure.

## Month 1 Deliverables

### 1. PostgreSQL setup script

[sql/01_setup.sql](sql/01_setup.sql) creates the `raw`, `staging`, and `warehouse` schemas and defines the `raw.raw_food_prices` table with proper data types for dates, text fields, and numeric price columns.

### 2. Python CSV-to-PostgreSQL load script

[python/load.py](python/load.py) loads the CSV into PostgreSQL using pandas and SQLAlchemy, and also creates the core schemas and tables when they are missing.

### 3. SQL exploration queries

[sql/02_analysis_queries.sql](sql/02_analysis_queries.sql) covers:

- `SELECT`, `WHERE`, and `ORDER BY` for market-level lookups.
- Aggregates such as `AVG`, `MIN`, `MAX`, and `COUNT`.
- `GROUP BY` plus `HAVING` for commodities with broad market coverage.
- Date handling with `DATE_TRUNC` and `EXTRACT`.
- A data profiling query to summarize missing values and dataset size.

### 4. Basic Airflow DAG

[airflow/dags/kenya_food_prices_dag.py](airflow/dags/kenya_food_prices_dag.py) provides a simple monthly DAG that resolves the CSV source, loads raw data, cleans it, runs quality checks, loads staging and warehouse tables, and optionally mirrors the data to Snowflake.

### 5. README with observed data issues

This file documents the observed issues and the implementation choices behind the pipeline.

## Month 2 Deliverables

### 1. Modular ETL scripts

- [python/extract.py](python/extract.py)
- [python/clean.py](python/clean.py)
- [python/quality.py](python/quality.py)
- [python/load.py](python/load.py)
- [python/pipeline.py](python/pipeline.py)

Together these scripts implement extract -> transform -> quality checks -> load, with logging, error handling, and incremental staging logic.

### 2. Cleaned staging and dimension tables

[sql/03_staging_and_dims.sql](sql/03_staging_and_dims.sql) creates the cleaned staging table plus `dim_market`, `dim_commodity`, `dim_date`, and `fact_prices` in PostgreSQL.

### 3. Load cleaned data to PostgreSQL

The cleaned load is handled by [python/pipeline.py](python/pipeline.py) and the lower-level functions in [python/load.py](python/load.py).

### 4. Optional Snowflake support

[sql/04_snowflake_queries.sql](sql/04_snowflake_queries.sql) shows the Snowflake-specific examples and highlights where Snowflake syntax differs from PostgreSQL.

### 5. Dashboard asset

[visuals/grafana_dashboard.json](visuals/grafana_dashboard.json) defines a dashboard with a maize trend line, commodity price bars, record counts, and table views.

### 6. Updated Airflow DAG

[airflow/dags/kenya_food_prices_dag.py](airflow/dags/kenya_food_prices_dag.py) includes the cleaning, quality, staging, dimension, fact, and optional Snowflake steps.

### 7. Full modular ETL/ELT project

The pipeline is structured as reusable functions instead of a single notebook-style script. The clean step derives year/month and `price_per_kg`, the quality step enforces critical checks, and the load step performs idempotent incremental inserts.

### 8. Star schema in PostgreSQL

[sql/03_staging_and_dims.sql](sql/03_staging_and_dims.sql) implements the warehouse star schema. The dimensions are built around business keys, not just surrogate IDs:

- market uniqueness uses market + district + county
- commodity uniqueness uses commodity + unit
- date is prebuilt in `dim_date` for the full project range

### 9. dbt models for aggregate views

- [dbt/models/staging/stg_food_prices.sql](dbt/models/staging/stg_food_prices.sql)
- [dbt/models/marts/mart_monthly_avg_prices.sql](dbt/models/marts/mart_monthly_avg_prices.sql)
- [dbt/models/marts/mart_maize_price_trends.sql](dbt/models/marts/mart_maize_price_trends.sql)

These models are paired with tests in [dbt/models/schema.yml](dbt/models/schema.yml) and use the `dbt_utils` package for validation.

### 10. README with design choices and assumptions

This section captures the main assumptions used across the pipeline.

### 11. Presentation/demo support

The repo now includes the code, SQL, dbt, and dashboard assets needed for a 10-15 minute demo.

### 12. Repo-ready structure

The repo also includes [.env.example](.env.example), [.gitignore](.gitignore), `docker-compose.yml`, and `requirements.txt` to make the project easier to run locally or in containers.

## How To Run

### Local Python run

```bash
pip install -r requirements.txt

export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=kenya_food_prices
export PG_USER=kenya
export PG_PASSWORD=kenya123
export LOCAL_CSV_PATH=./wfp_food_prices_ken.csv

python python/pipeline.py --local wfp_food_prices_ken.csv --skip-snowflake
```

### Docker stack

```bash
docker compose up -d
psql -h localhost -U kenya -d kenya_food_prices -f sql/01_setup.sql -f sql/03_staging_and_dims.sql
```

Airflow uses the mounted CSV at `/opt/airflow/data/wfp_food_prices_ken.csv`.

### dbt

```bash
cd dbt
dbt deps
dbt run --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
```

## What Each Layer Does

### Extraction

[python/extract.py](python/extract.py) is local-first. It loads the repo CSV if present, then falls back to a downloaded copy if needed.

### Cleaning

[python/clean.py](python/clean.py) standardizes headers, parses the date field, normalizes strings, coerces numeric price columns, derives year and month, and calculates `price_per_kg` when the unit can be converted reliably.

### Quality checks

[python/quality.py](python/quality.py) checks for empty data, required columns, excessive null prices, invalid date ranges, negative prices, duplicate business keys, and the ratio of valid rows.

### Load

[python/load.py](python/load.py) ensures schemas and tables exist, loads raw data, performs incremental staging inserts, upserts dimensions, and populates the fact table.

### Orchestration

[python/pipeline.py](python/pipeline.py) is the one-shot runner for local execution, while [airflow/dags/kenya_food_prices_dag.py](airflow/dags/kenya_food_prices_dag.py) is the monthly scheduler for the same workflow.

## Data Issues Observed

The CSV is usable, but a few issues still matter for analysis:

1. Missing county and district values

There are rows where `admin1` and `admin2` are null. The cleaning layer preserves the records and downstream models fall back to `Unknown` where needed.

2. Unit inconsistency across the same commodity

The dataset mixes units like `KG`, `90 KG`, `50 KG`, `200 G`, `500 ML`, `L`, `Head`, and `Bunch`. The pipeline only derives `price_per_kg` when the conversion is safe.

3. Mixed measurement families

Some commodities are sold by weight, others by volume or count. That means direct price comparisons are not always meaningful, so the warehouse keeps the original unit values.

## Design Choices And Assumptions

- The capstone is local-first because the CSV is already in the repository and the brief does not require re-running extraction.
- The raw table mirrors the source file closely so lineage stays visible from CSV to warehouse.
- The warehouse dimensions use business keys that are more specific than a single name field.
- Incremental loading is based on a natural business key, not only a max-date watermark, so same-day reruns do not create duplicates.
- Snowflake remains optional so the full demo can run entirely on PostgreSQL if cloud credentials are unavailable.

## Presentation Outline

1. Problem context and business value
2. Pipeline architecture
3. Month 1 SQL exploration
4. ETL, quality checks, and incremental logic
5. dbt models and dashboard outputs
6. Challenges, lessons learned, and future improvements

## Suggested Demo Flow

1. Explain the Kenya food prices problem and why market-level monthly data matters.
2. Show the raw CSV and the PostgreSQL raw table schema.
3. Run [sql/02_analysis_queries.sql](sql/02_analysis_queries.sql).
4. Trigger the Airflow DAG or run [python/pipeline.py](python/pipeline.py).
5. Show the star schema and dbt marts.
6. Open Grafana or Metabase and walk through the maize trend and average price visuals.

## Final Proof Bundle

The submission proof bundle is documented in [proofs/FINAL_PROOF.md](proofs/FINAL_PROOF.md). It links the completed deliverables to the execution evidence:

- [dbt_deps.txt](proofs/dbt_deps.txt)
- [dbt_run.txt](proofs/dbt_run.txt)
- [dbt_test.txt](proofs/dbt_test.txt)
- [airflow_webserver.log](proofs/airflow_webserver.log)
- [airflow_scheduler.log](proofs/airflow_scheduler.log)
- [docker_compose_ps.txt](proofs/docker_compose_ps.txt)
- [notify_success_log_path.txt](proofs/notify_success_log_path.txt)

Place the final screenshots in `proofs/screenshots/` using these filenames:

- `airflow_success.png`
- `grafana_dashboard.png`
- `dbt_run.png`
- `dbt_test.png`

The repository is complete when the proof markdown, the log files, and the screenshots are together in the `proofs/` folder.

## Future Improvements

- Add stronger unit conversion for non-kg products.
- Add warehouse-level SQL tests for duplicate facts and null foreign keys.
- Standardize dashboard provisioning for Grafana and Metabase.
- Extend Snowflake support into a full dual-target dbt deployment.
