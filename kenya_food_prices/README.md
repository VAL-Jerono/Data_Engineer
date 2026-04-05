# Kenya Food Prices Data Engineering Capstone

**Author:** Rene Bosire  
**Bootcamp:** Everything Data, Data Engineering Track  
**Submission target:** End of March 2026

## Project Summary

This capstone builds a local-first data engineering pipeline around the WFP Kenya food prices dataset. The repo covers both deliverable phases:

- **Month 1:** raw PostgreSQL setup, CSV load script, SQL exploration queries, basic Airflow orchestration, and an initial data issues review.
- **Month 2:** modular ETL, cleaned staging layer, warehouse star schema, dbt models, Airflow orchestration, dashboard assets, and presentation/demo support.

The dataset already exists in this repo as [`wfp_food_prices_ken.csv`](/Users/leonida/Documents/code/kenya_food_prices/wfp_food_prices_ken.csv), so extraction is treated as a local-file step by default.

## Dataset Snapshot

Based on the local CSV included in this repo:

- Rows: `18,837`
- Date range: `2006-01-15` to `2026-03-15`
- Markets: `226`
- Commodities: `51`
- Distinct units: `14`

## Architecture

```text
Local CSV
   |
   v
extract.py (local-first)
   |
   v
raw.raw_food_prices
   |
   v
clean.py -> quality.py
   |
   v
staging.stg_food_prices
   |
   v
warehouse.dim_date
warehouse.dim_market
warehouse.dim_commodity
warehouse.fact_prices
   |
   +--> dbt staging + marts
   +--> Grafana / Metabase
   +--> optional Snowflake mirror
```

## Repository Structure

```text
kenya_food_prices/
├── airflow/dags/kenya_food_prices_dag.py
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   └── models/
├── python/
│   ├── extract.py
│   ├── clean.py
│   ├── quality.py
│   ├── load.py
│   └── pipeline.py
├── sql/
│   ├── 01_setup.sql
│   ├── 02_analysis_queries.sql
│   ├── 03_staging_and_dims.sql
│   ├── 04_snowflake_queries.sql
│   └── init_postgres.sh
├── visuals/grafana_dashboard.json
├── docker-compose.yml
├── requirements.txt
└── wfp_food_prices_ken.csv
```

## Month 1 Deliverables

1. PostgreSQL setup script  
[`sql/01_setup.sql`](/Users/leonida/Documents/code/kenya_food_prices/sql/01_setup.sql)

2. Python CSV-to-PostgreSQL loading  
[`python/load.py`](/Users/leonida/Documents/code/kenya_food_prices/python/load.py)

3. SQL exploration queries  
[`sql/02_analysis_queries.sql`](/Users/leonida/Documents/code/kenya_food_prices/sql/02_analysis_queries.sql)

4. Basic Airflow DAG  
[`airflow/dags/kenya_food_prices_dag.py`](/Users/leonida/Documents/code/kenya_food_prices/airflow/dags/kenya_food_prices_dag.py)

5. README with observed data issues  
This file

## Month 2 Deliverables

1. Modular ETL scripts  
[`python/extract.py`](/Users/leonida/Documents/code/kenya_food_prices/python/extract.py)  
[`python/clean.py`](/Users/leonida/Documents/code/kenya_food_prices/python/clean.py)  
[`python/quality.py`](/Users/leonida/Documents/code/kenya_food_prices/python/quality.py)  
[`python/load.py`](/Users/leonida/Documents/code/kenya_food_prices/python/load.py)  
[`python/pipeline.py`](/Users/leonida/Documents/code/kenya_food_prices/python/pipeline.py)

2. Cleaned staging table plus dimensions and fact table  
[`sql/03_staging_and_dims.sql`](/Users/leonida/Documents/code/kenya_food_prices/sql/03_staging_and_dims.sql)

3. Load cleaned data to PostgreSQL  
Handled by [`python/pipeline.py`](/Users/leonida/Documents/code/kenya_food_prices/python/pipeline.py)

4. Optional Snowflake support  
[`sql/04_snowflake_queries.sql`](/Users/leonida/Documents/code/kenya_food_prices/sql/04_snowflake_queries.sql)

5. Dashboard asset  
[`visuals/grafana_dashboard.json`](/Users/leonida/Documents/code/kenya_food_prices/visuals/grafana_dashboard.json)

6. Updated Airflow DAG with cleaning and load steps  
[`airflow/dags/kenya_food_prices_dag.py`](/Users/leonida/Documents/code/kenya_food_prices/airflow/dags/kenya_food_prices_dag.py)

7. Incremental ETL logic  
Implemented in [`python/load.py`](/Users/leonida/Documents/code/kenya_food_prices/python/load.py)

8. Star schema in PostgreSQL  
Implemented in [`sql/03_staging_and_dims.sql`](/Users/leonida/Documents/code/kenya_food_prices/sql/03_staging_and_dims.sql)

9. dbt models for aggregates and trends  
[`dbt/models/staging/stg_food_prices.sql`](/Users/leonida/Documents/code/kenya_food_prices/dbt/models/staging/stg_food_prices.sql)  
[`dbt/models/marts/mart_monthly_avg_prices.sql`](/Users/leonida/Documents/code/kenya_food_prices/dbt/models/marts/mart_monthly_avg_prices.sql)  
[`dbt/models/marts/mart_maize_price_trends.sql`](/Users/leonida/Documents/code/kenya_food_prices/dbt/models/marts/mart_maize_price_trends.sql)

10. README with design choices and assumptions  
This file

11. Presentation/demo support  
Use the sections below together with the Airflow DAG, SQL scripts, dbt models, and Grafana asset.

12. Repo-ready structure  
Added `.env.example` and `.gitignore` for a cleaner handoff.

## How To Run

### Option 1: Local Python run

```bash
pip install -r requirements.txt

export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=kenya_food_prices
export PG_USER=kenya
export PG_PASSWORD=kenya123
export LOCAL_CSV_PATH=/Users/leonida/Documents/code/kenya_food_prices/wfp_food_prices_ken.csv

python python/pipeline.py --local wfp_food_prices_ken.csv --skip-snowflake
```

### Option 2: Docker stack

```bash
docker compose up -d
psql -h localhost -U kenya -d kenya_food_prices -f sql/01_setup.sql -f sql/03_staging_and_dims.sql
```

Airflow will use the mounted local CSV at `/opt/airflow/data/wfp_food_prices_ken.csv`.

### Run dbt

```bash
cd dbt
dbt deps
dbt run --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
```

## Data Issues Observed

The current local CSV is cleaner than many public data files, but a few real issues still matter for analysis:

1. **Missing county and district values**
There are `63` rows where `admin1` and `admin2` are null. The cleaning layer preserves the rows and lets downstream models fall back to `"Unknown"` where needed.

2. **Unit inconsistency across the same commodity**
The dataset mixes units like `KG`, `90 KG`, `50 KG`, `200 G`, `500 ML`, `L`, `Head`, and `Bunch`. The cleaning layer derives `price_per_kg` only where the conversion is reliable.

3. **Mixed measurement families**
Some commodities are sold by weight, others by volume or count. This means direct price comparisons are not always meaningful, so the warehouse keeps original `unit` values and avoids forcing unsafe conversions.

## Design Choices And Assumptions

- The capstone is **local-first** because the CSV is already available and the brief says extraction does not need to be redone.
- The raw table preserves the source CSV columns closely, including source IDs and coordinates, so the project can show clear lineage from raw data to warehouse.
- The warehouse dimensions use business keys that include more than just a name:
  market uniqueness uses `market + district + county`, and commodity uniqueness uses `commodity + unit`.
- Incremental loading is based on natural business keys in staging rather than only a max date watermark, which avoids missing same-date rows on reruns.
- Snowflake remains optional so the project can be demonstrated fully with PostgreSQL even if cloud credentials are unavailable.

## Suggested Demo Flow

1. Explain the Kenya food prices problem and why monthly market data matters.
2. Show the raw CSV and the raw table schema.
3. Run `sql/02_analysis_queries.sql` to demonstrate SQL exploration.
4. Trigger the Airflow DAG or run [`python/pipeline.py`](/Users/leonida/Documents/code/kenya_food_prices/python/pipeline.py).
5. Show the star schema and dbt marts.
6. Open Grafana or Metabase and walk through the maize trend and average price visuals.

## Presentation Outline

1. Problem context and business value
2. Pipeline architecture
3. SQL exploration from month 1
4. ETL, quality checks, and incremental logic from month 2
5. dbt and dashboard outputs
6. Challenges, lessons learned, and future improvements

## Future Improvements

- Add a stronger unit-conversion framework for non-kg products.
- Add warehouse-level SQL tests for duplicate fact rows and null foreign keys.
- Standardize dashboard provisioning for Grafana and Metabase.
- Extend Snowflake support into a full dual-target dbt deployment.
