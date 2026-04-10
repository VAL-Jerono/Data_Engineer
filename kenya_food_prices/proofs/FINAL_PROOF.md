# Kenya Food Prices Capstone Proof

## Summary

This document is the submission proof bundle for the Kenya Food Prices data engineering capstone. It records the successful execution evidence for the Airflow pipeline, dbt models, and the supporting Grafana dashboard.

## Completed Deliverables

### Month 1

- PostgreSQL raw setup script implemented in [sql/01_setup.sql](../sql/01_setup.sql).
- CSV-to-PostgreSQL loading implemented in [python/load.py](../python/load.py).
- SQL exploration queries included in [sql/02_analysis_queries.sql](../sql/02_analysis_queries.sql).
- Airflow orchestration implemented in [airflow/dags/kenya_food_prices_dag.py](../airflow/dags/kenya_food_prices_dag.py).
- Observed data issues documented in [README.md](../README.md).

### Month 2

- Modular ETL implemented in [python/extract.py](../python/extract.py), [python/clean.py](../python/clean.py), [python/quality.py](../python/quality.py), [python/load.py](../python/load.py), and [python/pipeline.py](../python/pipeline.py).
- Clean staging and warehouse schema implemented in [sql/03_staging_and_dims.sql](../sql/03_staging_and_dims.sql).
- Optional Snowflake script included in [sql/04_snowflake_queries.sql](../sql/04_snowflake_queries.sql).
- dbt models and tests implemented in [dbt/models](../dbt/models).
- Grafana dashboard asset included in [visuals/grafana_dashboard.json](../visuals/grafana_dashboard.json).

## Successful Execution Proof

### Airflow

- DAG: `kenya_food_prices_etl`
- Final run status: successful
- Evidence: screenshot from the Airflow UI saved by the user
- Supporting log path: [notify_success_log_path.txt](notify_success_log_path.txt)
- Supporting server logs: [airflow_webserver.log](airflow_webserver.log) and [airflow_scheduler.log](airflow_scheduler.log)

### dbt

- `dbt deps`: successful
- `dbt run --profiles-dir . --target dev`: successful
- `dbt test --profiles-dir . --target dev`: successful
- Evidence files: [dbt_deps.txt](dbt_deps.txt), [dbt_run.txt](dbt_run.txt), [dbt_test.txt](dbt_test.txt)

### Grafana

- Grafana login: successful
- Datasource: PostgreSQL connection fixed to the Docker service name
- Dashboard panels: populated with data
- Evidence: screenshot from the Grafana UI saved by the user

## Screenshot Placeholders

Save the final screenshots here so they can be attached cleanly to the submission bundle:

- proofs/screenshots/airflow_success.png
- proofs/screenshots/grafana_dashboard.png
- proofs/screenshots/dbt_run.png
- proofs/screenshots/dbt_test.png

## Notes

- The `dbt` proof files show a clean run and test cycle with 3 models and 20 data tests passing.
- Airflow was adjusted so optional Snowflake and dbt-in-container failures do not block the core PostgreSQL ETL path.
- Grafana was configured to use the Docker service name `postgres` instead of `localhost`.
