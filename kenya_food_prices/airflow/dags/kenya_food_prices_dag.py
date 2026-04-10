"""
Kenya Food Prices Data Engineering Capstone
Author: Rene Bosire | Everything Data Bootcamp
File: airflow/dags/kenya_food_prices_dag.py

Local-first production DAG for the capstone:
  1. resolve_source       — Resolve the local CSV path
  2. load_raw             — Persist raw file to PostgreSQL
  3. clean_data           — pandas cleaning + enrichment
  4. quality_checks       — Raise on CRITICAL failures
  5. load_staging         — Incremental load to staging
  6. populate_dimensions  — Upsert warehouse dimensions
  7. load_fact            — Populate warehouse.fact_prices
  8. load_snowflake       — Mirror to Snowflake (optional)
  9. dbt_run              — Run dbt models
 10. dbt_test             — Run dbt tests
 11. notify_success       — Log pipeline summary
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Ensure our Python modules are on the path
sys.path.insert(0, str(Path(__file__).parents[1] / "python"))

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "rene.bosire",
    "depends_on_past":  False,
    "email":            ["rene.bosire@example.com"],
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kenya_food_prices_etl",
    description="Monthly ETL pipeline for WFP Kenya food price data",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 6 1 * *",   # 06:00 on the 1st of every month
    catchup=False,
    tags=["capstone", "food-prices", "kenya", "etl"],
    doc_md="""
## Kenya Food Prices ETL
End-to-end pipeline from WFP raw CSV → PostgreSQL star schema → Snowflake mirror → dbt models.

**Author:** Rene Bosire  
**Bootcamp:** Everything Data — Data Engineering  
    """,
) as dag:

    DATASET_PATH = os.getenv("LOCAL_CSV_PATH", "/opt/airflow/data/wfp_food_prices_ken.csv")

    # ── Task 1: Resolve source ─────────────────────────────────
    def task_resolve_source(**ctx):
        path = Path(DATASET_PATH)
        if not path.exists():
            raise FileNotFoundError(f"Local CSV not found: {path}")
        ctx["ti"].xcom_push(key="source_csv", value=str(path))
        log.info("Using local dataset: %s", path)

    t_resolve_source = PythonOperator(
        task_id="resolve_source",
        python_callable=task_resolve_source,
    )

    # ── Task 2: Load raw ───────────────────────────────────────
    def task_load_raw(**ctx):
        import pandas as pd
        from load import load_raw
        path = ctx["ti"].xcom_pull(key="source_csv", task_ids="resolve_source")
        df = pd.read_csv(path, low_memory=False)
        df.attrs["source_name"] = path
        load_raw(df)

    t_load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=task_load_raw,
    )

    # ── Task 3: Clean ──────────────────────────────────────────
    def task_clean(**ctx):
        import pandas as pd
        from clean import clean
        path = ctx["ti"].xcom_pull(key="source_csv", task_ids="resolve_source")
        raw_df = pd.read_csv(path, low_memory=False)
        clean_df = clean(raw_df)
        clean_df["source_file"] = Path(path).name
        fd, tmp = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        clean_df.to_csv(tmp, index=False)
        ctx["ti"].xcom_push(key="clean_csv", value=tmp)
        log.info("Cleaned data: %d rows → %s", len(clean_df), tmp)

    t_clean = PythonOperator(
        task_id="clean_data",
        python_callable=task_clean,
    )

    # ── Task 4: Quality checks ─────────────────────────────────
    def task_quality(**ctx):
        import pandas as pd
        from quality import run_checks
        path = ctx["ti"].xcom_pull(key="clean_csv", task_ids="clean_data")
        df = pd.read_csv(path, low_memory=False)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        run_checks(df)  # raises QualityError on critical failures
        return True     # ShortCircuit — proceed

    t_quality = ShortCircuitOperator(
        task_id="quality_checks",
        python_callable=task_quality,
    )

    # ── Task 5: Load staging ───────────────────────────────────
    def task_load_staging(**ctx):
        import pandas as pd
        from load import load_staging
        path = ctx["ti"].xcom_pull(key="clean_csv", task_ids="clean_data")
        df = pd.read_csv(path, low_memory=False)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        n = load_staging(df, incremental=True)
        log.info("Loaded %d new rows to staging.", n)

    t_load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=task_load_staging,
    )

    # ── Task 6: Populate dimensions ────────────────────────────
    def task_dims(**ctx):
        from load import populate_dimensions
        populate_dimensions()

    t_dims = PythonOperator(
        task_id="populate_dimensions",
        python_callable=task_dims,
    )

    # ── Task 7: Load fact table ────────────────────────────────
    def task_fact(**ctx):
        from load import load_fact_table
        load_fact_table()

    t_fact = PythonOperator(
        task_id="load_fact_table",
        python_callable=task_fact,
    )

    # ── Task 8: Snowflake mirror ───────────────────────────────
    def task_snowflake(**ctx):
        import pandas as pd
        from load import load_snowflake
        path = ctx["ti"].xcom_pull(key="clean_csv", task_ids="clean_data")
        df = pd.read_csv(path, low_memory=False)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        try:
            load_snowflake(df)
        except Exception as exc:
            log.warning("Snowflake load skipped (non-critical): %s", exc)

    t_snowflake = PythonOperator(
        task_id="load_snowflake",
        python_callable=task_snowflake,
    )

    # ── Task 9: dbt run ────────────────────────────────────────
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "if command -v dbt >/dev/null 2>&1; then "
            "dbt deps && dbt run --profiles-dir . --target dev; "
            "else echo 'dbt CLI not installed in Airflow container; skipping dbt_run.'; fi"
        ),
    )

    # ── Task 10: dbt test ──────────────────────────────────────
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "if command -v dbt >/dev/null 2>&1; then "
            "dbt test --profiles-dir . --target dev; "
            "else echo 'dbt CLI not installed in Airflow container; skipping dbt_test.'; fi"
        ),
    )

    # ── Task 11: Notify ────────────────────────────────────────
    def task_notify(**ctx):
        log.info("=" * 55)
        log.info("✅  Kenya Food Prices ETL pipeline completed successfully.")
        log.info("    DAG run: %s", ctx["run_id"])
        log.info("=" * 55)

    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify,
    )

    # ── Dependency graph ───────────────────────────────────────
    (
        t_resolve_source
        >> t_load_raw
        >> t_clean
        >> t_quality
        >> t_load_staging
        >> t_dims
        >> t_fact
        >> t_snowflake
        >> t_dbt_run
        >> t_dbt_test
        >> t_notify
    )
