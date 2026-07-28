"""
Slipstream ELT pipeline.

Replaces the original loadStg.sh, which chained six TPT loads and then
invoked BTEQ for the staging-to-transformation step.

dbt and Airflow have incompatible dependency pins, so dbt runs from its
own virtualenv, invoked by absolute path.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/mnt/c/Users/rohit/OneDrive/Desktop/slipstream-elt-modernization"
DBT_DIR = f"{PROJECT_DIR}/slipstream_dbt"
DBT_VENV = "/home/rohit/dbt-venv/bin"

default_args = {
    "owner": "rohit",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="slipstream_pipeline",
    description="CSV ingestion into DuckDB, dbt transformations, data tests",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",          # daily at 02:00
    catchup=False,
    tags=["slipstream", "elt"],
) as dag:

    # ingest.py moves files out of data/raw on success, so each run
    # re-stages them from the version-controlled source directory.
    stage_source_files = BashOperator(
        task_id="stage_source_files",
        bash_command=f"cp {PROJECT_DIR}/datasources/*.csv {PROJECT_DIR}/data/raw/",
    )

    ingest = BashOperator(
        task_id="ingest_csv_to_duckdb",
        bash_command=f"{DBT_VENV}/python {PROJECT_DIR}/ingestion/ingest.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT_VENV}/dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_VENV}/dbt test",
    )

    stage_source_files >> ingest >> dbt_run >> dbt_test
