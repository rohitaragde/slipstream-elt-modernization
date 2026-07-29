"""
Slipstream ELT pipeline.

Replaces the original loadStg.sh, which chained six TPT loads and then
invoked BTEQ for the staging-to-transformation step.

dbt and Airflow have incompatible dependency pins, so dbt runs from its
own virtualenv, invoked by absolute path.

The final task is a data quality gate: it reconciles source, staging and
model row counts, checks dbt test results, and posts the full report to
Slack. A mismatch fails the task.
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/mnt/c/Users/rohit/OneDrive/Desktop/slipstream-elt-modernization"
DBT_DIR = f"{PROJECT_DIR}/slipstream_dbt"
DBT_VENV = "/home/rohit/dbt-venv/bin"


def notify_failure(context):
    """Alert on any task failure. Never raises - alerting must not fail the run."""
    try:
        webhook = Variable.get("slack_webhook_url", default_var=None)
        if not webhook:
            return
        ti = context["task_instance"]
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":red_circle: *Slipstream pipeline failed*\n"
                            f"*Task:* `{ti.task_id}`\n"
                            f"*Run:* {context['dag_run'].run_id}\n"
                            f"*Attempt:* {ti.try_number}",
                },
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"<{ti.log_url}|View logs>"}],
            },
        ]
        requests.post(
            webhook,
            json={"text": f"Slipstream failed: {ti.task_id}", "blocks": blocks},
            timeout=10,
        )
    except Exception:
        pass


default_args = {
    "owner": "rohit",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="slipstream_pipeline",
    description="CSV ingestion into DuckDB, dbt transformations, data quality gate",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["slipstream", "elt"],
) as dag:

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

    quality_report = BashOperator(
        task_id="data_quality_report",
        bash_command=f"{DBT_VENV}/python {PROJECT_DIR}/quality/report.py",
        env={"SLACK_WEBHOOK_URL": "{{ var.value.slack_webhook_url }}"},
        append_env=True,
    )

    stage_source_files >> ingest >> dbt_run >> dbt_test >> quality_report
