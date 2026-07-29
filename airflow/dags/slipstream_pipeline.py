"""
Slipstream ELT pipeline.

Replaces the original loadStg.sh, which chained six TPT loads and then
invoked BTEQ for the staging-to-transformation step.

dbt and Airflow have incompatible dependency pins, so dbt runs from its
own virtualenv, invoked by absolute path.

The final task is a data quality gate: it reconciles source, staging and
model row counts, checks dbt test results, and posts the full report to
Slack. A mismatch fails the task.

On any task failure, the Slack alert includes the actual error lines
pulled from the project's own logs, rather than only a link into the
Airflow UI - so someone unfamiliar with Airflow can see what broke.
"""

import re
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/mnt/c/Users/rohit/OneDrive/Desktop/slipstream-elt-modernization"
DBT_DIR = f"{PROJECT_DIR}/slipstream_dbt"
DBT_VENV = "/home/rohit/dbt-venv/bin"


def recent_errors(max_lines=12):
    """Pull the actual error lines from project logs, newest last."""
    candidates = []
    log_dir = Path(PROJECT_DIR) / "logs"
    if log_dir.exists():
        candidates += sorted(log_dir.glob("ingest_*.log"), reverse=True)[:1]
    dbt_log = Path(PROJECT_DIR) / "slipstream_dbt" / "logs" / "dbt.log"
    if dbt_log.exists():
        candidates.append(dbt_log)

    pattern = re.compile(r"error|fail|exception|traceback", re.I)
    found = []
    for path in candidates:
        try:
            lines = path.read_text(errors="replace").splitlines()
            hits = [ln.strip() for ln in lines if pattern.search(ln)]
            if hits:
                found.append(f"--- {path.name} ---")
                found += hits[-max_lines:]
        except Exception:
            continue
    return found


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
        ]

        errors = recent_errors()
        if errors:
            excerpt = "\n".join(errors)[:2800]  # Slack block text limit
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*What went wrong*\n```{excerpt}```",
                    },
                }
            )

        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"<{ti.log_url}|View full logs in Airflow>"}],
            }
        )

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

    # retries=0: ingest.py MOVES each source file to data/archive/ as soon as
    # it loads successfully. If it later fails on a different file, a retry
    # of this task finds data/raw empty - the prior attempt already consumed
    # everything that worked - so every file fails on retry, including ones
    # that were fine. Retrying isn't safe here; fail fast instead.
    ingest = BashOperator(
        task_id="ingest_csv_to_duckdb",
        bash_command=f"{DBT_VENV}/python {PROJECT_DIR}/ingestion/ingest.py",
        retries=0,
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
