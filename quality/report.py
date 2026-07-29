"""
Data quality report and gate.

Four checks, one report:

  1. Row-count reconciliation - every source row reached the transformed tables
  2. dbt test results          - parsed from the last dbt invocation
  3. Volume delta              - row counts compared against the previous run
  4. Business metrics          - billable totals the pipeline exists to produce

Reconciliation failures and failing data tests FAIL the task, because both
always indicate a defect. Volume changes only WARN: a swing may be legitimate,
and failing a pipeline on legitimate change teaches people to ignore alerts.

Each run is recorded to history tables in DuckDB, which is what makes volume
comparison possible and gives the dashboard something to trend.

On failure the report includes log locations and a grep command, so whoever
receives the alert does not need to know Airflow to start investigating.

Counting note: `wc -l` counts newline characters, so a file whose final line
lacks a trailing newline is undercounted. Two source files here are written
that way. csv.reader counts records and is correct for both.
"""

import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_DIR = BASE_DIR / "datasources"
REJECT_DIR = BASE_DIR / "data" / "reject"
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "slipstream.duckdb"
DBT_LOG = "slipstream_dbt/logs/dbt.log"
DBT_RESULTS = BASE_DIR / "slipstream_dbt" / "target" / "run_results.json"
AIRFLOW_LOG = "~/airflow/logs/dag_id=slipstream_pipeline/"

WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

# Row-count change beyond this fraction is surfaced as a warning.
VOLUME_TOLERANCE = 0.20

# Source file -> staging table. Mirrors ingestion/ingest.py; the two fact
# tables are each fed by three run files.
FILE_TO_TABLE = {
    "Cust_Prfl.csv": "stg_cust_profile",
    "Cust_Stats1.csv": "stg_cust_status",
    "Loyalty_file.csv": "stg_loyalty",
    "Plan_Master.csv": "stg_plan_master",
    "Data_Plan_Run_1.csv": "stg_data_plan",
    "Data_Plan_Run_2.csv": "stg_data_plan",
    "Data_Plan_Run_3.csv": "stg_data_plan",
    "Voice_Plan_Run_1.csv": "stg_voice_plan",
    "Voice_Plan_Run_2.csv": "stg_voice_plan",
    "Voice_Plan_Run_3.csv": "stg_voice_plan",
}

STAGING_TO_MODEL = {
    "stg_cust_profile": "trn_cust_profile",
    "stg_cust_status": "trn_cust_status",
    "stg_loyalty": "trn_loyalty",
    "stg_plan_master": "trn_plan_master",
    "stg_data_plan": "trn_data_plan",
    "stg_voice_plan": "trn_voice_plan",
}

LABELS = {
    "stg_cust_profile": "Customer profile",
    "stg_cust_status": "Customer status",
    "stg_loyalty": "Loyalty",
    "stg_plan_master": "Plan master",
    "stg_data_plan": "Data usage",
    "stg_voice_plan": "Voice usage",
}


# ---------------------------------------------------------------- counting


def count_csv_records(path):
    """Data rows in a CSV, excluding the header."""
    with open(path, newline="", encoding="latin-1") as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)


def source_counts():
    """Expected rows per staging table, summed across its source files."""
    counts = {}
    for filename, table in FILE_TO_TABLE.items():
        path = SOURCE_DIR / filename
        if path.exists():
            counts[table] = counts.get(table, 0) + count_csv_records(path)
    return counts


def rejected_rows():
    """Rows quarantined during ingestion, keyed by file."""
    if not REJECT_DIR.exists():
        return {}
    return {
        p.name: count_csv_records(p)
        for p in sorted(REJECT_DIR.glob("reject_*.csv"))
    }


# ---------------------------------------------------------------- history


def ensure_history(con):
    """History tables are created on first run and appended thereafter."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quality_row_history (
            run_ts     TIMESTAMP,
            table_name VARCHAR,
            row_count  BIGINT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quality_metric_history (
            run_ts TIMESTAMP,
            metric VARCHAR,
            value  DOUBLE
        )
        """
    )


def previous_counts(con):
    """Row counts from the most recent prior run. Empty on first run."""
    rows = con.execute(
        """
        SELECT table_name, row_count
        FROM quality_row_history
        WHERE run_ts = (SELECT MAX(run_ts) FROM quality_row_history)
        """
    ).fetchall()
    return dict(rows)


def run_count(con):
    """How many runs have been recorded, including this one."""
    return con.execute(
        "SELECT COUNT(DISTINCT run_ts) FROM quality_row_history"
    ).fetchone()[0]


def record_run(con, run_ts, counts, metrics):
    con.executemany(
        "INSERT INTO quality_row_history VALUES (?, ?, ?)",
        [(run_ts, table, n) for table, n in counts.items()],
    )
    con.executemany(
        "INSERT INTO quality_metric_history VALUES (?, ?, ?)",
        [(run_ts, name, float(v)) for name, v in metrics.items()],
    )


# ---------------------------------------------------------------- checks


def reconcile(con, expected):
    """Compare source, staging and model counts. Returns (rows, failures)."""
    rows, failures = [], []

    def count(table):
        try:
            return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            return None

    for staging, model in STAGING_TO_MODEL.items():
        want = expected.get(staging)
        got_staging, got_model = count(staging), count(model)
        label = LABELS.get(staging, staging)

        if None in (want, got_staging, got_model):
            status = "MISSING"
            failures.append(f"{label} — table or source file not found")
        elif want == got_staging == got_model:
            status = "OK"
        else:
            status = "MISMATCH"
            failures.append(
                f"{label} — source {want:,} · staging {got_staging:,} "
                f"· model {got_model:,}"
            )

        rows.append((staging, want, got_staging, got_model, status))

    return rows, failures


def volume_deltas(rows, previous):
    """Row-count movement since the last run. Returns (deltas, warnings)."""
    deltas, warnings = [], []

    for staging, _want, _staging_n, model_n, _status in rows:
        prior = previous.get(staging)
        if model_n is None:
            continue
        if prior is None:
            deltas.append((staging, model_n, None, None))
            continue

        change = (model_n - prior) / prior if prior else 0.0
        deltas.append((staging, model_n, prior, change))

        if abs(change) > VOLUME_TOLERANCE:
            direction = "up" if change > 0 else "down"
            warnings.append(
                f"{LABELS.get(staging, staging)} — {direction} "
                f"{abs(change) * 100:.0f}% ({prior:,} → {model_n:,})"
            )

    return deltas, warnings


def business_metrics(con):
    """The numbers the pipeline exists to produce."""

    def scalar(sql, default=0.0):
        try:
            v = con.execute(sql).fetchone()[0]
            return float(v) if v is not None else default
        except Exception:
            return default

    data_rev = scalar("SELECT SUM(billable_data_amt) FROM trn_data_plan")
    voice_rev = scalar("SELECT SUM(billable_voice_amt) FROM trn_voice_plan")
    sms_rev = scalar("SELECT SUM(billable_sms_amt) FROM trn_voice_plan")
    customers = scalar("SELECT COUNT(*) FROM trn_cust_profile")

    return {
        "billable_data": data_rev,
        "billable_voice": voice_rev,
        "billable_sms": sms_rev,
        "billable_total": data_rev + voice_rev + sms_rev,
        "customers": customers,
    }


def dbt_test_results():
    """Parse the last dbt invocation. Returns (passed, failed, failed_names)."""
    if not DBT_RESULTS.exists():
        return None, None, []

    with open(DBT_RESULTS) as f:
        results = json.load(f).get("results", [])

    tests = [r for r in results if r.get("unique_id", "").startswith("test.")]
    if not tests:
        return None, None, []

    failed = [
        r["unique_id"].split(".")[2]
        for r in tests
        if r.get("status") not in ("pass", "success")
    ]
    return len(tests) - len(failed), len(failed), failed


# ---------------------------------------------------------------- logs


def latest_ingest_log():
    """Filename of the most recent ingestion log, if any."""
    if not LOG_DIR.exists():
        return None
    logs = sorted(LOG_DIR.glob("ingest_*.log"), reverse=True)
    return logs[0].name if logs else None


def log_locations():
    """Where to look when something fails, as (label, path) pairs."""
    ingest = latest_ingest_log()
    return [
        ("Ingestion", f"logs/{ingest}" if ingest else "logs/ (none yet)"),
        ("dbt", DBT_LOG),
        ("Airflow tasks", AIRFLOW_LOG),
    ]


GREP_HINT = "grep -inE 'error|fail|reject' logs/ingest_*.log " + DBT_LOG


# ---------------------------------------------------------------- reporting


def money(v):
    return f"${v:,.0f}"


def build_blocks(state):
    """Slack Block Kit payload."""
    ok = state["ok"]
    rows = state["rows"]
    metrics = state["metrics"]
    total_rows = sum(r[3] for r in rows if r[3] is not None)
    tables_ok = sum(1 for r in rows if r[4] == "OK")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Data quality passed" if ok else "Data quality FAILED",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":white_check_mark: Every source row reached the transformed "
                    "tables and all data tests passed."
                    if ok
                    else ":rotating_light: The pipeline produced data that does not "
                    "reconcile with its source. Details below."
                ),
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Rows reconciled*\n{total_rows:,}"},
                {"type": "mrkdwn", "text": f"*Tables*\n{tables_ok} of {len(rows)}"},
                {
                    "type": "mrkdwn",
                    "text": "*Data tests*\n"
                    + (
                        f"{state['tests_passed']} passed"
                        if not state["tests_failed"]
                        else f"{state['tests_failed']} failed, "
                        f"{state['tests_passed']} passed"
                    ),
                },
                {"type": "mrkdwn", "text": f"*Pipeline run*\n#{state['run_no']}"},
            ],
        },
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Billing output*"}},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Total billable*\n{money(metrics['billable_total'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Customers*\n{int(metrics['customers']):,}",
                },
                {"type": "mrkdwn", "text": f"*Data*\n{money(metrics['billable_data'])}"},
                {
                    "type": "mrkdwn",
                    "text": "*Voice + SMS*\n"
                    + money(metrics["billable_voice"] + metrics["billable_sms"]),
                },
            ],
        },
    ]

    if state["warnings"]:
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":warning: *Volume changed since the last run*\n"
                    + "\n".join(f"• {w}" for w in state["warnings"])
                    + "\n_Counts reconcile, so this is a change in the source "
                    "data rather than a pipeline defect._",
                },
            }
        )

    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Source → staging → model*"},
        }
    )

    fields = []
    delta_by_table = {d[0]: d for d in state["deltas"]}
    for staging, want, _staging_n, model_n, status in rows:
        mark = ":small_blue_diamond:" if status == "OK" else ":small_red_triangle:"
        label = LABELS.get(staging, staging)
        value = f"{model_n:,} rows" if model_n is not None else "not found"

        d = delta_by_table.get(staging)
        if d and d[3] is not None and abs(d[3]) > 0.0001:
            value += f"  ({d[3] * 100:+.0f}%)"
        if status != "OK" and want is not None:
            value += f"  source {want:,}"

        fields.append({"type": "mrkdwn", "text": f"{mark} *{label}*\n{value}"})

    for i in range(0, len(fields), 10):  # Slack caps fields at 10 per section
        blocks.append({"type": "section", "fields": fields[i : i + 10]})

    if state["failures"]:
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*What did not reconcile*\n"
                    + "\n".join(f"• {f}" for f in state["failures"]),
                },
            }
        )

    if state["tests_failed"]:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Failing data tests*\n"
                    + "\n".join(f"• `{n}`" for n in state["failed_names"][:10]),
                },
            }
        )

    if state["rejects"]:
        total = sum(state["rejects"].values())
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":warning: *{total:,} rows quarantined* during "
                    "ingestion (excluded from staging by design)",
                },
            }
        )

    # Troubleshooting paths only when something is wrong - a green report
    # does not need them.
    if not ok:
        blocks.append({"type": "divider"})
        paths = "\n".join(f"• *{label}*  `{path}`" for label, path in log_locations())
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Where to look*\n{paths}\n\n```{GREP_HINT}```",
                },
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Slipstream ELT  ·  run #{state['run_no']}  ·  "
                    f"{state['run_ts']:%d %b %Y, %H:%M}",
                }
            ],
        }
    )
    return blocks


def post_to_slack(state):
    """Post the report. Never raises - alerting must not fail the pipeline."""
    if not WEBHOOK:
        return
    try:
        requests.post(
            WEBHOOK,
            json={
                "text": "Slipstream — data quality "
                + ("passed" if state["ok"] else "FAILED"),
                "blocks": build_blocks(state),
            },
            timeout=10,
        )
    except Exception as e:
        print(f"Slack notification failed (continuing): {e}")


def print_report(state):
    rows = state["rows"]
    metrics = state["metrics"]
    delta_by_table = {d[0]: d for d in state["deltas"]}

    print("=" * 72)
    print(
        f"DATA QUALITY REPORT  ·  run #{state['run_no']}  ·  "
        f"{state['run_ts']:%Y-%m-%d %H:%M}"
    )
    print("=" * 72)

    print(
        f"\n{'table':<16}{'source':>8}{'staging':>10}{'model':>10}"
        f"{'vs prev':>10}   status"
    )
    print("-" * 72)
    for staging, want, staging_n, model_n, status in rows:
        fmt = lambda v: "-" if v is None else f"{v:,}"
        d = delta_by_table.get(staging)
        delta = "baseline" if not d or d[3] is None else f"{d[3] * 100:+.0f}%"
        print(
            f"{staging.replace('stg_', ''):<16}{fmt(want):>8}{fmt(staging_n):>10}"
            f"{fmt(model_n):>10}{delta:>10}   {status}"
        )

    print("\nBilling output")
    print(f"  total billable   {money(metrics['billable_total'])}")
    print(f"  data             {money(metrics['billable_data'])}")
    print(f"  voice            {money(metrics['billable_voice'])}")
    print(f"  sms              {money(metrics['billable_sms'])}")
    print(f"  customers        {int(metrics['customers']):,}")

    if state["tests_passed"] is not None:
        print(
            f"\ndbt tests: {state['tests_passed']} passed, "
            f"{state['tests_failed']} failed"
        )
        for n in state["failed_names"]:
            print(f"  FAILED: {n}")

    if state["warnings"]:
        print("\nVolume warnings:")
        for w in state["warnings"]:
            print(f"  {w}")

    if state["rejects"]:
        print(f"\nRejected rows: {sum(state['rejects'].values()):,}")
        for name, n in state["rejects"].items():
            print(f"  {name}: {n:,}")

    if not state["ok"]:
        print("\nWhere to look")
        for label, path in log_locations():
            print(f"  {label:<16}{path}")
        print(f"\n  {GREP_HINT}")


def main():
    run_ts = datetime.now()
    con = duckdb.connect(str(DB_PATH))
    ensure_history(con)

    expected = source_counts()
    rows, failures = reconcile(con, expected)
    previous = previous_counts(con)
    deltas, warnings = volume_deltas(rows, previous)
    metrics = business_metrics(con)
    tests_passed, tests_failed, failed_names = dbt_test_results()
    rejects = rejected_rows()

    counts_now = {r[0]: r[3] for r in rows if r[3] is not None}
    record_run(con, run_ts, counts_now, metrics)
    run_no = run_count(con)
    con.close()

    state = {
        "ok": not failures and not tests_failed,
        "rows": rows,
        "failures": failures,
        "deltas": deltas,
        "warnings": warnings,
        "metrics": metrics,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "failed_names": failed_names,
        "rejects": rejects,
        "run_ts": run_ts,
        "run_no": run_no,
    }

    print_report(state)
    post_to_slack(state)

    print()
    if not state["ok"]:
        print("GATE FAILED — pipeline output is not trustworthy.")
        print("=" * 72)
        sys.exit(1)

    print("GATE PASSED — counts reconcile and all data tests passed.")
    print("=" * 72)


if __name__ == "__main__":
    main()
