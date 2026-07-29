# ⚡ Slipstream — Legacy Teradata ELT, Modernized

A legacy Teradata billing pipeline rebuilt end to end on a modern, open-source data stack: **Python + DuckDB** for ingestion, **dbt** for transformations, **Airflow** for orchestration, a **data-quality gate** with Slack alerting, and a **live Streamlit dashboard** on top.

**🔗 Live dashboard:** [slipstream-analytics.streamlit.app](https://slipstream-analytics.streamlit.app/)

---

## Why this project

The original system was a Teradata ELT stack: BTEQ scripts calling stored procedures, shell loaders, and a formatted-VARCHAR output feeding a Tableau report. It worked, but it was proprietary, hard to test, and opaque about data quality. Slipstream reproduces the same billing logic on a stack that is free to run, fully version-controlled, testable at every layer, and honest about the state of its own data.

The goal wasn't just "make it run on new tools." It was to make every transformation *explainable* — to know what each dataset is, what question it answers, and what's wrong with it — before writing a line of SQL.

---

## Architecture

```mermaid
flowchart LR
    A[10 source CSVs] -->|ingest.py| B[(DuckDB<br/>6 staging tables)]
    B -->|dbt| C[(6 trn_ models<br/>transformed + tested)]
    C --> D[Data-quality gate<br/>quality/report.py]
    D --> E[Streamlit dashboard]
    D -->|Block Kit| F[Slack alerts]

    subgraph Airflow DAG · daily 02:00
        A2[stage_source_files] --> A3[ingest_csv_to_duckdb] --> A4[dbt_run] --> A5[dbt_test] --> A6[data_quality_report]
    end
```

**Flow:** raw CSVs → DuckDB staging → dbt transforms (with tests) → data-quality gate → dashboard, with Airflow orchestrating the chain nightly and Slack carrying failures and completion reports.

---

## Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python + DuckDB | Fast, embedded, zero-server analytical DB; raw CSV → typed staging tables |
| Transformation | dbt (dbt-duckdb) | Version-controlled SQL models, built-in testing, lineage docs |
| Orchestration | Airflow | Schedules and chains the pipeline; callbacks for alerting |
| Testing | dbt tests + pytest | Data-quality tests on models; unit tests on ingestion |
| Alerting | Slack incoming webhook | Failure + completion reports, no extra provider dependency |
| Dashboard | Streamlit + Plotly | Reads DuckDB directly; deployed free on Streamlit Cloud |

---

## Project structure

```
slipstream-elt-modernization/
├── ingestion/          # ingest.py — CSVs → DuckDB staging tables
├── slipstream_dbt/     # dbt project: 6 trn_ models + tests + sources
│   └── models/staging/ # trn_cust_profile, trn_cust_status, trn_loyalty,
│                       # trn_plan_master, trn_data_plan, trn_voice_plan
├── airflow/dags/       # slipstream_pipeline.py — the orchestration DAG
├── quality/            # report.py — the data-quality gate
├── dashboard/          # app.py (Streamlit) + slim dashboard.duckdb
├── datasources/        # Data Mapping.xlsx, Test_Cases.xlsx (source spec)
├── .streamlit/         # config.toml — pins the dark theme
└── requirements.txt
```

---

## The pipeline, layer by layer

### 1. Ingestion (`ingestion/ingest.py`)

Loads 10 source CSVs into 6 DuckDB staging tables. Built with a portable `BASE_DIR` (derived from `__file__`) so it runs anywhere, and — importantly — it **fails loudly**: an early version logged per-file errors and still exited 0, so Airflow would mark ingestion green while data was silently missing. It now collects failed filenames and `sys.exit(1)`, so a missing file stops the pipeline instead of quietly corrupting everything downstream.

### 2. Transformation (`slipstream_dbt/`)

Six dbt models rebuild the Teradata `STG_* → TRN_*` logic on `dbt-duckdb`. The full column-level mapping — every `STG_*` column, its transform rule, and its `TRN_*` target — was reconstructed from `datasources/Data Mapping.xlsx` and validated against `Test_Cases.xlsx`, not guessed. The original `.bteq` file was just a pointer to a stored procedure, so the logic had to be rebuilt from the spec rather than translated line-for-line.

**Sources & lineage.** All six staging tables are declared in `models/staging/sources.yml` (`source('slipstream', 'stg_…')`), and every model's `FROM` clause references them through `{{ source() }}` / `{{ ref() }}`. That's what lets `dbt docs generate && dbt docs serve` render a complete lineage graph — six source nodes fanning into six models, with `trn_plan_master` fanning out into the two billing models. Before the sources were declared, the docs graph showed four disconnected floating models; wiring them up is what makes the lineage tell the real story.

**The six models:**

| Model | Rows | What it does |
|---|---|---|
| `trn_cust_profile` | 599 | Cleans customer identity — de-underscores names (`REPLACE`), parses `M/D/YYYY` dates (`strptime`), and splits a single packed `Address` field into address / state / PIN using `SPLIT_PART` / `LEFT` / `RIGHT` / `TRIM`, nulling out 3 incomplete comma-less addresses. |
| `trn_cust_status` | 589 | Subscriber payment status. Handles a column with **two mixed date formats** in one field (see Engineering decisions). |
| `trn_loyalty` | 599 | Decodes loyalty badge codes (`BRNZ→Bronze`, `SLVR→Silver`, `PLAT→Platinum`, `GOLD→Gold`) via `CASE`, and normalizes a registration-date column where the separator predicts the format — a **documented 42% assumption**. |
| `trn_plan_master` | 4 | The rate card. Source columns had leading spaces (`" free_voice_minutes"`) and inner spaces (`"Plan Description"`); the model quotes them exactly and renames them clean, making this the boundary where the source naming mess stops. |
| `trn_data_plan` | 729 | **First JOIN model** — `LEFT JOIN` to `trn_plan_master` on plan type; computes `billable_data_amt = ROUND(data_consumed * data_rate_per_kb, 2)` as a real `DECIMAL`. Verified zero unmatched rows. |
| `trn_voice_plan` | 737 | Bills voice and SMS, each against a free allowance: `CASE WHEN usage > allowance THEN (usage-allowance)*rate ELSE 0`. Needs an explicit `NULL` guard for the free-SMS plan (see Engineering decisions). |

**Testing.** Data-quality tests live alongside the models in `schema.yml` (26 tests — `not_null`, `unique`, `accepted_values`, relationship checks). `dbt test` runs them as its own DAG task, and their pass/fail is parsed by the quality gate downstream, so a broken assumption fails the pipeline rather than silently reaching the dashboard.

The judgment calls each model forced are written up in **[Engineering decisions](#engineering-decisions-the-interesting-part)** below.

### 3. Orchestration (`airflow/dags/slipstream_pipeline.py`)

A single DAG, `slipstream_pipeline`, chains the whole pipeline and runs it nightly:

```
stage_source_files → ingest_csv_to_duckdb → dbt_run → dbt_test → data_quality_report
```

Scheduled `0 2 * * *` (daily at 02:00). Each task runs green end to end.

**Why separate virtualenvs.** The biggest lesson here: **dbt-core and Airflow have irreconcilable dependency pins** — they disagree on `protobuf`, `click`, `jinja2`, `typing-extensions`, and `sqlparse`, so they cannot share one environment. They live in two:

- `~/airflow-venv` — Airflow only
- `~/dbt-venv` — dbt + pandas + duckdb

The DAG's `dbt_run` / `dbt_test` tasks invoke `~/dbt-venv/bin/dbt` by **full path through a `BashOperator`**, rather than importing dbt into the Airflow process. That isolation is not a workaround — it's the standard production pattern for running dbt under Airflow.

**Running on Windows.** Airflow can't run natively on Windows, so the whole scheduler runs in **WSL (Ubuntu)**. Two environment gotchas worth recording:

- Ubuntu's default Python was too new for Airflow, so `python3.12` was added via the deadsnakes PPA and Airflow installed against its official `constraints-3.12` file.
- `AIRFLOW_HOME` is kept on the native Linux filesystem (`~/airflow`), because Airflow's SQLite metadata DB hits file-locking bugs on the Windows-mounted `/mnt/c`. The project itself stays on `/mnt/c`; only Airflow's home moves.

**Slack alerting.** Wired into the DAG's callbacks, not bolted on afterward:

- `on_failure_callback` in `default_args` covers **every** task — any failure posts to Slack.
- `on_success_callback` on the **last** task only, so a clean run produces exactly one completion message (not one per task).
- Posts go through plain `requests` to an incoming webhook, deliberately **not** the Slack provider package — that avoids adding yet another dependency to clash with the two above.
- The webhook URL is stored as an Airflow **Variable** (`slack_webhook_url`) and passed to tasks via `env=`, so it stays out of the repo and out of rendered task logs.
- The posting helper swallows all exceptions, so alerting can **never** be the thing that fails the pipeline.

### 4. Data-quality gate (`quality/report.py`)

The final DAG task, and the safety net. Four checks:

1. **Row-count reconciliation** — source CSV → staging → model. A mismatch exits 1 and fails the run.
2. **dbt test results** — parsed from `target/run_results.json`; any failing test fails the run.
3. **Volume delta** — compares row counts to the previous run; a >20% swing *warns* rather than fails, to avoid alert fatigue on legitimate change.
4. **Business metrics** — billable totals and customer counts, logged each run.

It writes `quality_row_history` and `quality_metric_history` tables to DuckDB every run (this history feeds the dashboard's Pipeline health panel) and posts a Block Kit report to Slack — with, on failure, the log paths and a ready-to-run grep command so someone without Airflow access can still investigate.

### 5. Dashboard (`dashboard/app.py`)

A Streamlit + Plotly single-page app reading straight from DuckDB: revenue by plan, revenue share, loyalty-tier value, subscribers by state, payments over time, and pipeline health. Dark-themed, deployed free on Streamlit Cloud from a slim 2 MB copy of the database containing only the seven tables the dashboard reads.

---

## Engineering decisions (the interesting part)

The tools were the easy part. These are the judgment calls that made the data trustworthy:

- **Two date formats in one column.** `trn_cust_status.Paid_Date` held a mix: 333 rows unambiguously US format, 2 unambiguously international, 254 ambiguous. Solved with `COALESCE(try_strptime(…US…), try_strptime(…intl…))`, which parses all 589 rows with zero failures — no guessing on the ambiguous middle.

- **A documented assumption, not a silent one.** In `trn_loyalty`, the separator perfectly predicts the date format: all slash-dated rows are unambiguous, but all 252 dash-dated rows are ambiguous with an Excel fingerprint suggesting the original day/month order may be permanently lost. The pipeline normalizes them consistently **and flags this as an assumption affecting 42% of the table** rather than pretending the data is clean.

- **Omitting is a valid answer.** Several columns were declared in the ERD (`Defaulter`, `Deactivated`, `Row_Active_Index`) but never existed in the source CSVs and had no surviving transform rule. Rather than invent business logic to fill them, they're **omitted and the gap is documented**. Inventing plausible-looking data is worse than admitting it isn't there.

- **Modernizing the output type.** Teradata emitted billing amounts as formatted VARCHAR (`'$1,234.00'`). The dbt models keep them as real `DECIMAL`, so downstream math and charts work without re-parsing strings.

- **NULL means "not applicable," not "zero."** `PLN-2K` has a blank free-SMS allowance because SMS is free on that plan (rate 0.0) — so it's kept `NULL`, since `0` would wrongly read as "no free SMS." The billing model then needs an explicit guard (`WHEN free_sms IS NULL THEN 0`) because in SQL, `used > NULL` evaluates to `NULL`, not `false`.

---

## Running it locally

```bash
# 1. Ingest CSVs into DuckDB
python ingestion/ingest.py

# 2. Build + test the dbt models
cd slipstream_dbt && dbt run && dbt test && cd ..

# 3. Run the data-quality gate
python quality/report.py

# 4. Launch the dashboard
streamlit run dashboard/app.py
```

Or run the whole chain through Airflow by triggering the `slipstream_pipeline` DAG.

---

## Deployment

The dashboard is deployed on **Streamlit Community Cloud**, reading a slim `dashboard/dashboard.duckdb` — a 2 MB copy built by `dashboard/build_dashboard_db.py` containing only the seven tables the dashboard queries (the full pipeline DB is much larger and is git-ignored). Re-run the build script whenever the pipeline produces new data you want reflected live.

---

## Caveat

**The billing figures are synthetic demo data and are not realistic.** The generated source has usage volumes (`data_consumed ~500,000` against a per-KB rate) that produce implausible totals (~$490M across 599 customers). The pipeline *math* is correct; the *input* isn't real. This is deliberately surfaced on the dashboard rather than hidden.
