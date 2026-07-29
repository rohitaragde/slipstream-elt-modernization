# Slipstream — ELT Modernization

A legacy Teradata ELT pipeline, rebuilt on a modern open-source stack.

The original Slipstream project loaded telecom subscriber and usage data into Teradata using
BTEQ and TPT utilities, orchestrated by shell scripts. This repository rebuilds the same
pipeline — same source data, same star schema, same business logic — using Python, DuckDB,
dbt and Airflow.

---

## Why rebuild it

The original ran on infrastructure that no longer exists: a licensed Teradata instance, TPT
load operators, and a `StgToTrn` stored procedure whose body was never in version control. The
transformation logic survived only as a column-level mapping spreadsheet.

Reconstructing it on an open stack makes the pipeline reproducible by anyone with a laptop, and
replaces several Teradata-specific workarounds with native functions.

| Layer | Original | Rebuilt as |
|-------|----------|------------|
| Extract / Load | TPT (`tbuild -f load*.tpt`) | Python + pandas → DuckDB |
| Transform | BTEQ → `StgToTrn` stored proc | dbt models |
| Orchestration | `loadStg.sh` shell script | Airflow DAG |
| Testing | Manual test-case spreadsheet | dbt data tests |

---

## Data model

Six tables: two customer dimensions, one loyalty dimension, one plan reference table, and two
usage fact tables.

```
                        ┌──────────────────┐
                        │   Cust_Profile   │
                        │  (sbscrbr PK)    │
                        └────────┬─────────┘
              ┌──────────────────┼──────────────────┐
              │                  │                  │
      ┌───────┴──────┐  ┌────────┴───────┐  ┌───────┴────────┐
      │ Cust_Status  │  │  Loyalty_file  │  │  usage facts   │
      └──────────────┘  └────────────────┘  └───────┬────────┘
                                                    │
                        ┌───────────────────────────┴──────────┐
                        │                                      │
                ┌───────┴────────┐                    ┌────────┴───────┐
                │  Billing_Data  │                    │ Billing_Voice  │
                └───────┬────────┘                    └────────┬───────┘
                        └──────────────┬──────────────────────-┘
                                ┌──────┴───────┐
                                │ Plan_Master  │
                                │ (rate card)  │
                                └──────────────┘
```

---

## Pipeline

**Ingestion** — `ingestion/ingest.py` reads 10 CSVs from `data/raw/` into 6 DuckDB staging
tables. The two usage tables are each fed by three run files (an initial load plus two
incremental batches). Rows failing key validation are written to `data/reject/`; source files
are archived only after a successful load, so a failed file is retried on the next run. Staging
tables are dropped and rebuilt each run, making the load idempotent.

**Transformation** — six dbt models in `slipstream_dbt/models/staging/` clean the staged data
and calculate billing.

| Model | Rows | What it does |
|-------|------|--------------|
| `trn_cust_profile` | 599 | Name cleanup, date parsing, splits packed address into address / state / PIN |
| `trn_cust_status` | 589 | Parses payment dates arriving in two different formats |
| `trn_loyalty` | 599 | Decodes badge codes, parses registration dates |
| `trn_plan_master` | 4 | Rate card — normalises malformed source column names |
| `trn_data_plan` | 729 | Joins to rate card; `billable = consumed × rate_per_kb` |
| `trn_voice_plan` | 737 | Joins to rate card; allowance-based billing for voice and SMS |

Staging tables are declared as dbt sources in `models/staging/sources.yml`, so the full
lineage — raw tables through to billing — renders in the generated docs.

**Testing** — 26 dbt tests covering primary-key uniqueness, non-null constraints, referential
integrity between usage facts and the rate card, and accepted values for loyalty tiers.

**Orchestration** — `airflow/dags/slipstream_pipeline.py` chains the pipeline into a scheduled
DAG, replacing the original `loadStg.sh`:

```
stage_source_files → ingest_csv_to_duckdb → dbt_run → dbt_test
```

Each task only runs if the previous one succeeded, failures retry once after two minutes, and
every run's logs are retained. Scheduled daily at 02:00.

---

## Billing logic

Data is charged from the first kilobyte — there is no free allowance:

```
billable_data_amt = data_consumed × data_rate_per_kb
```

Voice and SMS both use an allowance model, charging only for usage above the plan's free tier:

```
billable     = usage > allowance ? (usage − allowance) × rate : 0
non_billable = LEAST(usage, allowance) × rate
```

Amounts are stored as `DECIMAL`. The original cast them to formatted strings
(`'$1,051,246.00'` as `VARCHAR(20)`), which prevents any downstream aggregation.

---

## Assumptions and data-quality notes

Real source data forced several judgment calls. Each is recorded here because the reasoning
matters more than the result.

### Mixed date formats

`Paid_Date` contains two conventions in one column. Of 589 rows, 333 are unambiguously
`M/D/YYYY` (day > 12), 2 are unambiguously `D/M/YYYY`, and 254 are readable either way. Parsing
attempts `M/D/YYYY` first and falls back to `D/M/YYYY`:

```sql
COALESCE(
    try_strptime(Paid_Date, '%-m/%-d/%Y'),
    try_strptime(Paid_Date, '%-d/%-m/%Y')
)
```

`try_strptime` returns `NULL` on a failed match rather than raising, which is what makes the
fallback possible. All 589 rows parse.

**Assumption:** the 254 ambiguous rows are read as `M/D/YYYY`, inferred from the dominant
convention among rows that can be resolved.

### `Reg_date_time` — probable spreadsheet corruption

`Reg_date_time` splits cleanly by separator: all 347 slash-delimited rows are unambiguously
`MM/DD`, and all 252 dash-delimited rows are ambiguous — with both date components maxing at
exactly 12 across all 252.

For 252 genuine dates spanning several years, that distribution does not occur naturally. It is
the signature of a spreadsheet having opened the file: values interpretable as dates were
silently reformatted (acquiring dashes), while values with a component above 12 did not match
the locale pattern and were left as text.

**Consequence:** for those 252 rows the original day/month order may already be lost, and no
query can recover it. They are parsed as `MM-DD` for consistency with the only rows that carry
evidence. This assumption affects 42% of the table.

### Columns declared but never delivered

`Defaulter`, `Deactivated` and `Row_Active_Index` appear in the ERD and in the mapping
spreadsheet with declared types, but no source file ever contained them and no transformation
rule for them survived.

They are omitted rather than fabricated. Inventing business logic to fill a schema gap produces
a pipeline that looks complete and is quietly wrong.

### `free_sms` on the premium plan

`PLN-2K` has a blank `free_sms` where the other three plans have 50, 100 and 200. Its
`sms_rate` is `0.0` — SMS is free on that plan, making an allowance meaningless.

Kept as `NULL` rather than coerced to `0`, since `0` would read as *no free SMS* — the opposite
of the truth. The billing model guards this explicitly, because `sms_used > NULL` evaluates to
`NULL` rather than `false` and would otherwise fall through the `CASE`.

### Incomplete addresses

Three of 599 addresses contain only a street, with no comma, city, state or PIN. `state_cd` and
`pin` are set to `NULL` for those rows rather than extracting fragments from a string that never
held the values.

### Deviation from the mapping spec

The spec defines `Non_Billable_Sms_Amt` as `sms_used × sms_rate` — the value of *all* usage,
which contradicts the column name. It is implemented here as `LEAST(usage, allowance) × rate`:
the portion that was not charged.

The voice rows of the same spreadsheet are visibly misaligned — `Non-Billable_Voice_Amt` maps to
a source column of `sms_used`, and `Billable_Voice_Amt` to `load_date` with a date-parsing
formula attached — so the voice formulas were derived by analogy with the SMS logic.

---

## Teradata → DuckDB translations

Several transformations were verbose in Teradata purely because the platform lacked a native
function.

**Date parsing.** The original tokenised the string, tested the month's length, zero-padded it,
reassembled the components in a new order, and only then parsed:

```sql
TO_DATE(
  CASE WHEN CHARACTER_LENGTH(strtok(DOB, '/', 1)) = 1
       THEN strtok(DOB,'/',3)||'-'||'0'||strtok(DOB,'/',1)||'-'||strtok(DOB,'/',2)
       ELSE strtok(DOB,'/',3)||'-'||strtok(DOB,'/',1)||'-'||strtok(DOB,'/',2)
  END, 'YYYY-MM-DD')
```

Replaced by a single format pattern, where `%-m` accepts a single- or double-digit month:

```sql
strptime("D.O.B.", '%-m/%-d/%Y')
```

**Fixed-position address slicing.** The original used `SUBSTR(State_CD, 1, 3)` and
`SUBSTR(State_CD, 4, 6)` — correct only if every address sits at identical character offsets.
The rebuilt version anchors on the comma instead, which survives addresses of varying length:

```sql
LEFT(TRIM(SPLIT_PART(Address, ',', 2)), 2)   -- state
RIGHT(Address, 5)                            -- PIN
```

**Currency as text.** `CAST(... AS FORMAT '$$$,zz,zz9.99')` stored monetary amounts as
`VARCHAR`. Amounts are kept numeric here and formatted at the presentation layer.

---

## Running it

### Pipeline only

```bash
python -m venv venv
source venv/Scripts/activate          # Windows; use venv/bin/activate elsewhere
pip install pandas duckdb pyarrow dbt-duckdb

cp datasources/*.csv data/raw/
python ingestion/ingest.py

cd slipstream_dbt
dbt run
dbt test
```

`profiles.yml` must point at `slipstream.duckdb` in the repository root.

`datasources/` holds the source CSVs and is version-controlled. `data/` is a working directory
rebuilt on every run and is not tracked.

### Documentation and lineage

```bash
cd slipstream_dbt
dbt docs generate
dbt docs serve --port 8081
```

Port 8081 avoids colliding with Airflow on 8080.

### Orchestration

Airflow has no native Windows support, so it runs under WSL. Two details are worth knowing
before reproducing this:

**dbt and Airflow cannot share a virtualenv.** Their dependency pins conflict directly —
`protobuf`, `click`, `jinja2`, `typing-extensions` and `sqlparse` all resolve to incompatible
versions. Installing both in one environment leaves whichever was installed last in a broken
state. They are kept separate, and the DAG invokes dbt by absolute path:

```bash
python3.12 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
pip install "apache-airflow==2.10.5" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

python3.12 -m venv ~/dbt-venv
source ~/dbt-venv/bin/activate
pip install dbt-duckdb pandas duckdb pyarrow
```

**`AIRFLOW_HOME` must live on the Linux filesystem.** Airflow's SQLite metadata database hits
file-locking problems under `/mnt/c`. The project itself can stay on the Windows side; only
Airflow's internals need to be native.

```bash
source ~/airflow-venv/bin/activate
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/mnt/c/.../slipstream-elt-modernization/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

The UI is then at `localhost:8080`. dbt also needs its own `~/.dbt/profiles.yml` inside WSL,
with a Linux path to `slipstream.duckdb`.

Airflow requires Python ≤ 3.12. Ubuntu 26.04 ships 3.14 only, so 3.12 was installed from the
deadsnakes PPA.

---

## Repository layout

```
ingestion/ingest.py                   CSV → DuckDB staging
datasources/                          source CSVs, ERD, mapping spec, test cases
slipstream_dbt/models/staging/        six transformation models, sources, tests
airflow/dags/                         pipeline DAG
data/                                 working directory (gitignored)
```

---

## Status

- [x] Ingestion — 10 CSVs → 6 staging tables
- [x] Transformation — 6 dbt models
- [x] Data tests — 26 passing
- [x] Source declarations and lineage docs
- [x] Orchestration — Airflow DAG
- [ ] Alerting — Slack notifications on failure
- [ ] Dashboard
