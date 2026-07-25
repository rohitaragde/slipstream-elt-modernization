# ingestion/ingest.py
# Replaces: loadCustomer.tpt, loadCustomerStats.tpt, loadLoyalty.tpt,
#           loadPlanMaster.tpt, loadDataPlanRun.tpt, loadVoicePlanRun.tpt

import pandas as pd
import duckdb
import os
import shutil                      # FIX #5: needed for archiving
import logging
from pathlib import Path           # FIX #1: was missing in your merge
from datetime import datetime

# ---- FIX #1: Base directory derived from this file's location ----
# Portable: no drive letters, works no matter where the project moves,
# because __file__ is THIS script's own path.
BASE_DIR = Path(__file__).resolve().parent.parent   # ingestion/ -> project root

# ---- Paths ----
RAW_DIR     = BASE_DIR / 'data' / 'raw'
LANDING_DIR = BASE_DIR / 'data' / 'landing'
REJECT_DIR  = BASE_DIR / 'data' / 'reject'
ARCHIVE_DIR = BASE_DIR / 'data' / 'archive'
LOG_DIR     = BASE_DIR / 'logs'
DB_PATH     = BASE_DIR / 'slipstream.duckdb'

# Directories must exist BEFORE logging tries to open a file in LOG_DIR
for d in (LANDING_DIR, REJECT_DIR, ARCHIVE_DIR, LOG_DIR):
    os.makedirs(d, exist_ok=True)

# ---- Logging setup: exactly ONE basicConfig, AFTER dirs exist ----
# (basicConfig only works the first time it's called; later calls are ignored)
logging.basicConfig(
    filename=LOG_DIR / f'ingest_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -- File to Table mapping (replaces 6 separate TPT scripts) --
FILES = {
    'Cust_Prfl.csv':        'stg_cust_profile',
    'Cust_Stats1.csv':      'stg_cust_status',
    'Loyalty_file.csv':     'stg_loyalty',
    'Plan_Master.csv':      'stg_plan_master',
    'Data_Plan_Run_1.csv':  'stg_data_plan',
    'Data_Plan_Run_2.csv':  'stg_data_plan',
    'Data_Plan_Run_3.csv':  'stg_data_plan',
    'Voice_Plan_Run_1.csv': 'stg_voice_plan',
    'Voice_Plan_Run_2.csv': 'stg_voice_plan',
    'Voice_Plan_Run_3.csv': 'stg_voice_plan',
}

# FIX #3: your code checked for 'subscbr'. The original dbt model used
# 'sbscrbr'.  >>> VERIFY this against the real header in Cust_Prfl.csv <
# and correct KEY_COLUMN if needed.
KEY_COLUMN = 'sbscrbr'


def ingest_file(filename, table_name, con):
    filepath = RAW_DIR / filename
    logging.info(f'Starting ingestion: {filename} -> {table_name}')
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Loading {filename} -> {table_name}')

    # Read CSV (replaces TPT File_Reader operator)
    df = pd.read_csv(filepath, encoding='latin-1')

    # Drop junk columns (e.g. Unnamed: 6 in Cust_Prfl)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # Add run_date column (replaces Teradata CURRENT_TIMESTAMP)
    df['run_date'] = datetime.now()

    # ---- FIX #3: validation that can't fail silently ----
    # Your old version: if the column name didn't match, the whole block
    # was skipped with no error and no message = silent failure.
    # Now: if the key column is missing, we say so loudly in the log.
    if KEY_COLUMN in df.columns:
        rejects = df[df[KEY_COLUMN].isnull()]
        if not rejects.empty:
            reject_path = REJECT_DIR / f'reject_{filename}'
            rejects.to_csv(reject_path, index=False)
            logging.warning(f'{len(rejects)} rejected rows written to {reject_path}')
        df = df[df[KEY_COLUMN].notnull()]
    else:
        logging.warning(
            f'{filename}: key column "{KEY_COLUMN}" not found '
            f'(columns are: {list(df.columns)}) - validation skipped'
        )

    # Load into DuckDB (replaces TPT LOAD operator -> Teradata STG table)
    con.execute(f'CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0')
    con.execute(f'INSERT INTO {table_name} SELECT * FROM df')

    # Save to Parquet in landing (replaces raw CSV in landing folder)
    parquet_path = LANDING_DIR / f'{table_name}_{filename.replace(".csv", "")}.parquet'
    df.to_parquet(parquet_path, index=False)

    # ---- FIX #5: archive the source file, ONLY after everything succeeded ----
    # If anything above raised, we never reach this line, so a failed file
    # stays in raw/ and gets retried next run. That's why it goes last.
    shutil.move(str(filepath), str(ARCHIVE_DIR / filename))

    # ---- FIX #2: lowercase 'logging' (Logging with capital L = NameError) ----
    logging.info(f'Completed: {filename} | Rows loaded: {len(df)}')
    print(f'[{datetime.now().strftime("%H:%M:%S")}] OK - {len(df)} rows loaded')


def main():
    print('=' * 50)
    print('SLIPSTREAM MODERN - INGESTION LAYER')
    print('=' * 50)
    logging.info('Pipeline started')

    con = duckdb.connect(str(DB_PATH))

    # ---- FIX #4: idempotency — truncate-and-reload, like the TPT jobs did ----
    # Without this, every rerun INSERTs on top of existing rows and doubles
    # the counts. Drop each staging table ONCE before the loop (not inside
    # ingest_file — three files share stg_data_plan, so dropping per-file
    # would wipe runs 1 and 2 when run 3 loads).
    for table_name in set(FILES.values()):
        con.execute(f'DROP TABLE IF EXISTS {table_name}')

    for filename, table_name in FILES.items():
        try:
            ingest_file(filename, table_name, con)
        except Exception as e:
            logging.error(f'FAILED: {filename} | Error: {e}')
            print(f'ERROR loading {filename}: {e}')

    con.close()
    logging.info('Pipeline completed')
    print('=' * 50)
    print('INGESTION COMPLETE')
    print('=' * 50)


if __name__ == '__main__':
    main()