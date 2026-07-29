"""Build a slim dashboard.duckdb containing only the seven tables/views the
dashboard reads, materialized as real tables so it fits GitHub + Streamlit Cloud.

The trn_ models are dbt VIEWS whose definitions reference the source catalog by
its original name ("slipstream"), so the source file must be attached under that
exact name for the views to resolve. CREATE TABLE ... AS SELECT then reads through
each view and bakes it into stored data.

Run once from repo root:
    python dashboard/build_dashboard_db.py

Re-run whenever the pipeline produces new data you want reflected live.
"""

import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "slipstream.duckdb"                  # the big 4GB source
OUT = BASE / "dashboard" / "dashboard.duckdb"     # slim, committed to git

TABLES = [
    "trn_cust_profile", "trn_cust_status", "trn_loyalty",
    "trn_plan_master", "trn_data_plan", "trn_voice_plan",
    "quality_row_history",
]

OUT.unlink(missing_ok=True)                        # start fresh each build
con = duckdb.connect(str(OUT))
# attach AS slipstream — the name the dbt views reference internally
con.execute(f"ATTACH '{SRC}' AS slipstream (READ_ONLY)")
for t in TABLES:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM slipstream.main.{t}")
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {n} rows")
con.execute("DETACH slipstream")
con.close()
print(f"\nBuilt {OUT} ({OUT.stat().st_size / 1e6:.1f} MB)")
