import os
import pathlib
import sys

import pandas as pd
from dotenv import load_dotenv

from ..load.snowflake_loader import exec_sql, write_df
from ..utils.logging import log

ROOT = pathlib.Path(__file__).resolve().parents[3]
SQL_DIR = ROOT / "airflow" / "sql"
DATA_DIR = ROOT / "data" / "processed"


def apply_ddl():
    for f in ["00_roles_warehouses.sql", "01_db_schemas.sql", "02_staging_tables.sql"]:
        p = SQL_DIR / f
        sql_text = p.read_text()
        # Split on semicolons, strip whitespace, skip empties
        statements = [s.strip() for s in sql_text.split(";") if s.strip()]
        for stmt in statements:
            log(f"Applying: {stmt[:80]}...")  # preview first 80 chars
            exec_sql(stmt)


def load_raw():
    eq = pd.read_parquet(DATA_DIR / "equity_daily.parquet")
    fx = pd.read_parquet(DATA_DIR / "fx_daily.parquet")
    raw_schema = os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW")
    log(f"Loading equities -> {raw_schema}.EQUITY_DAILY ({len(eq)} rows)")
    write_df(eq, table="EQUITY_DAILY", schema=raw_schema)
    log(f"Loading FX -> {raw_schema}.FX_DAILY ({len(fx)} rows)")
    write_df(fx, table="FX_DAILY", schema=raw_schema)


if __name__ == "__main__":
    load_dotenv()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "--help"
    if cmd == "--apply-ddl":
        apply_ddl()
    elif cmd == "--load-raw":
        load_raw()
    else:
        print(
            "Usage: python -m src.pipeline.jobs.load_to_snowflake [--apply-ddl | --load-raw]"
        )
        sys.exit(2)
