import os
from typing import Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine, text

from ..utils.logging import log


def engine():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "PORTFOLIO")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    # schema set per-write
    return create_engine(
        f"snowflake://{user}:{password}@{account}/{database}/PUBLIC"
        f"?role={role}&warehouse={warehouse}"
    )


def exec_sql(sql: str):
    with engine().connect() as conn:
        conn.execute(text(sql))


def get_last_loaded_date(source: str) -> Optional[str]:
    """
    Get last_loaded_date for a source from RAW.LOAD_METADATA.
    Returns a date string 'YYYY-MM-DD' or None if not set.
    """
    sql = text("SELECT last_loaded_date FROM RAW.LOAD_METADATA WHERE source = :src")
    with engine().connect() as conn:
        row = conn.execute(sql, {"src": source}).fetchone()
        return str(row[0]) if row and row[0] else None


def update_last_loaded_date(source: str, date_str: str):
    """
    Upsert last_loaded_date for a given source.
    """
    sql = text("""
        MERGE INTO RAW.LOAD_METADATA t
        USING (SELECT :src AS source, :dt AS last_loaded_date) s
        ON t.source = s.source
        WHEN MATCHED THEN UPDATE SET last_loaded_date = s.last_loaded_date, _updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (source, last_loaded_date) VALUES (s.source, s.last_loaded_date)
    """)
    with engine().connect() as conn:
        conn.execute(sql, {"src": source, "dt": date_str})


def sf_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "PORTFOLIO"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def write_df(df: pd.DataFrame, table: str, schema: str = "RAW") -> int:
    """
    Load a Pandas DataFrame into a Snowflake table.

    Args:
        df: DataFrame with columns matching the target table
        table: Table name (string, no schema prefix)
        schema: Target schema (default = RAW)

    Returns:
        Number of rows inserted
    """
    with sf_conn() as conn:
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name=table,
            schema=schema,
            quote_identifiers=False,  # use uppercase cols without quotes
        )
        log(f"[LOAD] {nrows} rows into {schema}.{table} (success={success})")
        return nrows
