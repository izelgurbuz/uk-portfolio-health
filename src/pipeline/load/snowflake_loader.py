import os

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
