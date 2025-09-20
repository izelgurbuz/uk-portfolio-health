import os

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from ..utils.logging import log

load_dotenv("/opt/airflow/.env")


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


def write_df(conn, df: pd.DataFrame, table: str, schema: str = "PORTFOLIO.RAW") -> int:
    """
    Load a Pandas DataFrame into a Snowflake table.

    """

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table,
        schema=schema,
        quote_identifiers=False,  # use uppercase cols without quotes
    )
    log(f"[LOAD] {nrows} rows into {schema}.{table} (success={success})")
    return nrows
