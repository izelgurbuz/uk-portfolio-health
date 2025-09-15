import os

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine, text


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


def write_df(df: pd.DataFrame, table: str, schema: str):
    eng = engine()
    with eng.connect() as conn:
        conn = conn.connection  # unwrap to raw Snowflake connection
        success, nchunks, nrows, _ = write_pandas(
            conn, df, table_name=table, schema=schema, overwrite=False
        )
        print(f"Loaded {nrows} rows into {schema}.{table}")
