import os

import pandas as pd
from dotenv import load_dotenv

from ..load.snowflake_loader import sf_conn, write_df
from ..utils.logging import log

CSV_PATH = "data/raw/portfolio_transactions.csv"
STAGE_TABLE = "PORTFOLIO.RAW.PORTFOLIO_TRANSACTIONS_STAGE"
TARGET_TABLE = "PORTFOLIO.RAW.PORTFOLIO_TRANSACTIONS"


def _ensure_stage_table(cur):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {STAGE_TABLE} (
      transaction_id STRING,
      portfolio_id   STRING,
      symbol         STRING,
      quantity_delta NUMBER(18,6),
      transaction_date DATE,
      transaction_type STRING
    )
    """

    cur.execute(sql)


def _merge_stage_into_target(cur):
    sql = f"""
    MERGE INTO {TARGET_TABLE} t
    USING {STAGE_TABLE} s
    ON t.TRANSACTION_ID = s.TRANSACTION_ID
    WHEN MATCHED THEN UPDATE SET
        PORTFOLIO_ID = s.PORTFOLIO_ID,
        SYMBOL = s.SYMBOL,
        QUANTITY_DELTA = s.QUANTITY_DELTA ,
        TRANSACTION_DATE = s.TRANSACTION_DATE,
        TRANSACTION_TYPE = s.TRANSACTION_TYPE
    WHEN NOT MATCHED THEN INSERT (TRANSACTION_ID,PORTFOLIO_ID ,SYMBOL ,QUANTITY_DELTA,TRANSACTION_DATE ,TRANSACTION_TYPE)
    VALUES ( s.TRANSACTION_ID,s.PORTFOLIO_ID,s.SYMBOL,s.QUANTITY_DELTA ,s.TRANSACTION_DATE,s.TRANSACTION_TYPE)
    """

    cur.execute(sql)


def _truncate_stage(cur):
    cur.execute(f"TRUNCATE TABLE {STAGE_TABLE}")


def _verify_truncate(cur):
    result = cur.execute(f"SELECT COUNT(*) FROM {STAGE_TABLE}")
    print(f"Stage row count after truncate: {result}")


def main():
    load_dotenv()
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")
    with sf_conn() as conn:
        cur = conn.cursor()

        _ensure_stage_table(cur)

        df = pd.read_csv(CSV_PATH)
        # basic validation
        required = {
            "transaction_id",
            "portfolio_id",
            "symbol",
            "quantity_delta",
            "transaction_date",
            "transaction_type",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        _truncate_stage(cur)
        _verify_truncate(cur)
        write_df(conn, df, table="PORTFOLIO_TRANSACTIONS_STAGE", schema="RAW")
        _merge_stage_into_target(cur)
        log(f"Loaded {len(df)} transactions from {CSV_PATH}")


if __name__ == "__main__":
    main()
