import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

# Load .env at the project root
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR.parent / ".env")


def get_snowflake_conn():
    """
    Create and return a Snowflake connector connection.
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "PORTFOLIO")
    schema = os.getenv("SNOWFLAKE_SCHEMA_ANALYTICS", "ANALYTICS")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    if not account or not user or not password:
        raise ValueError("Missing Snowflake credentials. Check your .env file.")

    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )
