from sqlalchemy import text

from ..load.snowflake_loader import engine
from ..utils.logging import log


def profile_snowflake_queries():
    """
    Run multiple profiling queries against Snowflake query history
    and log the results.
    """

    queries = {
        "recent_queries": text("""
            SELECT QUERY_ID, TOTAL_ELAPSED_TIME, ROWS_PRODUCED, BYTES_SCANNED
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ))
            ORDER BY START_TIME DESC
            LIMIT 20
        """),
        "top_expensive": text("""
            SELECT
                QUERY_ID,
                USER_NAME,
                WAREHOUSE_NAME,
                DATABASE_NAME,
                SCHEMA_NAME,
                TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SECONDS,
                ROWS_PRODUCED,
                BYTES_SCANNED / 1024 / 1024 / 1024 AS SCANNED_GB,
                QUERY_TEXT
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ))
            WHERE QUERY_TYPE = 'SELECT'
            ORDER BY BYTES_SCANNED DESC, ELAPSED_SECONDS DESC
            LIMIT 10
        """),
    }

    with engine().connect() as conn:
        for name, sql in queries.items():
            log(f"=== Profiling report: {name} ===")
            rows = conn.execute(sql).fetchall()
            for r in rows:
                log(" | ".join(f"{col}={val}" for col, val in r._mapping.items()))
