from ..load.snowflake_loader import sf_conn
from ..utils.logging import log


def profile_snowflake_queries():
    """
    Run multiple profiling queries against Snowflake query history
    and log the results.
    """

    queries = {
        "recent_queries": """
            SELECT QUERY_ID, TOTAL_ELAPSED_TIME, ROWS_PRODUCED, BYTES_SCANNED
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ))
            ORDER BY START_TIME DESC
            LIMIT 20
        """,
        "top_expensive": """
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
        """,
    }

    with sf_conn() as conn:
        with conn.cursor() as cur:
            for name, sql in queries.items():
                cur.execute(sql)

                # Extract column names
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                # Log results nicely
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    log(" | ".join(f"{col}={val}" for col, val in row_dict.items()))

                if not rows:
                    log(f" No results for {name}")
