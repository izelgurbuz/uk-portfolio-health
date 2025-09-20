from typing import Optional

from snowflake.connector import SnowflakeConnection


def get_last_loaded_date(conn: SnowflakeConnection, source: str) -> Optional[str]:
    """
    Get last_loaded_date for a source from RAW.LOAD_METADATA.
    Returns a date string 'YYYY-MM-DD' or None if not set.
    """
    cur = conn.cursor()
    sql = "SELECT last_loaded_date FROM RAW.LOAD_METADATA WHERE source = %s"
    row = cur.execute(sql, (source)).fetchone()
    return str(row[0]) if row and row[0] else None


def update_last_loaded_date(conn: SnowflakeConnection, source: str, date_str: str):
    """
    Upsert last_loaded_date for a given source.
    """
    sql = """
        MERGE INTO RAW.LOAD_METADATA t
        USING (SELECT %s AS source, %s AS last_loaded_date) s
        ON t.source = s.source
        WHEN MATCHED THEN UPDATE SET last_loaded_date = s.last_loaded_date, _updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (source, last_loaded_date) VALUES (s.source, s.last_loaded_date)
    """
    cur = conn.cursor()
    cur.execute(sql, (source, date_str))
