import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from ..load.snowflake_loader import engine
from ..utils.io import processed_dir
from ..utils.logging import log


def export_portfolio_metrics():
    """
    Export the VIEW_PORTFOLIO_METRICS to Parquet (for BI or reporting tools).
    """
    load_dotenv()
    out_dir = processed_dir() / "snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)

    sql = text("""
        SELECT *
        FROM PORTFOLIO.ANALYTICS.VIEW_PORTFOLIO_METRICS
        WHERE DATE >= CURRENT_DATE - INTERVAL '30 day'
    """)

    with engine().connect() as conn:
        df = pd.read_sql(sql, conn)

    out_file = out_dir / "portfolio_metrics.parquet"
    df.to_parquet(out_file, index=False)

    log(f"[EXPORT] Wrote {len(df)} rows to {out_file}")
