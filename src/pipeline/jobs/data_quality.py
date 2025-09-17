from sqlalchemy import text

from ..load.snowflake_loader import engine
from ..utils.logging import log


def dq_check():
    with engine().connect() as conn:
        fx_missing = conn.execute(
            text("""
            SELECT COUNT(*) FROM PORTFOLIO.RAW.EQUITY_DAILY e
            LEFT JOIN PORTFOLIO.RAW.FX_DAILY fx
                ON e.DATE = fx.DATE AND fx.pair = 'USDGBP'
            WHERE fx.RATE IS NULL                  
        """)
        ).scalar()

        if fx_missing > 0:
            raise ValueError(f"Missing FX rates for {fx_missing} rows")

        neg_prices = conn.execute(
            text("""
            SELECT COUNT(*) FROM PORTFOLIO.RAW.EQUITY_DAILY WHERE close <= 0
        """)
        ).scalar()

        if neg_prices > 0:
            raise ValueError(f"Found {neg_prices} rows with non-positive close prices")

    log("All checks passed.")
