import datetime
import uuid

from sqlalchemy import text

from ..load.snowflake_loader import engine
from ..utils.alerts import send_slack_alert
from ..utils.logging import log


def dq_check():
    run_id = str(uuid.uuid4())
    run_date = datetime.date.today()

    def log_result(task_name, status, count=None, error_message=None):
        with engine().connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO ANALYTICS.PIPELINE_MONITORING(run_id, run_date, task_name, status, record_count, error_message)
                    VALUES (:run_id, :run_date, :task_name, :status, :count, :error_message)
                """),
                {
                    "run_id": run_id,
                    "run_date": run_date,
                    "task_name": task_name,
                    "status": status,
                    "count": count,
                    "error_message": error_message,
                },
            )

    try:
        with engine().connect() as conn:
            fx_missing = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM RAW.FX_DAILY WHERE DATE = CURRENT_DATE
                    """
                )
            ).scalar()

            if fx_missing == 0:
                msg = f"No FX data for today ({run_date})!"
                send_slack_alert(msg)
                log_result(
                    "FX_DAILY_TODAY_CHECK", "FAIL", count=fx_missing, error_message=msg
                )
                raise ValueError(msg)
            else:
                log_result("FX_DAILY_TODAY_CHECK", "PASS", count=fx_missing)

            eq_missing = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM RAW.EQUITY_DAILY WHERE date = CURRENT_DATE
                    """
                )
            ).scalar()

            if eq_missing == 0:
                msg = f"No equities data for today ({run_date})!"
                send_slack_alert(msg)
                log_result(
                    "EQUITY_DAILY_TODAY_CHECK",
                    "FAIL",
                    count=eq_missing,
                    error_message=msg,
                )
                raise ValueError(msg)
            else:
                log_result("EQUITY_DAILY_TODAY_CHECK", "PASS", count=eq_missing)

            pos_count = conn.execute(
                text("""
                SELECT COUNT(*) FROM RAW.PORTFOLIO_POSITIONS
            """)
            ).scalar()
            if pos_count == 0:
                msg = " No positions found in RAW.PORTFOLIO_POSITIONS!"
                send_slack_alert(msg)
                log_result(
                    "POSITIONS_CHECK", "FAIL", count=pos_count, error_message=msg
                )
                raise ValueError(msg)
            else:
                log_result("POSITIONS_CHECK", "PASS", count=pos_count)

        log("All checks passed successfully.")
    except Exception as e:
        log(f"ERROR] {e}")
        raise
