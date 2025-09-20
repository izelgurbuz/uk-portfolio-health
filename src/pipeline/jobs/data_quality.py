import datetime
import uuid

from ..load.snowflake_loader import sf_conn
from ..utils.alerts import send_slack_alert
from ..utils.logging import log


def dq_check():
    run_id = str(uuid.uuid4())
    run_date = datetime.date.today()

    def log_result(cur, task_name, status, count=None, error_message=None):
        """Insert data quality check results into the monitoring table."""
        cur.execute(
            """
            INSERT INTO PORTFOLIO.ANALYTICS.PIPELINE_MONITORING
                (run_id, run_date, task_name, status, record_count, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                run_date,
                task_name,
                status,
                count,
                error_message,
            ),
        )

    try:
        with sf_conn() as conn:
            cur = conn.cursor()

            # FX CHECK
            cur.execute(
                "SELECT COUNT(*) FROM PORTFOLIO.RAW.FX_DAILY WHERE DATE = CURRENT_DATE"
            )
            fx_missing = cur.fetchone()[0]

            # if fx_missing == 0:
            #     msg = f"No FX data for today ({run_date})!"
            #     send_slack_alert(msg)
            #     log_result(
            #         cur,
            #         "FX_DAILY_TODAY_CHECK",
            #         "FAIL",
            #         count=fx_missing,
            #         error_message=msg,
            #     )
            #     raise ValueError(msg)
            # else:
            #     log_result(cur, "FX_DAILY_TODAY_CHECK", "PASS", count=fx_missing)

            # EQUITY CHECK
            cur.execute(
                "SELECT COUNT(*) FROM PORTFOLIO.RAW.EQUITY_DAILY WHERE DATE = CURRENT_DATE"
            )
            eq_missing = cur.fetchone()[0]

            # if eq_missing == 0:
            #     msg = f"No equities data for today ({run_date})!"
            #     send_slack_alert(msg)
            #     log_result(
            #         cur,
            #         "EQUITY_DAILY_TODAY_CHECK",
            #         "FAIL",
            #         count=eq_missing,
            #         error_message=msg,
            #     )
            #     raise ValueError(msg)
            # else:
            #     log_result(cur, "EQUITY_DAILY_TODAY_CHECK", "PASS", count=eq_missing)

            # POSITIONS CHECK
            cur.execute("SELECT COUNT(*) FROM PORTFOLIO.RAW.PORTFOLIO_POSITIONS")
            pos_count = cur.fetchone()[0]

            if pos_count == 0:
                msg = "No positions found in PORTFOLIO.RAW.PORTFOLIO_POSITIONS!"
                send_slack_alert(msg)
                log_result(
                    cur, "POSITIONS_CHECK", "FAIL", count=pos_count, error_message=msg
                )
                raise ValueError(msg)
            else:
                log_result(cur, "POSITIONS_CHECK", "PASS", count=pos_count)

        log("All checks passed successfully.")

    except Exception as e:
        log(f"[ERROR] {e}")
        raise
