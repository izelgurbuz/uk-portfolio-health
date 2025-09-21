from datetime import datetime, timedelta

from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow import DAG
from src.pipeline.jobs.data_quality import dq_check
from src.pipeline.jobs.export_snapshots import export_portfolio_metrics
from src.pipeline.jobs.incremental_load import main as run_incremental_main
from src.pipeline.jobs.load_transactions_csv import main as load_transactions_csv
from src.pipeline.jobs.profile_queries import profile_snowflake_queries
from src.pipeline.jobs.upload_to_s3 import upload_latest_snapshot
from src.pipeline.utils.alerts import send_slack_alert


def airflow_failure_callback(context):
    """
    Called when any task fails in Airflow.
    """
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    msg = f"Task `{task_id}` in DAG `{dag_id}` failed!"
    send_slack_alert(msg)


default_args = {
    "owner": "izel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_failure_callback,
}

with DAG(
    dag_id="etl_uk_portfolio_health",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1-5",  # Run daily(weekdays) at 06:00
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["portfolio", "etl", "analytics"],
    template_searchpath=["/opt/airflow/sql"],
) as dag:
    t_apply_roles = SnowflakeOperator(
        task_id="apply_roles",
        sql="00_roles_warehouses.sql",
        snowflake_conn_id="snowflake_default",
    )

    t_apply_schemas = SnowflakeOperator(
        task_id="apply_schemas",
        sql="01_db_schemas.sql",
        snowflake_conn_id="snowflake_default",
    )

    t_apply_tables = SnowflakeOperator(
        task_id="apply_tables",
        sql="02_staging_tables.sql",
        snowflake_conn_id="snowflake_default",
    )

    t_incremental_load = PythonOperator(
        task_id="incremental_load",
        python_callable=run_incremental_main,
    )

    t_dq_check = PythonOperator(
        task_id="dq_check",
        python_callable=dq_check,
    )
    t_build_fact_prices = SnowflakeOperator(
        task_id="build_fact_prices",
        sql="03_analytics_table.sql",
        snowflake_conn_id="snowflake_default",
    )
    t_load_transactions_csv = PythonOperator(
        task_id="load_transactions_csv",
        python_callable=load_transactions_csv,
    )
    t_build_positions_daily = SnowflakeOperator(
        task_id="build_positions_daily",
        sql="05_positions_daily.sql",
        snowflake_conn_id="snowflake_default",
    )

    t_build_portfolio_metrics = SnowflakeOperator(
        task_id="build_portfolio_metrics",
        sql="04_portfolio_metrics.sql",
        snowflake_conn_id="snowflake_default",
    )
    t_build_advanced_metrics = SnowflakeOperator(
        task_id="build_advanced_metrics",
        sql="06_advanced_metrics.sql",
        snowflake_conn_id="snowflake_default",
    )

    t_export_snapshot = PythonOperator(
        task_id="export_snapshot",
        python_callable=export_portfolio_metrics,
    )
    t_profile_queries = PythonOperator(
        task_id="profile_queries",
        python_callable=profile_snowflake_queries,
    )
    t_upload_s3 = PythonOperator(
        task_id="upload_s3_snapshot",
        python_callable=upload_latest_snapshot,
    )

    chain(
        t_apply_roles,
        t_apply_schemas,
        t_apply_tables,
        t_incremental_load,
        t_dq_check,
        t_build_fact_prices,
        t_load_transactions_csv,
        t_build_positions_daily,
        t_build_portfolio_metrics,
        t_build_advanced_metrics,
        t_export_snapshot,
        t_upload_s3,
        t_profile_queries,
    )
