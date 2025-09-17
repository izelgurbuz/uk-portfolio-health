from datetime import datetime, timedelta

from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow import DAG
from src.pipeline.jobs.data_quality import dq_check
from src.pipeline.jobs.export_snapshots import export_portfolio_metrics
from src.pipeline.jobs.incremental_load import main as run_incremental_main

default_args = {"owner": "izel", "retries": 2, "retry_delay": timedelta(minutes=5)}

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
        sql="03_analytics_views.sql",
        snowflake_conn_id="snowflake_default",
    )
    t_build_portfolio_metrics = SnowflakeOperator(
        task_id="build_portfolio_metrics",
        sql="04_portfolio_metrics.sql",
        snowflake_conn_id="snowflake_default",
    )

    t_export_snapshot = PythonOperator(
        task_id="export_snapshot",
        python_callable=export_portfolio_metrics,
    )

    chain(
        t_apply_roles,
        t_apply_schemas,
        t_apply_tables,
        t_incremental_load,
        t_dq_check,
        t_build_fact_prices,
        t_build_portfolio_metrics,
        t_export_snapshot,
    )
