from datetime import datetime, timedelta

from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow import DAG
from src.pipeline.jobs.data_quality import dq_check
from src.pipeline.jobs.incremental_load import main as run_incremental_main
from src.pipeline.jobs.load_to_snowflake import apply_ddl as sf_apply_ddl

default_args = {"owner": "izel", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="etl_uk_portfolio_health",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1-5",  # Run daily(weekdays) at 06:00
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["portfolio", "etl", "analytics"],
) as dag:
    t_apply_ddl = PythonOperator(task_id="apply_ddl", python_callable=sf_apply_ddl)

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
        sql="sql/03_analytics_views.sql",
        snowflake_conn_id="snowflake_default",
    )

    chain(
        t_apply_ddl,
        t_incremental_load,
        t_dq_check,
        t_build_fact_prices,
    )
