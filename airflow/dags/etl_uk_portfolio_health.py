from datetime import datetime, timedelta

from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

from airflow import DAG
from pipeline.jobs.load_to_snowflake import apply_ddl as sf_apply_ddl
from pipeline.jobs.load_to_snowflake import load_raw as sf_load_raw
from pipeline.jobs.run_local_etl import main as run_local_etl_main

default_args = {"owner": "izel", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="etl_uk_portfolio_health",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1-5",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["portfolio", "etl", "elt", "uk"],
) as dag:
    t_apply_ddl = PythonOperator(task_id="apply_ddl", python_callable=sf_apply_ddl)
    t_extract_clean_local = PythonOperator(
        task_id="extract_clean_local", python_callable=run_local_etl_main
    )
    t_load_raw = PythonOperator(task_id="load_raw", python_callable=sf_load_raw)

    chain(t_apply_ddl, t_extract_clean_local, t_load_raw)
