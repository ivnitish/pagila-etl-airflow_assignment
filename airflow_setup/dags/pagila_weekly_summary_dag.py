from __future__ import annotations

import pendulum # Airflow's preferred way to handle dates

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook # Import BaseHook
# We'll import your ETL script's function later
import etl_script_incremental_pandas 

# Define a Python callable that will run your ETL script
def run_pagila_etl_callable(**kwargs):
    """
    This function will be called by the PythonOperator.
    It will import and run the main logic from your etl_script_incremental_pandas.py
    """
    print("Attempting to run Pagila ETL script using incremental pandas approach...")
    
    pagila_db_conn_id = "pagila_postgres_connection"  
    rollup_db_conn_id = "rollup_postgres_connection"  

    # Get connection parameters using Airflow Hooks
    pagila_conn = BaseHook.get_connection(pagila_db_conn_id)
    rollup_conn = BaseHook.get_connection(rollup_db_conn_id)

    pagila_conn_params = {
        "host": pagila_conn.host,
        "port": pagila_conn.port,
        "dbname": pagila_conn.schema, # In Airflow, 'schema' usually holds the database name
        "user": pagila_conn.login,
        "password": pagila_conn.password,
    }

    rollup_conn_params = {
        "host": rollup_conn.host,
        "port": rollup_conn.port,
        "dbname": rollup_conn.schema,
        "user": rollup_conn.login,
        "password": rollup_conn.password,
    }

    # Call the main ETL function from the imported script
    etl_script_incremental_pandas.run_incremental_etl(
        pagila_conn_params=pagila_conn_params,
        rollup_conn_params=rollup_conn_params
    )

    print("Pagila ETL script (incremental pandas approach) execution attempt finished.")


with DAG(
    dag_id="pagila_weekly_summary_etl",
    schedule=None, # Manual trigger, or use cron e.g., "0 0 * * 0" for weekly on Sunday
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # A past date for the DAG to start
    catchup=False, # Don't run for past missed schedules
    tags=["pagila", "etl", "analytics_engineering"],
    doc_md="""
    ### Pagila Weekly Summary ETL DAG
    This DAG extracts data from the Pagila source database, calculates weekly rental summaries,
    and loads them into the rollup database using a replica and watermark ETL approach.
    It uses Airflow connections for database credentials.
    """
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_full_pagila_etl",
        python_callable=run_pagila_etl_callable,
        # op_kwargs={'config_param': 'value'} # If you need to pass static params
    )