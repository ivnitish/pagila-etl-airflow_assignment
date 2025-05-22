from __future__ import annotations

import pendulum # Airflow's preferred way to handle dates

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
# We'll import your ETL script's function later
import etl_script # Import your ETL script (updated name)

# Define a Python callable that will run your ETL script
# We'll need to modify your etl_script.py to be importable and take connection info
def run_pagila_etl_callable(**kwargs):
    """
    This function will be called by the PythonOperator.
    It will import and run the main logic from your etl_script.py
    using the replica and watermark approach.
    """
    # Placeholder for now - we will call your actual ETL script logic here
    print("Attempting to run Pagila ETL script using replica/watermark approach...")
    
    # Define the Airflow Connection IDs. These MUST match the ones
    # you will create in the Airflow UI (Admin -> Connections).
    pagila_db_conn_id = "pagila_postgres_connection"  # Example Connection ID
    rollup_db_conn_id = "rollup_postgres_connection"  # Example Connection ID

    # Call the main ETL function from the imported etl_script module
    etl_script.run_etl_replica_approach(
        pagila_conn_id=pagila_db_conn_id,
        rollup_conn_id=rollup_db_conn_id
    )
    # --- END INTEGRATION POINT ---

    print("Pagila ETL script (replica/watermark approach) execution attempt finished.")


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