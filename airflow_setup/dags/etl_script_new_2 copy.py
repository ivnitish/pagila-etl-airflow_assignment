import psycopg2
import psycopg2.extras
from datetime import date, timedelta
from psycopg2.extras import execute_values # Import execute_values

# Airflow specific import
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Database Connection Helper ---
def get_db_connection_from_airflow_hook(conn_id: str):
    """Gets a psycopg2 connection using an Airflow PostgresHook."""
    print(f"Attempting to get DB connection using Airflow Conn ID: {conn_id}")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    connection = pg_hook.get_conn()
    print(f"Successfully established DB connection for: {conn_id}")
    return connection

# --- Helper: Get Last Processed Week (from original script, still useful) ---
def get_last_processed_week(rollup_cur):
    """Fetches the last processed week_beginning date from the summary table."""
    rollup_cur.execute("SELECT MAX(week_beginning) FROM weekly_rental_summary;")
    result = rollup_cur.fetchone()
    # Ensure result and its first element are not None before accessing
    last_week = result[0] if result and result[0] is not None else None
    print(f"Last processed week from summary table: {last_week}")
    return last_week

# --- Stage 1: Extract from Source and Load to Staging Table in Target DB ---
def run_stage_1_extract_and_stage(pagila_conn_id: str, rollup_conn_id: str, overall_start_date: date, overall_end_date: date):
    """
    Extracts raw rental data from Pagila source DB within the given date range
    and loads it into a staging table in the Rollup target DB.
    """
    print(f"Stage 1: Extracting from Pagila and staging to Rollup for dates {overall_start_date} to {overall_end_date}")
    pagila_conn = None
    rollup_conn = None
    pagila_cur = None
    rollup_cur = None

    staging_table_name = "stg_rental_activity"

    try:
        pagila_conn = get_db_connection_from_airflow_hook(pagila_conn_id)
        rollup_conn = get_db_connection_from_airflow_hook(rollup_conn_id)
        pagila_cur = pagila_conn.cursor() # Standard cursor for Pagila
        rollup_cur = rollup_conn.cursor() # Standard cursor for Rollup

        # 1. Create/Truncate Staging Table in Rollup DB
        print(f"Preparing staging table: {staging_table_name} in Rollup DB.")
        rollup_cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {staging_table_name} (
                rental_id INTEGER,
                rental_date TIMESTAMP,
                return_date TIMESTAMP
            );
        """)
        rollup_cur.execute(f"TRUNCATE TABLE {staging_table_name};")
        print(f"Staging table {staging_table_name} truncated.")

        # 2. Extract data from Pagila Source DB
        # This query fetches all records that might be relevant.
        # A rental is relevant if it was rented before the end of our overall period
        # AND (it was not returned OR it was returned after the start of our overall period).
        # This ensures we catch rentals active during any part of the window.
        extract_sql = """
            SELECT rental_id, rental_date, return_date
            FROM rental
            WHERE rental_date <= %s -- overall_end_date
              AND (return_date IS NULL OR return_date >= %s); -- overall_start_date
        """
        print(f"Extracting data from Pagila with query: {extract_sql.strip()} using params: {overall_end_date}, {overall_start_date}")
        pagila_cur.execute(extract_sql, (overall_end_date, overall_start_date))

        # 3. Load extracted data into Staging Table in Rollup DB (batch insert)
        # Using execute_values for efficient batch insertion if psycopg2.extras is available
        # For simplicity here, we'll iterate if not, but execute_values or copy_from is better for large datasets
        
        fetched_rows = 0
        # In a high-performance scenario, instead of fetchall(), you might use fetchmany()
        # and psycopg2.extras.execute_values or COPY for bulk loading.
        # For this script, we'll do a basic iteration with executemany for simplicity.
        
        insert_sql = f"INSERT INTO {staging_table_name} (rental_id, rental_date, return_date) VALUES (%s, %s, %s)"
        batch_size = 500 # Process in batches
        while True:
            batch = pagila_cur.fetchmany(batch_size)
            if not batch:
                break
            rollup_cur.executemany(insert_sql, batch)
            fetched_rows += len(batch)
            print(f"  Loaded {len(batch)} rows into {staging_table_name} (total: {fetched_rows})...")

        rollup_conn.commit()
        print(f"Stage 1 completed. Total {fetched_rows} rows staged into {staging_table_name}.")

    except Exception as e:
        if rollup_conn:
            rollup_conn.rollback()
        print(f"Stage 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if pagila_cur: pagila_cur.close()
        if pagila_conn: pagila_conn.close()
        if rollup_cur: rollup_cur.close()
        if rollup_conn: rollup_conn.close()

# --- Stage 2: Transform from Staging and Load to Final Summary Table ---
def run_stage_2_transform_and_load(rollup_conn_id: str, weeks_to_process: list[date]):
    """
    Transforms data from the staging table in Rollup DB to calculate weekly summaries
    and upserts them into the final weekly_rental_summary table.
    Args:
        rollup_conn_id (str): Airflow Connection ID for the Rollup target database.
        weeks_to_process (list[date]): List of week_start_monday dates to process.
    """
    if not weeks_to_process:
        print("Stage 2: No weeks to process. Skipping transformation.")
        return

    print(f"Stage 2: Transforming staged data for {len(weeks_to_process)} weeks.") # Simplified log
    rollup_conn = None
    rollup_cur = None
    staging_table_name = "stg_rental_activity"
    temp_weeks_table_name = "temp_weeks_to_process_for_run"

    try:
        rollup_conn = get_db_connection_from_airflow_hook(rollup_conn_id)
        rollup_cur = rollup_conn.cursor() # Standard cursor is fine

        # 1. Create and populate a temporary table for weeks_to_process
        print(f"Creating and populating temporary table: {temp_weeks_table_name}")
        rollup_cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {temp_weeks_table_name} (week_start_monday DATE PRIMARY KEY);")
        rollup_cur.execute(f"TRUNCATE TABLE {temp_weeks_table_name};") # Ensure it's empty for this run
        
        # Prepare data for execute_values: list of tuples
        weeks_as_tuples_for_insert = [(w,) for w in weeks_to_process] # Ensure each date is a single-element tuple
        
        if weeks_as_tuples_for_insert:
            insert_weeks_sql = f"INSERT INTO {temp_weeks_table_name} (week_start_monday) VALUES %s"
            execute_values(rollup_cur, insert_weeks_sql, weeks_as_tuples_for_insert, page_size=len(weeks_as_tuples_for_insert))
            print(f"Populated {temp_weeks_table_name} with {rollup_cur.rowcount} week(s).")
        else:
            print(f"{temp_weeks_table_name} not populated as weeks_to_process is empty.")
            # If weeks_to_process was empty, we should have returned earlier, but double-check
            rollup_conn.commit() # Commit creation/truncation even if no weeks inserted
            return

        # 2. Main transformation SQL now refers to the temp table, no %s for data parameters
        transform_upsert_sql = f"""
            WITH relevant_weeks AS (
                SELECT week_start_monday FROM {temp_weeks_table_name}
            ),
            weekly_aggregates AS (
                SELECT
                    rw.week_start_monday,
                    COUNT(DISTINCT CASE
                        WHEN sra.rental_date <= (rw.week_start_monday + INTERVAL '6 days')
                        AND (sra.return_date IS NULL OR sra.return_date > (rw.week_start_monday + INTERVAL '6 days'))
                        THEN sra.rental_id
                    END) AS calculated_outstanding_rentals,
                    COUNT(DISTINCT CASE
                        WHEN sra.return_date >= rw.week_start_monday
                        AND sra.return_date <= (rw.week_start_monday + INTERVAL '6 days')
                        THEN sra.rental_id
                    END) AS calculated_returned_rentals
                FROM {staging_table_name} sra
                CROSS JOIN relevant_weeks rw
                WHERE sra.rental_date <= (rw.week_start_monday + INTERVAL '6 days')
                  AND (sra.return_date IS NULL OR sra.return_date >= rw.week_start_monday)
                GROUP BY rw.week_start_monday
            )
            INSERT INTO weekly_rental_summary (
                week_beginning, outstanding_rentals, returned_rentals, etl_last_updated_at
            )
            SELECT
                wa.week_start_monday, wa.calculated_outstanding_rentals, wa.calculated_returned_rentals, CURRENT_TIMESTAMP
            FROM weekly_aggregates wa
            ON CONFLICT (week_beginning) DO UPDATE SET
                outstanding_rentals = EXCLUDED.outstanding_rentals,
                returned_rentals = EXCLUDED.returned_rentals,
                etl_last_updated_at = CURRENT_TIMESTAMP;
        """
        
        print("Executing transform and upsert SQL (using temp table for weeks)...")
        rollup_cur.execute(transform_upsert_sql) # No second argument as SQL is fully formed
        print(f"Upserted {rollup_cur.rowcount} rows into weekly_rental_summary.")
        
        rollup_conn.commit()
        print("Stage 2 completed successfully.")

    except Exception as e:
        if rollup_conn:
            rollup_conn.rollback()
        print(f"Stage 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if rollup_cur:
            try:
                # Attempt to drop the temp table if it exists, but don't fail if it doesn't or if cursor/conn is closed
                rollup_cur.execute(f"DROP TABLE IF EXISTS {temp_weeks_table_name};")
                if rollup_conn and not rollup_conn.closed: rollup_conn.commit()
            except Exception as drop_e:
                print(f"Warning: Could not drop temp table {temp_weeks_table_name}: {drop_e}")
            rollup_cur.close()
        if rollup_conn: rollup_conn.close()

# --- Main ETL Orchestration Logic ---
def run_etl_two_stage(pagila_conn_id: str, rollup_conn_id: str):
    """
    Main ETL function using a two-stage approach.
    """
    print(f"Starting TWO-STAGE ETL process. Pagila Conn ID: '{pagila_conn_id}', Rollup Conn ID: '{rollup_conn_id}'.")
    
    # Temporary connection to Rollup to get metadata for date calculations
    # This connection will be closed after fetching metadata.
    # Stage 1 and Stage 2 will manage their own connections.
    # In Airflow, these could be separate tasks, each getting their own hook.
    temp_rollup_conn = None
    temp_rollup_cur = None
    pagila_conn_for_dates = None # Connection to Pagila for min/max dates
    pagila_cur_for_dates = None

    try:
        # --- Date Range Determination (similar to original script) ---
        print("Determining overall processing date range...")
        temp_rollup_conn = get_db_connection_from_airflow_hook(rollup_conn_id)
        temp_rollup_cur = temp_rollup_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        last_processed_week_start = get_last_processed_week(temp_rollup_cur)

        pagila_conn_for_dates = get_db_connection_from_airflow_hook(pagila_conn_id)
        pagila_cur_for_dates = pagila_conn_for_dates.cursor()

        if last_processed_week_start is None:
            pagila_cur_for_dates.execute("SELECT MIN(rental_date)::date FROM rental;")
            min_rental_date_result = pagila_cur_for_dates.fetchone()
            if not min_rental_date_result or not min_rental_date_result[0]:
                print("No rental data found in Pagila DB. ETL cannot proceed.")
                return
            effective_query_series_start_date = min_rental_date_result[0]
            print(f"No last processed week. Query series start from min_rental_date: {effective_query_series_start_date}")
        else:
            effective_query_series_start_date = last_processed_week_start
            print(f"Last processed week: {last_processed_week_start}. Query series start: {effective_query_series_start_date}")

        # Adjust to Monday of that week for consistency
        overall_processing_start_monday = effective_query_series_start_date - timedelta(days=effective_query_series_start_date.weekday())
        print(f"Overall processing start (Monday): {overall_processing_start_monday}")
        
        pagila_cur_for_dates.execute("SELECT MAX(GREATEST(rental_date, COALESCE(return_date, rental_date)))::date FROM rental;")
        max_data_date_result = pagila_cur_for_dates.fetchone()
        if not max_data_date_result or not max_data_date_result[0]:
            print("No max data date found in Pagila DB. No new data to process.")
            return
        overall_processing_end_pagila = max_data_date_result[0]
        print(f"Max data date from Pagila (overall processing end candidate): {overall_processing_end_pagila}")

        today_actual = date.today()
        today_week_start_limit = today_actual - timedelta(days=today_actual.weekday())
        print(f"Today's week start (upper limit for processing): {today_week_start_limit}")

        # Final overall end date for Stage 1 extraction
        # We extract up to the end of data in Pagila, but no later than today's week start
        overall_processing_end_final = min(overall_processing_end_pagila, today_week_start_limit)

        if overall_processing_start_monday > overall_processing_end_final:
            print(f"Overall processing start date {overall_processing_start_monday} is after overall end date {overall_processing_end_final}. No data to process.")
            return
        
        # --- Execute Stage 1: Extract and Stage ---
        run_stage_1_extract_and_stage(
            pagila_conn_id,
            rollup_conn_id,
            overall_processing_start_monday, # Start of the broad window
            overall_processing_end_final     # End of the broad window
        )
        
        # --- Determine Specific Weeks for Stage 2 (based on the broader window & last processed) ---
        # This logic re-uses parts of the original script's week generation
        # but operates on the assumption that staging table now holds all candidate data.
        
        # SQL to get weeks to process based on the overall range, similar to original
        # This query can be run against Pagila or Rollup (if generate_series is available)
        # Here, we run it against Pagila as in the original script for consistency in setup.
        # Alternatively, generate this list in Python using the date range.
        
        sql_get_weeks = """
            SELECT DISTINCT date_trunc('week', series_date)::date AS week_start_monday
            FROM generate_series(
                %s::timestamp,
                %s::timestamp,
                '7 day'::interval
            ) AS series_date
            WHERE date_trunc('week', series_date)::date <= %s;
        """
        
        actual_series_end_for_week_gen = min(overall_processing_end_pagila, today_week_start_limit) # same as overall_processing_end_final
        
        weeks_to_process = []
        if overall_processing_start_monday <= actual_series_end_for_week_gen:
            print(f"Generating list of weeks to process from {overall_processing_start_monday} to {actual_series_end_for_week_gen}, limited by {today_week_start_limit}")
            # Using pagila_cur_for_dates which is already open for this.
            pagila_cur_for_dates.execute(sql_get_weeks, (overall_processing_start_monday, actual_series_end_for_week_gen, today_week_start_limit))
            weeks_to_process_tuples = pagila_cur_for_dates.fetchall()
            weeks_to_process = [row[0] for row in weeks_to_process_tuples]
        else:
            print(f"Adjusted query series start {overall_processing_start_monday} is after actual series end {actual_series_end_for_week_gen}. No weeks to generate.")

        # Filter weeks: only process weeks that are >= last_processed_week_start (if it exists)
        # And also re-includes the last_processed_week_start for potential updates.
        if last_processed_week_start:
            weeks_to_process = [w for w in weeks_to_process if w >= last_processed_week_start]
        
        # Further filter: ensure weeks are not in the future relative to today_week_start_limit
        weeks_to_process = [w for w in weeks_to_process if w <= today_week_start_limit]
        
        # Remove duplicates and sort
        weeks_to_process = sorted(list(set(weeks_to_process)))

        if not weeks_to_process:
            print("ETL (Two-Stage) finished: No new weeks to calculate after date generation and filtering.")
            return

        print(f"Final list of weeks to process in Stage 2: {weeks_to_process}")

        # --- Execute Stage 2: Transform and Load ---
        run_stage_2_transform_and_load(rollup_conn_id, weeks_to_process)

        print("TWO-STAGE ETL process completed successfully.")

    except Exception as e:
        # General catch-all for the orchestrator, specific stages have their own
        print(f"TWO-STAGE ETL process FAILED in orchestrator: {e}")
        import traceback
        traceback.print_exc()
        # In Airflow, this would fail the main task
    finally:
        print("Closing temporary/metadata database cursors and connections in orchestrator.")
        if temp_rollup_cur: temp_rollup_cur.close()
        if temp_rollup_conn: temp_rollup_conn.close()
        if pagila_cur_for_dates: pagila_cur_for_dates.close()
        if pagila_conn_for_dates: pagila_conn_for_dates.close()
        print("Orchestrator database resources closed.")

# Example of how this might be called (e.g., by an Airflow PythonOperator)
# if __name__ == '__main__':
#     # Replace with your actual Airflow Connection IDs
#     pagila_connection_id = "pagila_db" 
#     rollup_connection_id = "rollup_db"
#
#     print("Simulating ETL run (if script is run directly)...")
#     # Mock Airflow Hook for local testing if needed, or set up real connections
#     # For this example, we assume connections are set up in Airflow
#     # and this script is part of a DAG.
#     # run_etl_two_stage(pagila_connection_id, rollup_connection_id)
#     print("To run, integrate this into an Airflow DAG and provide connection IDs.") 