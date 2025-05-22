import psycopg2
import psycopg2.extras
from datetime import date, timedelta, datetime, timezone # Added timezone
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

# --- Helper: Get Last Processed Week from weekly_rental_summary ---
def get_last_processed_week_from_summary(rollup_cur):
    """Fetches the last processed week_beginning date from the weekly_rental_summary table."""
    rollup_cur.execute("SELECT MAX(week_beginning) FROM weekly_rental_summary;")
    result = rollup_cur.fetchone()
    last_week = result[0] if result and result[0] is not None else None
    print(f"Last processed week from summary table: {last_week}")
    return last_week

# --- Helper: Ensure Replica and Watermark Tables Exist ---
def ensure_replica_and_watermark_tables_exist(rollup_cur):
    """
    Ensures that 'rental_replica' and 'etl_watermarks' tables exist in the Rollup DB.
    This is for storing a copy of source data and ETL watermarks.
    """
    print("Ensuring 'rental_replica' and 'etl_watermarks' tables exist...")
    # rental_replica: Stores a copy of rental data from source, updated incrementally.
    # last_update_source: Stores the 'last_update' timestamp from the Pagila source table.
    rollup_cur.execute("""
        CREATE TABLE IF NOT EXISTS rental_replica (
            rental_id INTEGER PRIMARY KEY,
            rental_date TIMESTAMP,
            return_date TIMESTAMP,
            last_update_source TIMESTAMP NOT NULL
        );
    """)
    # etl_watermarks: Tracks the last successfully processed 'last_update' timestamp from source tables.
    rollup_cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_watermarks (
            source_table_name TEXT PRIMARY KEY,
            last_processed_watermark_ts TIMESTAMP NOT NULL
        );
    """)
    print("'rental_replica' and 'etl_watermarks' tables checked/created.")

# --- Helper: Get ETL Watermark ---
def get_etl_watermark(rollup_cur, source_table_name: str) -> datetime:
    """
    Fetches the last successfully processed 'last_update' timestamp for a given source table.
    Returns a very old UTC-aware timestamp if no watermark is found (for initial run).
    """
    rollup_cur.execute(
        "SELECT last_processed_watermark_ts FROM etl_watermarks WHERE source_table_name = %s;",
        (source_table_name,)
    )
    result = rollup_cur.fetchone()
    watermark_val = None
    if result and result[0]:
        watermark_val = result[0]
        print(f"Retrieved raw watermark for '{source_table_name}': {watermark_val} (type: {type(watermark_val)}) TZ: {watermark_val.tzinfo}")
        if watermark_val.tzinfo is None or watermark_val.tzinfo.utcoffset(watermark_val) is None:
            # If naive or tzinfo is None/not effective, assume it's UTC and make it aware
            watermark_val = watermark_val.replace(tzinfo=timezone.utc)
            print(f"  Localized naive/problematic DB watermark to UTC: {watermark_val}")
        else:
            # If aware, convert to UTC for consistency
            watermark_val = watermark_val.astimezone(timezone.utc)
            print(f"  Converted DB watermark to UTC: {watermark_val}")
        return watermark_val
    else:
        # Default to a very old date if no watermark exists, UTC aware
        default_watermark = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        print(f"No watermark found for '{source_table_name}'. Using default UTC aware: {default_watermark}")
        return default_watermark

# --- Helper: Update ETL Watermark ---
def update_etl_watermark(rollup_cur, source_table_name: str, new_watermark_ts: datetime):
    """
    Updates the 'last_processed_watermark_ts' for a given source table in the Rollup DB.
    """
    print(f"Updating watermark for '{source_table_name}' to: {new_watermark_ts}")
    rollup_cur.execute(
        """
        INSERT INTO etl_watermarks (source_table_name, last_processed_watermark_ts)
        VALUES (%s, %s)
        ON CONFLICT (source_table_name) DO UPDATE
        SET last_processed_watermark_ts = EXCLUDED.last_processed_watermark_ts;
        """,
        (source_table_name, new_watermark_ts)
    )
    print(f"Watermark for '{source_table_name}' updated successfully.")


# --- Stage 1: Replicate Delta from Source to rental_replica in Target DB ---
def run_stage_1_replicate_delta(pagila_conn_id: str, rollup_conn_id: str):
    """
    Extracts new/updated rental data from Pagila (based on 'last_update' watermark)
    and UPSERTS it into the 'rental_replica' table in the Rollup target DB.
    Returns a list of affected rental_ids and the new potential watermark.
    Assumes Pagila 'rental' table has a 'last_update' column (TIMESTAMP/TIMESTAMPTZ).
    """
    print("Stage 1: Replicating delta from Pagila 'rental' to Rollup 'rental_replica'...")
    pagila_conn = None
    rollup_conn = None
    pagila_cur = None
    rollup_cur = None

    source_table_for_watermark = "rental" # The source table in Pagila we are tracking
    affected_rental_ids = set()
    current_max_source_last_update = None

    try:
        rollup_conn = get_db_connection_from_airflow_hook(rollup_conn_id)
        rollup_cur = rollup_conn.cursor()

        # 1. Get the last processed watermark for the 'rental' table
        previous_watermark = get_etl_watermark(rollup_cur, source_table_for_watermark)

        pagila_conn = get_db_connection_from_airflow_hook(pagila_conn_id)
        pagila_cur = pagila_conn.cursor()

        # 2. Determine the current maximum 'last_update' value in the source table.
        # This will be the upper bound for this run and the candidate for the next watermark.
        pagila_cur.execute("SELECT MAX(last_update) FROM rental;") # Assuming 'last_update' column exists
        max_lu_result = pagila_cur.fetchone()
        if not max_lu_result or not max_lu_result[0]:
            print("No 'last_update' data found in source 'rental' table or table is empty. No new data to replicate.")
            rollup_conn.commit() # Commit any table creations if they happened earlier by ensure_tables
            return [], previous_watermark # Return empty list and old watermark

        current_max_source_last_update = max_lu_result[0]
        print(f"Retrieved raw MAX(last_update) from source: {current_max_source_last_update} (type: {type(current_max_source_last_update)}) TZ: {current_max_source_last_update.tzinfo}")

        # Ensure current_max_source_last_update is UTC aware for comparison
        if current_max_source_last_update:
            if current_max_source_last_update.tzinfo is None or current_max_source_last_update.tzinfo.utcoffset(current_max_source_last_update) is None:
                # If naive or tzinfo is None/not effective, assume it's UTC and make it aware
                current_max_source_last_update = current_max_source_last_update.replace(tzinfo=timezone.utc)
                print(f"  Localized naive/problematic source MAX(last_update) to UTC: {current_max_source_last_update}")
            else:
                # If aware, convert to UTC
                current_max_source_last_update = current_max_source_last_update.astimezone(timezone.utc)
                print(f"  Converted source MAX(last_update) to UTC: {current_max_source_last_update}")

        if current_max_source_last_update <= previous_watermark:
            print(f"Current max 'last_update' ({current_max_source_last_update}) in source is not newer than previous watermark ({previous_watermark}). No new data to replicate.")
            rollup_conn.commit()
            return [], previous_watermark

        print(f"Extracting data from Pagila 'rental' where last_update > '{previous_watermark}' and last_update <= '{current_max_source_last_update}'.")
        
        # 3. Extract records from Pagila source DB updated since the last watermark
        extract_sql = """
            SELECT rental_id, rental_date, return_date, last_update
            FROM rental
            WHERE last_update > %s AND last_update <= %s;
        """
        pagila_cur.execute(extract_sql, (previous_watermark, current_max_source_last_update))

        # 4. UPSERT extracted data into 'rental_replica' in Rollup DB
        upsert_sql = """
            INSERT INTO rental_replica (rental_id, rental_date, return_date, last_update_source)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (rental_id) DO UPDATE SET
                rental_date = EXCLUDED.rental_date,
                return_date = EXCLUDED.return_date,
                last_update_source = EXCLUDED.last_update_source
            -- Only update if the incoming record's last_update_source is newer
            WHERE EXCLUDED.last_update_source > rental_replica.last_update_source;
        """
        # A simpler version if last_update is guaranteed to be always increasing for a given rental_id for subsequent changes:
        # ON CONFLICT (rental_id) DO UPDATE SET
        #     rental_date = EXCLUDED.rental_date,
        #     return_date = EXCLUDED.return_date,
        #     last_update_source = EXCLUDED.last_update_source;

        batch_size = 500
        total_rows_processed = 0
        while True:
            batch = pagila_cur.fetchmany(batch_size)
            if not batch:
                break
            
            # Collect affected rental_ids from this batch
            for row in batch:
                affected_rental_ids.add(row[0]) # rental_id is the first element

            # Perform UPSERT for the batch
            # executemany is suitable for INSERTs. For UPSERT with ON CONFLICT,
            # execute_values or a loop with execute might be more common or robust depending on driver.
            # For psycopg2, executemany should work with ON CONFLICT.
            rollup_cur.executemany(upsert_sql, batch)
            total_rows_processed += len(batch)
            print(f"  Processed {len(batch)} rows into rental_replica (total for this run: {total_rows_processed})...")

        rollup_conn.commit()
        print(f"Stage 1 completed. {total_rows_processed} rows processed into 'rental_replica'. {len(affected_rental_ids)} unique rental_ids affected.")
        
        # Return the set of affected IDs and the watermark that was used as the *upper bound* for this run.
        return list(affected_rental_ids), current_max_source_last_update

    except Exception as e:
        if rollup_conn:
            rollup_conn.rollback()
        print(f"Stage 1 (Replicate Delta) FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if pagila_cur: pagila_cur.close()
        if pagila_conn: pagila_conn.close()
        if rollup_cur: rollup_cur.close()
        if rollup_conn: rollup_conn.close()

# --- Stage 2: Recalculate Summaries from rental_replica ---
def run_stage_2_recalculate_from_replica(rollup_conn_id: str,
                                         affected_rental_ids: list[int],
                                         initial_last_processed_week_from_summary: date | None,
                                         today_week_start_limit: date):
    """
    Recalculates weekly summaries using data from 'rental_replica'.
    Determines weeks to process based on affected_rental_ids and the overall data range for initial runs.
    Args:
        rollup_conn_id (str): Airflow Connection ID for the Rollup target database.
        affected_rental_ids (list[int]): List of rental_ids that were changed in Stage 1.
        initial_last_processed_week_from_summary (date | None): The most recent week from weekly_rental_summary
                                                               before this ETL run started.
        today_week_start_limit (date): The Monday of the current week, acting as an upper bound for processing.
    """
    print("Stage 2: Recalculating summaries from 'rental_replica'.")
    rollup_conn = None
    rollup_cur = None
    temp_weeks_table_name = "temp_weeks_to_recalculate_for_run"
    weeks_to_recalculate = set()

    try:
        rollup_conn = get_db_connection_from_airflow_hook(rollup_conn_id)
        rollup_cur = rollup_conn.cursor()

        # 1. Determine weeks to recalculate
        print("Determining weeks to recalculate...")

        # a) Always consider the last processed week for recalculation if it exists
        if initial_last_processed_week_from_summary:
            weeks_to_recalculate.add(initial_last_processed_week_from_summary)
            print(f"  Added initial_last_processed_week_from_summary: {initial_last_processed_week_from_summary}")

        # b) Add weeks affected by changed rental_ids
        if affected_rental_ids:
            print(f"  Processing {len(affected_rental_ids)} affected rental_ids to find relevant weeks...")
            # SQL to get rental_date and return_date for affected_rental_ids from rental_replica
            # Using ANY(%s) with a list of IDs passed as a tuple.
            # Ensure affected_rental_ids is not empty before this query.
            query_affected_dates = "SELECT rental_date, return_date FROM rental_replica WHERE rental_id = ANY(%s);"
            rollup_cur.execute(query_affected_dates, (affected_rental_ids,)) # Pass list as a tuple
            
            for r_date, ret_date in rollup_cur.fetchall():
                if r_date:
                    week_of_rental = r_date.date() - timedelta(days=r_date.date().weekday())
                    weeks_to_recalculate.add(week_of_rental)
                if ret_date:
                    week_of_return = ret_date.date() - timedelta(days=ret_date.date().weekday())
                    weeks_to_recalculate.add(week_of_return)
            print(f"  Found {len(weeks_to_recalculate)} unique weeks after considering affected_rental_ids.")
        else:
            print("  No affected_rental_ids from Stage 1.")

        # c) For initial run (if weekly_rental_summary was empty), calculate range from rental_replica
        if initial_last_processed_week_from_summary is None:
            print("  Initial run detected (no prior summary data). Determining full range from 'rental_replica'.")
            rollup_cur.execute("""
                SELECT MIN(rental_date::date), MAX(COALESCE(return_date, rental_date)::date)
                FROM rental_replica;
            """)
            min_max_dates_result = rollup_cur.fetchone()
            if min_max_dates_result and min_max_dates_result[0] and min_max_dates_result[1]:
                overall_min_date = min_max_dates_result[0]
                overall_max_date = min_max_dates_result[1]
                print(f"  rental_replica data range: {overall_min_date} to {overall_max_date}")
                
                current_week_iter = overall_min_date - timedelta(days=overall_min_date.weekday())
                while current_week_iter <= overall_max_date:
                    weeks_to_recalculate.add(current_week_iter)
                    current_week_iter += timedelta(days=7)
                print(f"  Generated {len(weeks_to_recalculate)} unique weeks for initial full load.")
            else:
                print("  'rental_replica' is empty or contains no dates. No weeks for initial full load.")

        # Filter weeks: ensure they are not in the future relative to today_week_start_limit
        final_weeks_to_process = sorted([
            w for w in weeks_to_recalculate if w <= today_week_start_limit
        ])

        if not final_weeks_to_process:
            print("Stage 2: No weeks to process after filtering. Skipping summary calculation.")
            rollup_conn.commit() # Commit any DDL if temp table was managed differently.
            return

        print(f"Final list of {len(final_weeks_to_process)} weeks to process in Stage 2: {final_weeks_to_process}")

        # 2. Create and populate a temporary table for these weeks
        print(f"Creating and populating temporary table: {temp_weeks_table_name}")
        rollup_cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {temp_weeks_table_name} (week_start_monday DATE PRIMARY KEY);")
        rollup_cur.execute(f"TRUNCATE TABLE {temp_weeks_table_name};")
        
        weeks_as_tuples_for_insert = [(w,) for w in final_weeks_to_process]
        if weeks_as_tuples_for_insert:
            insert_weeks_sql = f"INSERT INTO {temp_weeks_table_name} (week_start_monday) VALUES %s"
            psycopg2.extras.execute_values(rollup_cur, insert_weeks_sql, weeks_as_tuples_for_insert, page_size=len(weeks_as_tuples_for_insert))
            print(f"Populated {temp_weeks_table_name} with {rollup_cur.rowcount} week(s).")

        # 3. Main transformation SQL using rental_replica
        # This SQL calculates summaries for the weeks listed in 'temp_weeks_table_name'
        # by querying 'rental_replica'.
        transform_upsert_sql = f"""
            WITH relevant_weeks AS (
                SELECT week_start_monday FROM {temp_weeks_table_name}
            ),
            weekly_aggregates AS (
                SELECT
                    rw.week_start_monday,
                    COUNT(DISTINCT CASE
                        WHEN rr.rental_date <= (rw.week_start_monday + INTERVAL '6 days') -- Rented by end of this week
                        AND (rr.return_date IS NULL OR rr.return_date > (rw.week_start_monday + INTERVAL '6 days')) -- And not returned by end of this week
                        THEN rr.rental_id
                    END) AS calculated_outstanding_rentals,
                    COUNT(DISTINCT CASE
                        WHEN rr.return_date >= rw.week_start_monday -- Returned on or after start of this week
                        AND rr.return_date <= (rw.week_start_monday + INTERVAL '6 days') -- And returned by end of this week
                        THEN rr.rental_id
                    END) AS calculated_returned_rentals
                FROM rental_replica rr -- Querying the replica table
                CROSS JOIN relevant_weeks rw
                -- Optimization: Filter records from rental_replica that could possibly affect any relevant week
                WHERE rr.rental_date <= (rw.week_start_monday + INTERVAL '6 days') -- Rental started by end of week
                  AND (rr.return_date IS NULL OR rr.return_date >= rw.week_start_monday) -- Not returned before start of week
                GROUP BY rw.week_start_monday
            )
            INSERT INTO weekly_rental_summary (
                week_beginning, outstanding_rentals, returned_rentals, etl_last_updated_at
            )
            SELECT
                wa.week_start_monday,
                COALESCE(wa.calculated_outstanding_rentals, 0), -- Ensure 0 if no activity
                COALESCE(wa.calculated_returned_rentals, 0),  -- Ensure 0 if no activity
                CURRENT_TIMESTAMP
            FROM weekly_aggregates wa
            ON CONFLICT (week_beginning) DO UPDATE SET
                outstanding_rentals = EXCLUDED.outstanding_rentals,
                returned_rentals = EXCLUDED.returned_rentals,
                etl_last_updated_at = CURRENT_TIMESTAMP;
        """
        
        print("Executing transform and upsert SQL (using rental_replica and temp table for weeks)...")
        rollup_cur.execute(transform_upsert_sql)
        print(f"Upserted/updated {rollup_cur.rowcount} rows into weekly_rental_summary.")
        
        rollup_conn.commit()
        print("Stage 2 completed successfully.")

    except Exception as e:
        if rollup_conn:
            rollup_conn.rollback()
        print(f"Stage 2 (Recalculate from Replica) FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if rollup_cur:
            try:
                rollup_cur.execute(f"DROP TABLE IF EXISTS {temp_weeks_table_name};")
                if rollup_conn and not rollup_conn.closed: rollup_conn.commit()
            except Exception as drop_e:
                print(f"Warning: Could not drop temp table {temp_weeks_table_name}: {drop_e}")
            rollup_cur.close()
        if rollup_conn: rollup_conn.close()

# --- Main ETL Orchestration Logic (Replica Approach) ---
def run_etl_replica_approach(pagila_conn_id: str, rollup_conn_id: str):
    """
    Main ETL function using the replica and watermark approach.
    Orchestrates delta replication (Stage 1) and summary recalculation (Stage 2).
    """
    print(f"Starting REPLICA APPROACH ETL process. Pagila Conn ID: '{pagila_conn_id}', Rollup Conn ID: '{rollup_conn_id}'.")
    
    # Determine current date and today's week start limit once
    today_actual = date.today()
    # Calculations are typically for weeks up to and including the one that just ended,
    # or the current week if it's considered complete enough.
    # Using Monday of the current week as a strict upper limit for week_beginning.
    today_week_start_limit = today_actual - timedelta(days=today_actual.weekday())
    print(f"Today's date: {today_actual}, today's week start (upper limit for processing): {today_week_start_limit}")

    temp_rollup_conn_for_metadata = None
    temp_rollup_cur_for_metadata = None
    initial_last_processed_week_from_summary = None
    new_watermark_candidate = None # This will be the high watermark from the source data processed in Stage 1

    try:
        # --- Initial Setup: Ensure tables exist and get metadata ---
        print("Performing initial setup (checking tables, getting initial metadata)...")
        temp_rollup_conn_for_metadata = get_db_connection_from_airflow_hook(rollup_conn_id)
        temp_rollup_cur_for_metadata = temp_rollup_conn_for_metadata.cursor()

        ensure_replica_and_watermark_tables_exist(temp_rollup_cur_for_metadata)
        initial_last_processed_week_from_summary = get_last_processed_week_from_summary(temp_rollup_cur_for_metadata)
        
        temp_rollup_conn_for_metadata.commit() # Commit table creations
        print("Initial setup completed.")

        # --- Execute Stage 1: Replicate Delta ---
        # This stage fetches changes from Pagila based on watermark and updates rental_replica.
        # It returns the list of rental_ids affected and the highest 'last_update' timestamp processed from source.
        affected_rental_ids, new_watermark_candidate = run_stage_1_replicate_delta(
            pagila_conn_id,
            rollup_conn_id
        )

        # --- Execute Stage 2: Recalculate Summaries from Replica ---
        # This stage uses rental_replica and the list of affected IDs / initial state
        # to determine which weeks' summaries to recalculate.
        run_stage_2_recalculate_from_replica(
            rollup_conn_id,
            affected_rental_ids,
            initial_last_processed_week_from_summary,
            today_week_start_limit
        )

        # --- Final Step: Update Watermark if everything was successful ---
        # Only update the watermark if Stage 1 and Stage 2 were successful and
        # new_watermark_candidate is valid.
        if new_watermark_candidate:
            # Re-fetch current watermark to ensure we are not overwriting a more recent one
            # if multiple ETLs could run (though typically not for the same source_table_name).
            # For simplicity, we assume this ETL is the sole writer for this watermark.
            # This connection is for the final watermark update.
            print(f"Attempting to update ETL watermark with candidate: {new_watermark_candidate}")
            conn_for_watermark_update = None
            cur_for_watermark_update = None
            try:
                conn_for_watermark_update = get_db_connection_from_airflow_hook(rollup_conn_id)
                cur_for_watermark_update = conn_for_watermark_update.cursor()
                update_etl_watermark(cur_for_watermark_update, "rental", new_watermark_candidate)
                conn_for_watermark_update.commit()
            finally:
                if cur_for_watermark_update: cur_for_watermark_update.close()
                if conn_for_watermark_update: conn_for_watermark_update.close()
        else:
            print("No new watermark candidate from Stage 1, or Stage 1 indicated no new data. Watermark not updated.")

        print("REPLICA APPROACH ETL process completed successfully.")

    except Exception as e:
        print(f"REPLICA APPROACH ETL process FAILED in orchestrator: {e}")
        import traceback
        traceback.print_exc()
        # If an error occurs, the watermark is NOT updated, ensuring data will be re-processed next run.
        raise # Re-raise to fail the Airflow task
    finally:
        print("Closing temporary metadata database cursors and connections in orchestrator.")
        if temp_rollup_cur_for_metadata: temp_rollup_cur_for_metadata.close()
        if temp_rollup_conn_for_metadata: temp_rollup_conn_for_metadata.close()
        print("Orchestrator database resources closed.")

# Example of how this might be called (e.g., by an Airflow PythonOperator)
# Important: Change the main function name in your Airflow DAG to 'run_etl_replica_approach'
if __name__ == '__main__':
    # Replace with your actual Airflow Connection IDs
    pagila_connection_id = "pagila_db"  # Example, use your Airflow connection ID
    rollup_connection_id = "rollup_db"  # Example, use your Airflow connection ID

    print("Simulating ETL run (if script is run directly)...")
    # This direct run is for local testing; in Airflow, the operator calls run_etl_replica_approach.
    # Ensure your environment can resolve PostgresHook or mock it for local tests.
    # For a real local test, you'd need to set up mock connections or have DBs accessible.
    
    # To test, you would call:
    # run_etl_replica_approach(pagila_connection_id, rollup_connection_id)
    
    print("To run, integrate 'run_etl_replica_approach' into an Airflow DAG and provide connection IDs.")
    print("Ensure the source 'rental' table in Pagila has a 'last_update' column (TIMESTAMP/TIMESTAMPTZ).")

# Rename the old main function if it exists or ensure it's not called.
# The new entry point for Airflow should be run_etl_replica_approach.
# def run_etl_two_stage(pagila_conn_id: str, rollup_conn_id: str):
# print("This is the old run_etl_two_stage function. It should be replaced by run_etl_replica_approach.")
# pass

# Rename the old main function if it exists or ensure it's not called.
# The new entry point for Airflow should be run_etl_replica_approach.
# def run_etl_two_stage(pagila_conn_id: str, rollup_conn_id: str):
# print("This is the old run_etl_two_stage function. It should be replaced by run_etl_replica_approach.")
# pass 