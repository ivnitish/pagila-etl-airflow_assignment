import psycopg2
import psycopg2.extras # For DictCursor if desired
import pandas as pd
from datetime import datetime, timedelta, timezone

# --- Configuration ---
PAGILA_CONN_ID = "pagila_postgres_connection"  # Airflow Connection ID for Pagila
ROLLUP_CONN_ID = "rollup_postgres_connection"  # Airflow Connection ID for Rollup
ETL_PROCESS_NAME = "pagila_weekly_rental_summary"
DEFAULT_WATERMARK_START_DATE = datetime(1900, 1, 1, tzinfo=timezone.utc) # Make it UTC aware

# --- Helper to get Airflow connection details (if running in Airflow) ---
def get_db_connection_details(conn_id):
    """
    Retrieves database connection parameters using Airflow's BaseHook.

    This function fetches connection details (host, port, dbname, user, password)
    for a given Airflow connection ID. It's used by the DAG to pass
    credentials to the ETL logic and can also be used for local testing
    if the local environment is configured to resolve Airflow connections.

    Args:
        conn_id (str): The Airflow connection ID to retrieve.

    Returns:
        dict: A dictionary containing database connection parameters.
    """
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "port": conn.port,
        "dbname": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }

def run_incremental_etl(pagila_conn_params, rollup_conn_params):
    """
    Main orchestrator for the incremental ETL process.
    """
    pagila_conn = None
    rollup_conn = None

    try:
        print("Starting incremental ETL for Pagila weekly summary...")

        # --- Database Connections ---
        print(f"Connecting to Pagila DB using: {pagila_conn_params.get('host')}")
        pagila_conn = psycopg2.connect(**pagila_conn_params)
        pagila_cur = pagila_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("Connected to Pagila DB.")

        print(f"Connecting to Rollup DB using: {rollup_conn_params.get('host')}")
        rollup_conn = psycopg2.connect(**rollup_conn_params)
        rollup_cur = rollup_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("Connected to Rollup DB.")

        # Ensure weekly_rental_summary table exists (idempotency)
        create_summary_table_sql = """
        CREATE TABLE IF NOT EXISTS weekly_rental_summary (
            week_beginning DATE PRIMARY KEY,
            "OutstandingRentals" INTEGER,
            "ReturnedRentals" INTEGER,
            newly_rented_during_week INTEGER,
            net_change_in_outstanding INTEGER,
            last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        rollup_cur.execute(create_summary_table_sql)
        rollup_conn.commit()
        print("Ensured 'weekly_rental_summary' table exists in Rollup DB.")

        # Ensure etl_watermarks table exists (idempotency)
        create_watermarks_table_sql = """
        CREATE TABLE IF NOT EXISTS etl_watermarks (
            process_name VARCHAR(255) PRIMARY KEY,
            last_successful_update_timestamp TIMESTAMP WITHOUT TIME ZONE
        );
        """
        rollup_cur.execute(create_watermarks_table_sql)
        rollup_conn.commit()
        print("Ensured 'etl_watermarks' table exists in Rollup DB.")

        # --- New Step: Proactively Reset Watermark if Target Table is Empty ---
        print("Step 0: Checking if target table 'weekly_rental_summary' is empty to potentially reset watermark...")
        rollup_cur.execute("SELECT 1 FROM weekly_rental_summary LIMIT 1;")
        target_table_has_any_data = rollup_cur.fetchone()

        if not target_table_has_any_data:
            print("  Target table 'weekly_rental_summary' is empty. Resetting watermark to default start date to ensure full load.")
            reset_watermark_sql = """
            INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (process_name) DO UPDATE SET
                last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            # Store naive UTC version of DEFAULT_WATERMARK_START_DATE
            rollup_cur.execute(reset_watermark_sql, (ETL_PROCESS_NAME, DEFAULT_WATERMARK_START_DATE.replace(tzinfo=None)))
            rollup_conn.commit()
            print(f"  Watermark for '{ETL_PROCESS_NAME}' reset to: {DEFAULT_WATERMARK_START_DATE.replace(tzinfo=None)}")
        else:
            print("  Target table 'weekly_rental_summary' is not empty. Proceeding with existing watermark.")

        # --- 1. Determine Time Window for Changes ---
        print("Step 1: Determining time window for changes...")
        rollup_cur.execute(
            "SELECT last_successful_update_timestamp FROM etl_watermarks WHERE process_name = %s",
            (ETL_PROCESS_NAME,)
        )
        watermark_result = rollup_cur.fetchone()
        
        previous_watermark_naive = watermark_result['last_successful_update_timestamp'] if watermark_result else None
        if previous_watermark_naive:
            # Assuming timestamp without time zone from etl_watermarks is intended to be UTC
            previous_watermark = previous_watermark_naive.replace(tzinfo=timezone.utc)
        else:
            previous_watermark = DEFAULT_WATERMARK_START_DATE # This is already UTC aware
        print(f"  Previous watermark (UTC): {previous_watermark}")

        pagila_cur.execute("SELECT MAX(last_update) FROM rental;")
        current_max_source_update_result = pagila_cur.fetchone()
        
        current_max_source_update_raw = current_max_source_update_result[0] if current_max_source_update_result and current_max_source_update_result[0] else None

        if current_max_source_update_raw:
            current_max_source_update = current_max_source_update_raw
            if current_max_source_update.tzinfo is None:
                 # This case implies rental.last_update is TIMESTAMP WITHOUT TIME ZONE
                 # and psycopg2 returned a naive datetime. We assume it's UTC.
                 print("Warning: MAX(last_update) from source is timezone-naive. Assuming UTC and making it aware.")
                 current_max_source_update = current_max_source_update.replace(tzinfo=timezone.utc)
        else:
            # If rental table is empty or all last_update are NULL, effectively no new data beyond previous watermark.
            # Setting it to previous_watermark ensures the subsequent comparison doesn't find "new" data.
            current_max_source_update = previous_watermark
        
        # Now both previous_watermark and current_max_source_update should be UTC-aware.
        # We no longer exit early here solely based on source last_update. 
        # We'll determine Set A (source changes) and Set B (target gaps) then decide.

        print(f"  Current max source update (UTC) from source: {current_max_source_update}") # Renamed for clarity

        # --- 2. Extract Changed Data (Delta Extraction) for Set A ---
        print(f"Step 2: Potentially extracting changed data from source for Set A (if last_update > {previous_watermark})...")
        changed_rentals_df = pd.DataFrame() # Initialize as empty

        if current_max_source_update > previous_watermark:
            delta_sql = """
                SELECT rental_id, rental_date, return_date, last_update
                FROM rental
                WHERE last_update > %s AND last_update <= %s;
            """
            changed_rentals_df = pd.read_sql_query(delta_sql, pagila_conn, params=(previous_watermark, current_max_source_update))
            print(f"  Found {len(changed_rentals_df)} changed/new rental records for Set A based on source last_update.")
        else:
            print(f"  No new rental records for Set A based on source last_update (current_max_source_update: {current_max_source_update} <= previous_watermark: {previous_watermark}).")

        # --- 3. Identify Affected Weeks ---
        # Step 3a: Identify Set A (weeks affected by source last_update changes)
        print("Step 3a: Identifying Set A (weeks affected by source 'last_update' changes)...")
        # Ensure date columns are datetime objects if dataframe is not empty
        if not changed_rentals_df.empty:
            changed_rentals_df['rental_date'] = pd.to_datetime(changed_rentals_df['rental_date'])
            changed_rentals_df['return_date'] = pd.to_datetime(changed_rentals_df['return_date'], errors='coerce') # Coerce invalid dates to NaT
        else:
            print("  No data in changed_rentals_df for Set A processing.")

        set_a_affected_weeks = set() 
        if not changed_rentals_df.empty: # Process only if there are records
            for _, row in changed_rentals_df.iterrows():
                if pd.notna(row['rental_date']):
                    set_a_affected_weeks.add((row['rental_date'] - timedelta(days=row['rental_date'].weekday())).date())
                if pd.notna(row['return_date']):
                    set_a_affected_weeks.add((row['return_date'] - timedelta(days=row['return_date'].weekday())).date())
        
        print(f"  Set A: Identified {len(set_a_affected_weeks)} unique weeks: {sorted(list(set_a_affected_weeks))}")

        # Step 3b: Identify Weeks Missing from Target's End (Set B) ---
        # This logic remains the same as before
        print("Step 3b: Identifying Set B (weeks potentially missing from the end of the target summary table)...")
        set_b_missing_target_weeks = set()

        pagila_cur.execute("SELECT MAX(GREATEST(rental_date, COALESCE(return_date, rental_date))) AS max_activity_date FROM rental;")
        max_source_activity_date_result = pagila_cur.fetchone()
        max_expected_week_in_source = None
        if max_source_activity_date_result and max_source_activity_date_result['max_activity_date']:
            max_activity_dt_obj = pd.to_datetime(max_source_activity_date_result['max_activity_date'])
            max_expected_week_in_source = (max_activity_dt_obj - timedelta(days=max_activity_dt_obj.weekday())).date()
            print(f"  Max expected week based on all source activity: {max_expected_week_in_source}")
        else:
            print("  Could not determine max expected week from source (source might be empty or no relevant dates found).")

        rollup_cur.execute("SELECT MAX(week_beginning) AS max_target_week FROM weekly_rental_summary;")
        max_target_week_result = rollup_cur.fetchone()
        actual_max_week_in_target = None
        if max_target_week_result and max_target_week_result['max_target_week']:
            actual_max_week_in_target = pd.to_datetime(max_target_week_result['max_target_week']).date()
            print(f"  Max week actually present in target summary table: {actual_max_week_in_target}")
        else:
            print("  Target summary table is empty or contains no week_beginning dates.")

        if max_expected_week_in_source: 
            start_date_for_set_b = None
            if actual_max_week_in_target is None: 
                # If target is empty, Set B will try to fill from the earliest source data up to max_expected_week_in_source
                # Need to find the MIN source week to start from in this specific scenario for Set B
                pagila_cur.execute("SELECT MIN(GREATEST(rental_date, COALESCE(return_date, rental_date))) AS min_activity_date FROM rental WHERE rental_date IS NOT NULL;") # ensure rental_date exists for a valid week
                min_source_activity_date_result = pagila_cur.fetchone()
                if min_source_activity_date_result and min_source_activity_date_result['min_activity_date']:
                    min_activity_dt_obj = pd.to_datetime(min_source_activity_date_result['min_activity_date'])
                    start_date_for_set_b = (min_activity_dt_obj - timedelta(days=min_activity_dt_obj.weekday())).date()
                    print(f"  Target is empty. Set B will aim to fill from earliest source week: {start_date_for_set_b} up to {max_expected_week_in_source}.")
                else:
                    print("  Target is empty, but could not determine earliest source week for Set B population. Set A (if any) will be primary.")
            elif actual_max_week_in_target < max_expected_week_in_source:
                start_date_for_set_b = actual_max_week_in_target + timedelta(weeks=1)
                print(f"  Target max week ({actual_max_week_in_target}) is before source max expected week ({max_expected_week_in_source}). Set B will check from {start_date_for_set_b}.")
            
            if start_date_for_set_b: 
                current_week_to_add = start_date_for_set_b
                while current_week_to_add <= max_expected_week_in_source:
                    set_b_missing_target_weeks.add(current_week_to_add)
                    current_week_to_add += timedelta(weeks=1)
        
        if set_b_missing_target_weeks:
            print(f"  Set B: Identified {len(set_b_missing_target_weeks)} weeks: {sorted(list(set_b_missing_target_weeks))}")

        # Step 3c: Combine Set A and Set B and Determine if Early Exit is Needed
        print("Step 3c: Combining Set A and Set B and determining if processing is needed...")
        affected_weeks_combined = set_a_affected_weeks.union(set_b_missing_target_weeks)
        affected_weeks_list = sorted(list(affected_weeks_combined))

        print(f"  Combined: Total {len(affected_weeks_list)} unique weeks to process: {affected_weeks_list}")
        
        if not affected_weeks_list:
            print("  No weeks identified for processing from either source changes (Set A) or target end-gaps (Set B). Updating watermark and exiting.")
            update_watermark_sql = """
                INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
                VALUES (%s, %s)
                ON CONFLICT (process_name) DO UPDATE SET
                    last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update.replace(tzinfo=None)))
            rollup_conn.commit()
            print(f"  Watermark updated to {current_max_source_update.replace(tzinfo=None)} (naive UTC) as no processing was needed.")
            return # THE ONLY EARLY EXIT POINT (after Step 0)

        # --- 4. Recalculate and Update Summaries for Affected Weeks --- (This step number is now effectively 4)
        print(f"Step 4: Recalculating and updating summaries for {len(affected_weeks_list)} affected week(s)...")
        
        for target_week_start_dt in affected_weeks_list:
            target_week_end_dt = target_week_start_dt + timedelta(days=6)
            print(f"  Processing week: {target_week_start_dt} to {target_week_end_dt}")

            # 4a. Recalculate Summary for 'target_week' from Source
            # This query is based on the logic from query.sql but scoped to a single week.
            # It queries the main 'rental' table in Pagila.
            recalculate_sql = """
            WITH week_boundaries AS (
                SELECT
                    %s::date AS week_start,
                    %s::date AS week_end
            ),
            rentals_in_scope AS ( -- Optimization: only consider rentals that could affect this week
                SELECT rental_id, rental_date, return_date
                FROM rental
                WHERE
                    -- Rentals that started before or during this week
                    rental_date <= (SELECT week_end FROM week_boundaries)
                    AND (
                        -- Rentals that ended during or after this week, or are still outstanding
                        return_date IS NULL OR
                        return_date >= (SELECT week_start FROM week_boundaries)
                    )
            )
            SELECT
                (SELECT week_start FROM week_boundaries) AS week_beginning,
                COUNT(CASE
                    WHEN r.rental_date >= (SELECT week_start FROM week_boundaries) AND r.rental_date <= (SELECT week_end FROM week_boundaries)
                    THEN r.rental_id ELSE NULL
                END) AS newly_rented_during_week,
                COUNT(CASE
                    WHEN r.return_date >= (SELECT week_start FROM week_boundaries) AND r.return_date <= (SELECT week_end FROM week_boundaries)
                    THEN r.rental_id ELSE NULL
                END) AS returned_rentals_during_week,
                (
                    SELECT COUNT(r_inv.rental_id)
                    FROM rental r_inv -- Querying the main rental table for outstanding
                    WHERE
                        r_inv.rental_date <= (SELECT week_end FROM week_boundaries)
                        AND (
                            r_inv.return_date IS NULL
                            OR r_inv.return_date > (SELECT week_end FROM week_boundaries)
                        )
                ) AS outstanding_rentals_at_week_end
            FROM rentals_in_scope r; -- Use rentals_in_scope to narrow down, but calculations might need broader view or direct source query
            """
            # The above query can be simplified if we directly query 'rental' for each metric.
            # Let's use a more direct approach for clarity of recalculation from source for the given week.

            recalculate_sql_direct = """
            SELECT
                %s::date AS week_beginning,
                (
                    SELECT COUNT(r1.rental_id) FROM rental r1
                    WHERE r1.rental_date >= %s::date AND r1.rental_date <= (%s::date + '6 days'::interval)::date
                ) AS newly_rented_during_week,
                (
                    SELECT COUNT(r2.rental_id) FROM rental r2
                    WHERE r2.return_date >= %s::date AND r2.return_date <= (%s::date + '6 days'::interval)::date
                ) AS returned_rentals_during_week,
                (
                    SELECT COUNT(r3.rental_id) FROM rental r3
                    WHERE r3.rental_date <= (%s::date + '6 days'::interval)::date
                    AND (r3.return_date IS NULL OR r3.return_date > (%s::date + '6 days'::interval)::date)
                ) AS outstanding_rentals_at_week_end;
            """
            # Parameters for recalculate_sql_direct:
            # (target_week_start_dt, target_week_start_dt, target_week_start_dt, target_week_start_dt, target_week_start_dt, target_week_start_dt, target_week_start_dt)
            
            pagila_cur.execute(recalculate_sql_direct, (
                target_week_start_dt,
                target_week_start_dt, target_week_start_dt, # for newly_rented
                target_week_start_dt, target_week_start_dt, # for returned_rentals
                target_week_start_dt, target_week_start_dt  # for outstanding
            ))
            summary_data = pagila_cur.fetchone()

            if summary_data:
                net_change = summary_data['newly_rented_during_week'] - summary_data['returned_rentals_during_week']
                
                # 4b. Upsert into Target Summary Table
                upsert_sql = """
                INSERT INTO weekly_rental_summary (
                    week_beginning, "OutstandingRentals", "ReturnedRentals",
                    newly_rented_during_week, net_change_in_outstanding, last_updated
                ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (week_beginning) DO UPDATE SET
                    "OutstandingRentals" = EXCLUDED."OutstandingRentals",
                    "ReturnedRentals" = EXCLUDED."ReturnedRentals",
                    newly_rented_during_week = EXCLUDED.newly_rented_during_week,
                    net_change_in_outstanding = EXCLUDED.net_change_in_outstanding,
                    last_updated = CURRENT_TIMESTAMP;
                """
                rollup_cur.execute(upsert_sql, (
                    summary_data['week_beginning'],
                    summary_data['outstanding_rentals_at_week_end'], # Maps to "OutstandingRentals"
                    summary_data['returned_rentals_during_week'],   # Maps to "ReturnedRentals"
                    summary_data['newly_rented_during_week'],
                    net_change,
                    # last_updated is handled by CURRENT_TIMESTAMP in the SQL
                ))
                print(f"    Upserted summary for week {summary_data['week_beginning']}.")
            else:
                print(f"    No summary data could be calculated for week {target_week_start_dt}. This might be an issue.")
        
        rollup_conn.commit() # Commit all weekly updates

        # --- 5. Update Watermark ---
        print("Step 5: Updating watermark...")
        update_watermark_sql = """
            INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (process_name) DO UPDATE SET
                last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
        """
        # Store naive UTC
        rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update.replace(tzinfo=None)))
        rollup_conn.commit()
        print(f"  Successfully updated watermark for '{ETL_PROCESS_NAME}' to {current_max_source_update.replace(tzinfo=None)} (naive UTC).")

        print("Incremental ETL for Pagila weekly summary finished successfully.")

    except Exception as e:
        print(f"Error during incremental ETL: {e}")
        if rollup_conn:
            rollup_conn.rollback() # Rollback any partial changes in target on error
        # Re-raise the exception so Airflow can mark the task as failed
        raise
    finally:
        # --- 6. Close Connections ---
        if pagila_cur: pagila_cur.close()
        if pagila_conn: pagila_conn.close()
        if rollup_cur: rollup_cur.close()
        if rollup_conn: rollup_conn.close()
        print("Database connections closed.")

# This part is for making it callable from the DAG or for local testing
if __name__ == "__main__":
    print("Attempting to run ETL script directly using Airflow connection configurations...")

    # Ensure your local environment is configured for Airflow (e.g., AIRFLOW_HOME set,
    # and you are in the correct Python environment where Airflow is installed)
    # for get_db_connection_details to work as expected.

    try:
        pagila_conn_params = get_db_connection_details(PAGILA_CONN_ID)
        rollup_conn_params = get_db_connection_details(ROLLUP_CONN_ID)
        
        print(f"Successfully fetched connection details for Pagila: {pagila_conn_params.get('host')}")
        print(f"Successfully fetched connection details for Rollup: {rollup_conn_params.get('host')}")

        # Before running locally:
        # 1. Ensure PostgreSQL is running.
        # 2. Ensure 'pagila' and 'rollup' databases exist.
        # 3. In 'pagila', ensure the 'rental' table has a 'last_update' column (e.g., TIMESTAMP).
        #    If not, you might need to add it and populate it:
        #    ALTER TABLE rental ADD COLUMN last_update TIMESTAMP WITHOUT TIME ZONE;
        #    UPDATE rental SET last_update = COALESCE(return_date, rental_date) + ((random() * 10) || ' days')::interval; -- Example population
        # 4. Create the 'etl_watermarks' and 'weekly_rental_summary' tables in the 'rollup' database using the SQL provided earlier in comments.
        # 5. Ensure Airflow connections PAGILA_CONN_ID and ROLLUP_CONN_ID are defined in your Airflow UI/config.

        run_incremental_etl(pagila_conn_params, rollup_conn_params)

    except Exception as e:
        print(f"Error during local direct run: {e}")
        print("This might be due to Airflow context not being available, or connection IDs not found.")
        print("For a pure standalone test without Airflow's connection management, you would need to")
        print("manually define 'local_pagila_params' and 'local_rollup_params' dictionaries with your DB credentials")
        print("and call run_incremental_etl(local_pagila_params, local_rollup_params) directly.")
        print("Example of manual params (update with your actual details):")
        print("""
        local_pagila_params = {
            "host": "localhost", "port": "5432", "dbname": "pagila",
            "user": "your_user", "password": "your_password"
        }
        local_rollup_params = {
            "host": "localhost", "port": "5432", "dbname": "rollup",
            "user": "your_user", "password": "your_password"
        }
        # run_incremental_etl(local_pagila_params, local_rollup_params)
        """)

    # export AIRFLOW_HOME=~/airflow # Or wherever your Airflow home directory is 