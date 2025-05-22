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
    Retrieves connection details from Airflow.
    This is a simplified version. In a real Airflow environment,
    you'd use BaseHook.get_connection(conn_id).
    For local testing, you might replace this with direct connection strings.
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
            newly_rented_during_week INTEGER,
            returned_rentals_during_week INTEGER,
            net_change_in_outstanding INTEGER,
            outstanding_rentals_at_week_end INTEGER,
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
        if not current_max_source_update_raw: # If there was no MAX(last_update) from source at all
            print("No 'last_update' values found in source rental table. Assuming no new data.")
            # Update watermark to current_max_source_update (which is previous_watermark here) or previous_watermark itself.
            # Update watermark to current_max_source_update to prevent reprocessing the same data if it's a no-op run
            update_watermark_sql = """
                INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
                VALUES (%s, %s)
                ON CONFLICT (process_name) DO UPDATE SET
                    last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            # We store naive UTC timestamps in the watermark table
            rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update.replace(tzinfo=None) if current_max_source_update else None))
            rollup_conn.commit()
            print(f"  Watermark updated to {current_max_source_update.replace(tzinfo=None) if current_max_source_update else None} (naive UTC) as no new data or no source activity.")
            return # Exit early

        # This comparison should now be safe between two aware datetime objects
        if current_max_source_update <= previous_watermark:
            print(f"No new data found based on 'last_update' in source ({current_max_source_update}) compared to previous watermark ({previous_watermark}).")
            update_watermark_sql = """
                INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
                VALUES (%s, %s)
                ON CONFLICT (process_name) DO UPDATE SET
                    last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            # Store naive UTC
            rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update.replace(tzinfo=None)))
            rollup_conn.commit()
            print(f"  Watermark updated to {current_max_source_update.replace(tzinfo=None)} (naive UTC).")
            return # Exit early

        print(f"  Current max source update (UTC): {current_max_source_update}")


        # --- 2. Extract Changed Data (Delta Extraction) ---
        # When passing datetimes to pd.read_sql_query params for psycopg2, it's often best to pass them as strings
        # or ensure psycopg2 handles the conversion correctly. psycopg2 can handle aware datetimes.
        print(f"Step 2: Extracting changed data from source (last_update > {previous_watermark} and <= {current_max_source_update})...")
        delta_sql = """
            SELECT rental_id, rental_date, return_date, last_update
            FROM rental
            WHERE last_update > %s AND last_update <= %s;
        """
        changed_rentals_df = pd.read_sql_query(delta_sql, pagila_conn, params=(previous_watermark, current_max_source_update))
        print(f"  Found {len(changed_rentals_df)} changed/new rental records.")

        if changed_rentals_df.empty:
            print("  No rental records found in the determined time window. Updating watermark and exiting.")
            update_watermark_sql = """
                INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
                VALUES (%s, %s)
                ON CONFLICT (process_name) DO UPDATE SET
                    last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            # Store naive UTC
            rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update.replace(tzinfo=None)))
            rollup_conn.commit()
            print(f"  Watermark updated to {current_max_source_update.replace(tzinfo=None)} (naive UTC).")
            return

        # --- 3. Identify Affected Weeks ---
        print("Step 3: Identifying affected weeks from the delta...")
        # Ensure date columns are datetime objects
        changed_rentals_df['rental_date'] = pd.to_datetime(changed_rentals_df['rental_date'])
        changed_rentals_df['return_date'] = pd.to_datetime(changed_rentals_df['return_date'], errors='coerce') # Coerce invalid dates to NaT

        set_a_affected_weeks = set() # Weeks affected by source last_update changes
        for _, row in changed_rentals_df.iterrows():
            if pd.notna(row['rental_date']):
                set_a_affected_weeks.add((row['rental_date'] - timedelta(days=row['rental_date'].weekday())).date())
            if pd.notna(row['return_date']):
                set_a_affected_weeks.add((row['return_date'] - timedelta(days=row['return_date'].weekday())).date())
        
        print(f"  Set A: Identified {len(set_a_affected_weeks)} unique weeks affected by source 'last_update' changes: {sorted(list(set_a_affected_weeks))}")

        # --- New Step: Identify Weeks Missing from Target's End (Set B) ---
        print("Step 3b: Identifying weeks potentially missing from the end of the target summary table...")
        set_b_missing_target_weeks = set()

        # Get max expected week from source data
        pagila_cur.execute("SELECT MAX(GREATEST(rental_date, COALESCE(return_date, rental_date))) AS max_activity_date FROM rental;")
        max_source_activity_date_result = pagila_cur.fetchone()
        max_expected_week_in_source = None
        if max_source_activity_date_result and max_source_activity_date_result['max_activity_date']:
            # Ensure it's converted to datetime before date operations
            max_activity_dt_obj = pd.to_datetime(max_source_activity_date_result['max_activity_date'])
            max_expected_week_in_source = (max_activity_dt_obj - timedelta(days=max_activity_dt_obj.weekday())).date()
            print(f"  Max expected week based on all source activity: {max_expected_week_in_source}")
        else:
            print("  Could not determine max expected week from source (source might be empty or no relevant dates found).")

        # Get max actual week in target summary table
        rollup_cur.execute("SELECT MAX(week_beginning) AS max_target_week FROM weekly_rental_summary;")
        max_target_week_result = rollup_cur.fetchone()
        actual_max_week_in_target = None
        if max_target_week_result and max_target_week_result['max_target_week']:
            actual_max_week_in_target = pd.to_datetime(max_target_week_result['max_target_week']).date()
            print(f"  Max week actually present in target summary table: {actual_max_week_in_target}")
        else:
            print("  Target summary table is empty or contains no week_beginning dates.")

        if max_expected_week_in_source: # Only proceed if we have a valid upper bound from source
            start_date_for_set_b = None
            if actual_max_week_in_target is None: 
                # Target is empty. We need to fill from the earliest source week up to max_expected_week_in_source
                # However, Set A (driven by old watermark 1900-01-01) should already identify all relevant weeks from source.
                # So, for an empty target, Set B can remain empty, relying on Set A logic.
                print("  Target is empty. Set A (source changes from 1900-01-01) should handle initial population.")
            elif actual_max_week_in_target < max_expected_week_in_source:
                start_date_for_set_b = actual_max_week_in_target + timedelta(weeks=1)
                print(f"  Target max week ({actual_max_week_in_target}) is before source max expected week ({max_expected_week_in_source}). Will check for missing weeks in Set B from {start_date_for_set_b}.")
            
            if start_date_for_set_b: # If there's a range to fill for Set B
                current_week_to_add = start_date_for_set_b
                while current_week_to_add <= max_expected_week_in_source:
                    set_b_missing_target_weeks.add(current_week_to_add)
                    current_week_to_add += timedelta(weeks=1)
        
        if set_b_missing_target_weeks:
            print(f"  Set B: Identified {len(set_b_missing_target_weeks)} weeks potentially missing from target's end: {sorted(list(set_b_missing_target_weeks))}")

        # Combine Set A and Set B
        affected_weeks_combined = set_a_affected_weeks.union(set_b_missing_target_weeks)
        affected_weeks_list = sorted(list(affected_weeks_combined))

        print(f"  Combined: Total {len(affected_weeks_list)} unique weeks to process: {affected_weeks_list}")
        
        if not affected_weeks_list:
            # This condition now means no source changes AND no target gaps to fill.
            print("  No weeks identified for processing from either source changes or target gaps. Updating watermark and exiting.")
            update_watermark_sql = """
                INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
                VALUES (%s, %s)
                ON CONFLICT (process_name) DO UPDATE SET
                    last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            # Store naive UTC
            rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update.replace(tzinfo=None)))
            rollup_conn.commit()
            print(f"  Watermark updated to {current_max_source_update.replace(tzinfo=None)} (naive UTC).")
            return

        # --- 4. Recalculate and Update Summaries for Affected Weeks ---
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
                    week_beginning, newly_rented_during_week, returned_rentals_during_week,
                    net_change_in_outstanding, outstanding_rentals_at_week_end, last_updated
                ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (week_beginning) DO UPDATE SET
                    newly_rented_during_week = EXCLUDED.newly_rented_during_week,
                    returned_rentals_during_week = EXCLUDED.returned_rentals_during_week,
                    net_change_in_outstanding = EXCLUDED.net_change_in_outstanding,
                    outstanding_rentals_at_week_end = EXCLUDED.outstanding_rentals_at_week_end,
                    last_updated = CURRENT_TIMESTAMP;
                """
                rollup_cur.execute(upsert_sql, (
                    summary_data['week_beginning'],
                    summary_data['newly_rented_during_week'],
                    summary_data['returned_rentals_during_week'],
                    net_change, # Calculated net_change
                    summary_data['outstanding_rentals_at_week_end']
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