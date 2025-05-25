import psycopg2
import psycopg2.extras # For DictCursor
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
PAGILA_CONN_ID = "pagila_postgres_connection"
ROLLUP_CONN_ID = "rollup_postgres_connection"
ETL_PROCESS_NAME = "pagila_weekly_rental_summary"
DEFAULT_WATERMARK_START_DATE = datetime(1900, 1, 1)

# --- Helper to get Airflow connection details (if running in Airflow) ---
def get_db_connection_details(conn_id):
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
    pagila_conn = None
    rollup_conn = None

    try:
        print("Starting incremental ETL for Pagila weekly summary (Simplified Timezone Handling)...")

        # --- Database Connections ---
        print(f"Connecting to Pagila DB using: {pagila_conn_params.get('host')}")
        pagila_conn = psycopg2.connect(**pagila_conn_params)
        pagila_cur = pagila_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("Connected to Pagila DB.")

        print(f"Connecting to Rollup DB using: {rollup_conn_params.get('host')}")
        rollup_conn = psycopg2.connect(**rollup_conn_params)
        rollup_cur = rollup_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("Connected to Rollup DB.")

        # Ensure weekly_rental_summary table exists
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

        # Ensure etl_watermarks table exists
        create_watermarks_table_sql = """
        CREATE TABLE IF NOT EXISTS etl_watermarks (
            process_name VARCHAR(255) PRIMARY KEY,
            last_successful_update_timestamp TIMESTAMP WITHOUT TIME ZONE
        );
        """
        rollup_cur.execute(create_watermarks_table_sql)
        rollup_conn.commit()
        print("Ensured 'etl_watermarks' table exists in Rollup DB.")

        # --- Step 0: Proactive Watermark Reset if Target Table is Empty ---
        print("Step 0: Checking if target table 'weekly_rental_summary' is empty to potentially reset watermark...")
        rollup_cur.execute("SELECT 1 FROM weekly_rental_summary LIMIT 1;")
        target_table_has_any_data = rollup_cur.fetchone()

        if not target_table_has_any_data:
            print("  Target table 'weekly_rental_summary' is empty. Resetting watermark to default start date.")
            reset_watermark_sql = """
            INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (process_name) DO UPDATE SET
                last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            rollup_cur.execute(reset_watermark_sql, (ETL_PROCESS_NAME, DEFAULT_WATERMARK_START_DATE))
            rollup_conn.commit()
            print(f"  Watermark for '{ETL_PROCESS_NAME}' reset to: {DEFAULT_WATERMARK_START_DATE}")
        else:
            print("  Target table 'weekly_rental_summary' is not empty. Proceeding with existing watermark.")

        # --- 1. Determine Time Window for Changes ---
        print("Step 1: Determining time window for changes...")
        rollup_cur.execute(
            "SELECT last_successful_update_timestamp FROM etl_watermarks WHERE process_name = %s",
            (ETL_PROCESS_NAME,)
        )
        watermark_result = rollup_cur.fetchone()

        previous_watermark = watermark_result['last_successful_update_timestamp'] if watermark_result and watermark_result['last_successful_update_timestamp'] else DEFAULT_WATERMARK_START_DATE
        print(f"  Previous watermark: {previous_watermark}")

        pagila_cur.execute("SELECT MAX(last_update) FROM rental;")
        current_max_source_update_result = pagila_cur.fetchone()
        
        max_source_val = current_max_source_update_result[0] if current_max_source_update_result and current_max_source_update_result[0] else None

        if max_source_val:
            current_max_source_update = max_source_val
            # If current_max_source_update is timezone-aware, convert to naive (assuming UTC)
            if hasattr(current_max_source_update, 'tzinfo') and current_max_source_update.tzinfo is not None and current_max_source_update.tzinfo.utcoffset(current_max_source_update) is not None:
                print(f"  Converting timezone-aware max_source_update ({current_max_source_update}) to naive.")
                current_max_source_update = current_max_source_update.replace(tzinfo=None)
        else:
            # If no max_last_update from source (e.g., rental table empty), use previous_watermark
            current_max_source_update = previous_watermark
        
        print(f"  Current max source update from source (processed as naive): {current_max_source_update}")

        # --- 2. Extract Changed Data (Delta Extraction) ---
        print(f"Step 2: Extracting changed data from source (if last_update > {previous_watermark})...")
        changed_rentals_df = pd.DataFrame()

        if current_max_source_update > previous_watermark:
            delta_sql = """
                SELECT rental_id, rental_date, return_date, last_update
                FROM rental
                WHERE last_update > %s AND last_update <= %s;
            """
            changed_rentals_df = pd.read_sql_query(delta_sql, pagila_conn, params=(previous_watermark, current_max_source_update))
            print(f"  Found {len(changed_rentals_df)} changed/new rental records based on source last_update.")
        else:
            print(f"  No new rental records based on source last_update (current_max_source_update: {current_max_source_update} <= previous_watermark: {previous_watermark}).")

        # --- 3. Identify Affected Weeks ---
        # Step 3a: Identify weeks_with_data_changes (from source 'last_update' changes)
        print("Step 3a: Identifying 'weeks_with_data_changes' (from source 'last_update' modifications)...")
        if not changed_rentals_df.empty:
            changed_rentals_df['rental_date'] = pd.to_datetime(changed_rentals_df['rental_date'])
            changed_rentals_df['return_date'] = pd.to_datetime(changed_rentals_df['return_date'], errors='coerce')
        else:
            print("  No data in changed_rentals_df for identifying 'weeks_with_data_changes'.")

        weeks_with_data_changes = set() # Renamed from set_a_affected_weeks
        if not changed_rentals_df.empty:
            for _, row in changed_rentals_df.iterrows():
                if pd.notna(row['rental_date']):
                    weeks_with_data_changes.add((row['rental_date'] - timedelta(days=row['rental_date'].weekday())).date())
                if pd.notna(row['return_date']):
                    weeks_with_data_changes.add((row['return_date'] - timedelta(days=row['return_date'].weekday())).date())
        print(f"  'weeks_with_data_changes': Identified {len(weeks_with_data_changes)} unique weeks: {sorted(list(weeks_with_data_changes))}")

        # Step 3b: Identify weeks_to_backfill (weeks potentially missing from the end of the target summary table)
        print("Step 3b: Identifying 'weeks_to_backfill' (weeks potentially missing from the end of the target summary table)...")
        weeks_to_backfill = set() # Renamed from set_b_missing_target_weeks
        pagila_cur.execute("SELECT MAX(GREATEST(rental_date, COALESCE(return_date, rental_date))) AS max_activity_date FROM rental;")
        max_source_activity_date_result = pagila_cur.fetchone()
        max_expected_week_in_source = None

        if max_source_activity_date_result and max_source_activity_date_result['max_activity_date']:
            max_activity_dt_obj = pd.to_datetime(max_source_activity_date_result['max_activity_date'])
            max_expected_week_in_source = (max_activity_dt_obj - timedelta(days=max_activity_dt_obj.weekday())).date()
            print(f"  Max expected week based on all source activity: {max_expected_week_in_source}")
        else:
            print("  Could not determine max expected week from source.")

        rollup_cur.execute("SELECT MAX(week_beginning) AS max_target_week FROM weekly_rental_summary;")
        max_target_week_result = rollup_cur.fetchone()
        actual_max_week_in_target = None

        if max_target_week_result and max_target_week_result['max_target_week']:
            actual_max_week_in_target = max_target_week_result['max_target_week']
            print(f"  Max week actually present in target summary table: {actual_max_week_in_target}")
        else:
            print("  Target summary table is empty or contains no week_beginning dates.")

        if max_expected_week_in_source:
            start_date_for_backfill = None
            if actual_max_week_in_target is None:
                pagila_cur.execute("SELECT MIN(GREATEST(rental_date, COALESCE(return_date, rental_date))) AS min_activity_date FROM rental WHERE rental_date IS NOT NULL;")
                min_source_activity_date_result = pagila_cur.fetchone()
                if min_source_activity_date_result and min_source_activity_date_result['min_activity_date']:
                    min_activity_dt_obj = pd.to_datetime(min_source_activity_date_result['min_activity_date'])
                    start_date_for_backfill = (min_activity_dt_obj - timedelta(days=min_activity_dt_obj.weekday())).date()
                    print(f"  Target is empty. 'weeks_to_backfill' will aim to fill from earliest source week: {start_date_for_backfill} up to {max_expected_week_in_source}.")
                else:
                     print("  Target is empty, but could not determine earliest source week for 'weeks_to_backfill' population.")
            elif actual_max_week_in_target < max_expected_week_in_source:
                start_date_for_backfill = actual_max_week_in_target + timedelta(weeks=1)
                print(f"  Target max week ({actual_max_week_in_target}) is before source max expected week ({max_expected_week_in_source}). 'weeks_to_backfill' will check from {start_date_for_backfill}.")
            
            if start_date_for_backfill:
                current_week_to_add = start_date_for_backfill
                while current_week_to_add <= max_expected_week_in_source:
                    weeks_to_backfill.add(current_week_to_add)
                    current_week_to_add += timedelta(weeks=1)
        
        if weeks_to_backfill:
            print(f"  'weeks_to_backfill': Identified {len(weeks_to_backfill)} weeks: {sorted(list(weeks_to_backfill))}")

        # Step 3c: Combine identified weeks and determine if processing is needed
        print("Step 3c: Combining identified weeks and determining if processing is needed...")
        affected_weeks_combined = weeks_with_data_changes.union(weeks_to_backfill)
        affected_weeks_list = sorted(list(affected_weeks_combined))
        print(f"  Combined: Total {len(affected_weeks_list)} unique weeks to process: {affected_weeks_list}")
        
        if not affected_weeks_list:
            print("  No weeks identified for processing. Updating watermark and exiting.")
            update_watermark_sql = """
                INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
                VALUES (%s, %s)
                ON CONFLICT (process_name) DO UPDATE SET
                    last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
            """
            rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update))
            rollup_conn.commit()
            print(f"  Watermark updated to {current_max_source_update} as no processing was needed.")
            return

        # --- 4. Recalculate and Update Summaries for Affected Weeks ---
        print(f"Step 4: Recalculating and updating summaries for {len(affected_weeks_list)} affected week(s)...")
        
        for target_week_start_dt in affected_weeks_list:
            print(f"  Processing week starting: {target_week_start_dt}")

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
            pagila_cur.execute(recalculate_sql_direct, (
                target_week_start_dt,
                target_week_start_dt, target_week_start_dt,
                target_week_start_dt, target_week_start_dt,
                target_week_start_dt, target_week_start_dt
            ))
            summary_data = pagila_cur.fetchone()

            if summary_data:
                net_change = summary_data['newly_rented_during_week'] - summary_data['returned_rentals_during_week']
                
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
                    summary_data['outstanding_rentals_at_week_end'],
                    summary_data['returned_rentals_during_week'],
                    summary_data['newly_rented_during_week'],
                    net_change,
                ))
                print(f"    Upserted summary for week {summary_data['week_beginning']}.")
            else:
                print(f"    No summary data could be calculated for week {target_week_start_dt}.")
        
        rollup_conn.commit()

        # --- 5. Update Watermark ---
        print("Step 5: Updating watermark...")
        update_watermark_sql = """
            INSERT INTO etl_watermarks (process_name, last_successful_update_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (process_name) DO UPDATE SET
                last_successful_update_timestamp = EXCLUDED.last_successful_update_timestamp;
        """
        rollup_cur.execute(update_watermark_sql, (ETL_PROCESS_NAME, current_max_source_update))
        rollup_conn.commit()
        print(f"  Successfully updated watermark for '{ETL_PROCESS_NAME}' to {current_max_source_update}.")

        print("Incremental ETL for Pagila weekly summary finished successfully.")

    except Exception as e:
        print(f"Error during incremental ETL: {e}")
        if rollup_conn:
            rollup_conn.rollback()
        raise
    finally:
        if pagila_cur: pagila_cur.close()
        if pagila_conn: pagila_conn.close()
        if rollup_cur: rollup_cur.close()
        if rollup_conn: rollup_conn.close()
        print("Database connections closed.")

if __name__ == "__main__":
    print("Attempting to run ETL script directly...")
    try:
        # For local testing without full Airflow setup, define params manually:
        local_pagila_params = {
            "host": "localhost", # Changed from host.docker.internal
            "port": "5432",      # Assuming Pagila is on default port
            "dbname": "pagila",
            "user": "postgres",
            "password": "postgres_pass" 
        }
        local_rollup_params = {
            "host": "localhost", 
            "port": "5434",      # Assuming output/rollup DB is on this port, as per last attempt
            "dbname": "rollup_db", # << CORRECTED DATABASE NAME
            "user": "rollup_user", # << CORRECTED USER
            "password": "rollup_pass"  # << CORRECTED PASSWORD
        }
        
        print(f"Using Pagila connection: host={local_pagila_params['host']}, port={local_pagila_params['port']}, dbname={local_pagila_params['dbname']}")
        print(f"Using Rollup connection: host={local_rollup_params['host']}, port={local_rollup_params['port']}, dbname={local_rollup_params['dbname']}")

        # Before running locally:
        # 1. Ensure PostgreSQL is running.
        # 2. Ensure 'pagila' and 'rollup' databases exist with the correct user/password.
        # 3. In 'pagila', ensure the 'rental' table has a 'last_update' column (TIMESTAMP WITHOUT TIME ZONE).
        #    Example to add and populate 'last_update' if it doesn't exist or is NULL:
        #    ALTER TABLE rental ADD COLUMN IF NOT EXISTS last_update TIMESTAMP WITHOUT TIME ZONE;
        #    -- Simple initial population (run once or if many last_update are NULL):
        #    UPDATE rental SET last_update = GREATEST(rental_date, COALESCE(return_date, rental_date, '1900-01-01'::timestamp)) WHERE last_update IS NULL;
        #    -- For ongoing updates, your application/DB triggers should update 'last_update' on INSERT/UPDATE to rental.
        # 4. The script will create 'etl_watermarks' and 'weekly_rental_summary' in the 'rollup' database if they don't exist.

        run_incremental_etl(local_pagila_params, local_rollup_params)

    except ImportError:
        # This path is less likely now that get_db_connection_details is not directly called in __main__ unless uncommented.
        print("Airflow hooks not available. Ensure psycopg2 and pandas are installed.")
    except psycopg2.Error as db_err:
        print(f"Database connection error: {db_err}")
        print("Please check your database connection parameters, ensure PostgreSQL is running, and databases/user exist.")
    except Exception as e:
        print(f"An unexpected error occurred during local direct run: {e}") 