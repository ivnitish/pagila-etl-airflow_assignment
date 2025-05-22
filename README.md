# Pagila Weekly Rental Summary ETL - Incremental Approach

This document explains an ETL (Extract, Transform, Load) process designed to calculate and store weekly summaries of movie rental activity from a source database (Pagila) into a summary table in a target database (Rollup). This particular version uses an incremental approach, meaning it only processes changes since its last successful run, making it efficient.

## Key Tables and Columns

This ETL process interacts with the following key tables and columns:

**Source Database: Pagila**

*   **`rental` table:** This is the primary source of data.
    *   `rental_id`: Unique identifier for each rental.
    *   `rental_date`: Timestamp of when the rental occurred.
    *   `return_date`: Timestamp of when the item was returned (can be NULL if not yet returned).
    *   `last_update`: Timestamp indicating the last time the rental record was modified. This column is crucial for the incremental update logic.

**Destination Database: Rollup**

*   **`weekly_rental_summary` table:** Stores the aggregated weekly summaries.
    *   `week_beginning` (DATE, Primary Key): The Monday of the week being summarized.
    *   `"OutstandingRentals"` (INTEGER): Count of rentals outstanding at the end of the week.
    *   `"ReturnedRentals"` (INTEGER): Count of rentals returned during the week.
    *   `newly_rented_during_week` (INTEGER): Count of new rentals initiated during the week.
    *   `net_change_in_outstanding` (INTEGER): The net change in outstanding rentals (newly rented - returned).
    *   `last_updated` (TIMESTAMP): Timestamp of when this summary row was last calculated by the ETL.

*   **`etl_watermarks` table:** Used by the ETL to track its progress.
    *   `process_name` (VARCHAR, Primary Key): Identifier for the ETL process (e.g., "pagila_weekly_rental_summary").
    *   `last_successful_update_timestamp` (TIMESTAMP): The `last_update` value from the source `rental` table up to which data has been successfully processed and summarized.

## ETL Approach Explained

The main idea is to keep our `weekly_rental_summary` table in the Rollup database up-to-date without having to recalculate everything from scratch every time. We only focus on what's new or changed in the source `rental` table and ensure our summary isn't missing recent weeks.

**How it Works (Step-by-Step):**

1.  **Handling for a Fresh Start:**
    *   Before anything else, if the main `weekly_rental_summary` table in the Rollup database is found to be completely empty (like on a brand-new setup or after a manual reset), the script resets its "bookmark" (see next step) to the very beginning. This ensures it attempts to load all historical data from Pagila on its first proper run against an empty target.

2.  **Remembering the Last Run (The "Bookmark"):**
    *   The script first checks a  table called `etl_watermarks` in the Rollup database.
    *   This table stores a timestamp indicating the `last_update` time of the rental records that were considered during the last successful run. This is our bookmark.
    *   If no bookmark is found (and the target isn't empty as per Step 1), it usually assumes it needs to process everything from a very old default date.

3.  **Figuring Out What Needs Attention:**
    *   The script then looks for two main kinds of work:
        *   **A. Source Data Changes:** It queries the Pagila `rental` table for all records where the `last_update` timestamp is more recent than the previously stored bookmark.
        *   **B. Gaps at the End of Our Summary:** It checks if the Pagila `rental` table has activity for weeks more recent than what's currently in our `weekly_rental_summary` table. For instance, if our summary ends at "Week 10" but Pagila has rental activity in "Week 11" and "Week 12", these future weeks are noted.
    *   It also finds out the most recent `last_update` timestamp currently in the Pagila `rental` table. This will become our new bookmark if this run is successful and there were source data changes.

4.  **Deciding if There's Work To Do:**
    *   The script now looks at what it found in Step 3.
    *   If *neither* source data changes (A) nor gaps at the end of our summary (B) identify any weeks needing processing, the script simply updates the bookmark to the most recent `last_update` it found in Pagila (if applicable, or keeps it as is if source `last_update` hasn't advanced) and finishes. 

5.  **Listing All Affected Weeks (with Pandas):**
    *   If there *is* work to do (from A, B, or both), the script uses the Pandas library to make a combined, unique list of all weeks that need attention.
        *   For weeks identified by source data changes (A), it looks at the `rental_date` and `return_date` for each of these changed records. It determines all unique weeks these dates fall into.
        *   These are combined with any weeks identified as missing from the end of our summary (B).
    *   This final list contains all the specific weeks whose summaries might need to be (re)calculated.

6.  **Recalculating Summaries for Each Affected Week:**
    *   For *each week* in the "weeks to recheck" list from Step 5:
        *   The script goes back to the main Pagila `rental` table.
        *   It performs a fresh calculation for that specific week, counting:
            *   `"OutstandingRentals"`: How many movies were still out on rent at the very end of this week (this is the new name for `outstanding_rentals_at_week_end`).
            *   `"ReturnedRentals"`: How many movies were returned during this week (this is the new name for `returned_rentals_during_week`).
            *   `newly_rented_during_week`: How many new movies were rented out during this week.
            *   `net_change_in_outstanding`: The difference between newly rented and returned.
        *   This recalculation uses the current, complete data from the Pagila `rental` table for that week, regardless of why the week was flagged for processing.

7.  **Saving the Summaries:**
    *   The newly calculated summary figures for each affected week are then saved into the `weekly_rental_summary` table in the Rollup database. The columns will be in the new order: `week_beginning`, `"OutstandingRentals"`, `"ReturnedRentals"`, `newly_rented_during_week`, `net_change_in_outstanding`, `last_updated`.
    *   If a summary for that week already exists, it's updated with the new figures. If it's a new week, a new row is inserted.
    *   A `last_updated` timestamp is also recorded in the summary table for that week.

8.  **Updating the Bookmark:**
    *   Once all affected weeks are processed and summaries are saved successfully, the script updates its bookmark in the `etl_watermarks` table. The new bookmark will be the most recent `last_update` timestamp it found in Pagila earlier (from Step 3). This way, the next run knows where to start from for detecting source data changes.

**How Data Changes are Handled:**

*   **New Rental Record:**
    *   The script will pick this up via "Source Data Changes" (Step 3A) because its `last_update` timestamp will be recent.
    *   The week of its `rental_date` will be added to the "weeks to recheck."
    *   The summary for that week will be recalculated (Step 6), including this new rental in `newly_rented_during_week` and `"OutstandingRentals"`.

*   **Rental Record Updated (e.g., `return_date` is added/changed):**
    *   Picked up via "Source Data Changes" (Step 3A).
    *   The week of its `rental_date` AND the week of its `return_date` (if it has one) will be added to "weeks to recheck."
    *   Summaries for both these weeks will be recalculated:
        *   The `rental_date` week will have its `"OutstandingRentals"` count updated.
        *   The `return_date` week will have its `"ReturnedRentals"` count updated.

*   **Old Record Updated (e.g., a `return_date` from months ago is corrected):**
    *   As long as the `last_update` timestamp on that old record is changed, it's picked up by "Source Data Changes" (Step 3A).
    *   The weeks related to its `rental_date` and the (new/corrected) `return_date` will be added to "weeks to recheck."
    *   Summaries for these (potentially old) weeks will be fully recalculated from the source, ensuring historical accuracy for those specific weeks.

*   **No Source `last_update` Changes, but Target is Behind:**
    *   If the source Pagila database has rental activity (e.g., rentals from last week) that hasn't yet made it into our `weekly_rental_summary` table, "Gaps at the End of Our Summary" (Step 3B) will identify these missing weeks.
    *   These weeks will be added to "weeks to recheck" and their summaries will be calculated and added to the target table. This ensures the target catches up even if no individual records were recently modified via `last_update`.

**Important Note on `last_update`:** The "Source Data Changes" part of this process relies heavily on the `last_update` column in the source `rental` table being accurately maintained. Every time a rental record is created or any important field (like `return_date`) is changed, `last_update` must be updated to the current time.

## Key ETL Aspects Covered

*   **Incrementality:** Only processes data that has changed since the last run, based on the `last_update` timestamp and a stored watermark. This is efficient.
*   **Idempotency:** The script can be run multiple times with the same input data (or if it fails and is rerun) and will produce the same correct result in the target database without causing errors or data duplication. This is achieved by:
    *   Using `CREATE TABLE IF NOT EXISTS` for database tables.
    *   Using `INSERT ... ON CONFLICT DO UPDATE` (an "upsert") when saving summaries, so existing weekly records are updated, and new ones are inserted.
    *   Reliable watermarking ensures it picks up from where it left off.
*   **Reliability (Error Handling):**
    *   If an error occurs, the script attempts to roll back changes in the target database for the current run to avoid partial updates.
    *   The watermark is only updated after all steps complete successfully. If it fails midway, the watermark remains unchanged, and the next run will reprocess the same data window.
*   **Auditability:** The script includes print statements (which go to Airflow logs) indicating its progress. The `last_updated` column in `weekly_rental_summary` also provides an audit trail for when each week's summary was last processed.

##  Docker Setup for Development/Testing

While the script itself doesn't manage Docker, here's how you might set up the environment using Docker Compose for development or testing:

*   **`docker-compose.yml` file:** This file would define the services needed.

*   **Services:**
    1.  **Pagila Database (Source):**
        *   Use an official PostgreSQL Docker image (e.g., `postgres:latest` or a specific version).
        *   Mount a SQL initialization script (e.g., `pagila-schema.sql`, `pagila-data.sql`) to create the Pagila schema and load its data when the container starts.
        *   **Crucially, ensure the `rental` table in this setup has the `last_update` column added (e.g., `ALTER TABLE rental ADD COLUMN last_update TIMESTAMP WITHOUT TIME ZONE;`) and populated appropriately.**
        *   Define ports (e.g., map container port 5432 to a host port like `54320`).
        *   Set environment variables for `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`.

    2.  **Rollup Database (Target):**
        *   Another PostgreSQL Docker image.
        *   Define ports (e.g., map container port 5432 to a host port like `54321`).
        *   Set environment variables for its own `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`.
        *   The ETL script itself will create the `weekly_rental_summary` and `etl_watermarks` tables in this database.

    3.  **Airflow:**
        *   Use an official Apache Airflow Docker image (e.g., `apache/airflow:latest` or a specific version).
        *   Mount your `dags` folder (containing `pagila_weekly_summary_dag.py` and `etl_script_incremental_pandas.py`) into the Airflow container's DAGs directory.
        *   Mount an `airflow.cfg` file or set environment variables for Airflow configuration (like executor type, secrets backend).
        *   Define ports for the Airflow webserver (e.g., map container port 8080 to host port `8080`).
        *   Depends on the database services to ensure they start first.
        *   You'd need to set up Airflow connections (`pagila_postgres_connection`, `rollup_postgres_connection`) through the Airflow UI or environment variables, pointing to the Pagila and Rollup Docker services (e.g., hostnames would be `pagila-db` and `rollup-db` if those are the service names in `docker-compose.yml`).

*   **Running:**
    *   You would use `docker-compose up -d` to start all services.
    *   Access Airflow UI via `http://localhost:8080`.
    *   Access databases via their mapped host ports using a SQL client if needed for inspection.

This Docker setup provides an isolated and reproducible environment for running the ETL pipeline, with separate containers for each component. 