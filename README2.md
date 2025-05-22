# Pagila Weekly Rental Summary ETL - Incremental Approach

This document explains an ETL (Extract, Transform, Load) process designed to calculate and store weekly summaries of movie rental activity from a source database (Pagila) into a summary table in a target database (Rollup). This particular version uses an incremental approach, meaning it only processes changes since its last successful run, making it efficient.

## ETL Approach Explained

The main idea is to keep our `weekly_rental_summary` table in the Rollup database up-to-date without having to recalculate everything from scratch every time. We only focus on what's new or changed in the source `rental` table.

**How it Works (Step-by-Step):**

1.  **Remembering the Last Run:**
    *   The script first checks a special table called `etl_watermarks` in the Rollup database.
    *   This table stores a timestamp indicating the `last_update` time of the rental records that were processed during the last successful run. Think of this as a bookmark.
    *   If it's the very first time the script runs, it assumes it needs to process everything from the beginning of time (a very old default date like 1900-01-01).

2.  **Finding New or Changed Rentals:**
    *   The script then asks the source Pagila `rental` table: "Show me all rental records that have been updated or created since my last bookmark (`last_update` > previous bookmark)."
    *   It also finds out the most recent `last_update` timestamp currently in the Pagila `rental` table. This will become our new bookmark if this run is successful.

3.  **Handling No Changes:**
    *   If no rental records have changed since the last bookmark, the script simply updates the bookmark to the current time (the most recent `last_update` it found) and finishes. Quick and easy!

4.  **Listing Affected Weeks (with Pandas):**
    *   If there *are* new or changed rental records, the script uses the Pandas library to help with a simple task:
        *   It looks at the `rental_date` and `return_date` for each of these changed records.
        *   It makes a list of all the unique weeks that these dates fall into. For example, if a movie was rented on Monday, Jan 2nd, and returned on Friday, Jan 6th, both belonging to the same week, that week (e.g., starting Monday, Jan 2nd) is added to our list of "weeks to recheck."
    *   This list now contains all the specific weeks whose summaries might need to be updated.

5.  **Recalculating Summaries for Affected Weeks:**
    *   For *each week* in the "weeks to recheck" list:
        *   The script goes back to the main Pagila `rental` table.
        *   It performs a fresh calculation for that specific week, counting:
            *   `newly_rented_during_week`: How many new movies were rented out during this week.
            *   `returned_rentals_during_week`: How many movies were returned during this week.
            *   `outstanding_rentals_at_week_end`: How many movies were still out on rent at the very end of this week.
            *   `net_change_in_outstanding`: The difference between newly rented and returned.
        *   This recalculation uses the current, complete data from the Pagila `rental` table for that week.

6.  **Saving the Summaries:**
    *   The newly calculated summary figures for each affected week are then saved into the `weekly_rental_summary` table in the Rollup database.
    *   If a summary for that week already exists, it's updated with the new figures. If it's a new week, a new row is inserted.
    *   A `last_updated` timestamp is also recorded in the summary table for that week.

7.  **Updating the Bookmark:**
    *   Once all affected weeks are processed and summaries are saved successfully, the script updates its bookmark in the `etl_watermarks` table to the most recent `last_update` timestamp it found in step 2. This way, the next run knows where to start from.

**How Data Changes are Handled:**

*   **New Rental Record:**
    *   The script will pick up this new record because its `last_update` timestamp will be recent.
    *   The week of its `rental_date` will be added to the "weeks to recheck."
    *   The summary for that week will be recalculated, including this new rental in `newly_rented_during_week` and `outstanding_rentals_at_week_end`.

*   **Rental Record Updated (e.g., `return_date` is added/changed):**
    *   The script will pick up this updated record because its `last_update` timestamp will be recent.
    *   The week of its `rental_date` AND the week of its `return_date` (if it has one) will be added to the "weeks to recheck."
    *   Summaries for both these weeks will be recalculated:
        *   The `rental_date` week will have its `outstanding_rentals_at_week_end` count updated.
        *   The `return_date` week will have its `returned_rentals_during_week` count updated.
        *   `newly_rented_during_week` for the rental week usually won't change unless the rental record itself was just created.

*   **Old Record Updated (e.g., a `return_date` from months ago is corrected):**
    *   As long as the `last_update` timestamp on that old record is changed to reflect this correction, the script will pick it up.
    *   The weeks related to its `rental_date` and the (new/corrected) `return_date` will be added to the "weeks to recheck."
    *   Summaries for these (potentially old) weeks will be fully recalculated from the source, ensuring historical accuracy for those specific weeks.

**Important Note on `last_update`:** This whole process relies heavily on the `last_update` column in the source `rental` table being accurately maintained. Every time a rental record is created or any important field (like `return_date`) is changed, `last_update` must be updated to the current time.

## Key ETL Aspects Covered

*   **Incrementality:** Only processes data that has changed since the last run, based on the `last_update` timestamp and a stored watermark. This is efficient.
*   **Idempotency:** The script can be run multiple times with the same input data (or if it fails and is rerun) and will produce the same correct result in the target database without causing errors or data duplication. This is achieved by:
    *   Using `CREATE TABLE IF NOT EXISTS` for database tables.
    *   Using `INSERT ... ON CONFLICT DO UPDATE` (an "upsert") when saving summaries, so existing weekly records are updated, and new ones are inserted.
    *   Reliable watermarking ensures it picks up from where it left off.
*   **Modularity:** The Python script is organized into logical steps.
*   **Reliability (Error Handling):**
    *   If an error occurs, the script attempts to roll back changes in the target database for the current run to avoid partial updates.
    *   The watermark is only updated after all steps complete successfully. If it fails midway, the watermark remains unchanged, and the next run will reprocess the same data window.
*   **Auditability:** The script includes print statements (which go to Airflow logs) indicating its progress. The `last_updated` column in `weekly_rental_summary` also provides an audit trail for when each week's summary was last processed.

## Conceptual Docker Setup for Development/Testing

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