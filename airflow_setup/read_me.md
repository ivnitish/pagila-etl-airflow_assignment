# Pagila Rental Summary ETL

This project demonstrates an ETL (Extract, Transform, Load) process to extract rental data 
from the Pagila sample database, transform it into a weekly summary, and load it into a 
dedicated rollup database. The ETL pipeline is orchestrated using Apache Airflow.

## Project Goals
- Set up the Pagila source database using Docker.
- Set up a new PostgreSQL database (Rollup DB) for aggregated data using Docker.
- Set up Apache Airflow using Docker to orchestrate the ETL pipeline.
- Implement an ETL script in Python (`etl_script.py`) that:
    - Incrementally replicates changes from the Pagila `rental` table to a `rental_replica` table in the Rollup DB using a watermark based on a `last_update` timestamp.
    - Calculates weekly rental summaries (outstanding and returned rentals) based on the `rental_replica` table.
    - Upserts these summaries into a `weekly_rental_summary` table in the Rollup DB.
- Ensure the ETL process is robust, idempotent, and can handle historical data changes (updates) efficiently.

## Architecture Overview
The system consists of three main components, all containerized using Docker:
1.  **Pagila Source Database:** A PostgreSQL database containing the raw rental data.
2.  **Rollup Target Database:** A separate PostgreSQL database to store the replicated data and the final weekly summaries.
3.  **Apache Airflow:** Used to schedule, orchestrate, and monitor the ETL Python script.

## Setup

### Prerequisites
- Docker and Docker Compose (Docker Compose is recommended for managing multiple services like Airflow, Pagila, and the Rollup DB).
- Python 3.7+ (for local script development if needed, though the ETL runs within Airflow's Docker environment).
- `psycopg2-binary` and `apache-airflow-providers-postgres` (these are managed within the Airflow environment).

### 1. Run Pagila Source Database
We use the `dvdrental/pagila` Docker image.
```bash
docker run --name pagila-db \
  -e POSTGRES_USER=pagila \
  -e POSTGRES_PASSWORD=pagilapass \
  -e POSTGRES_DB=pagila \
  -p 5433:5432 \
  -d dvdrental/pagila
```
**Note:** Ensure the `rental` table in this database has a `last_update` column (e.g., `TIMESTAMP WITH TIME ZONE`) that is reliably updated on every insert or update to a rental record. This is crucial for the incremental ETL logic. If it doesn't exist, it needs to be added and backfilled. For example:
```sql
-- Add the column if it doesn't exist
-- ALTER TABLE rental ADD COLUMN last_update TIMESTAMP WITH TIME ZONE DEFAULT NOW();

-- Create a trigger to automatically update it (example for PostgreSQL)
-- CREATE OR REPLACE FUNCTION update_last_update_column()
-- RETURNS TRIGGER AS $$
-- BEGIN
--    NEW.last_update = NOW();
--    RETURN NEW;
-- END;
-- $$ language 'plpgsql';

-- CREATE TRIGGER rental_last_update_trigger
-- BEFORE INSERT OR UPDATE ON rental
-- FOR EACH ROW EXECUTE PROCEDURE update_last_update_column();

-- Backfill existing rows (run once)
-- UPDATE rental SET last_update = COALESCE(return_date, rental_date, NOW()); 
-- (Adjust backfill logic as appropriate for historical accuracy if possible)
```

### 2. Run Rollup Target Database
This is a standard PostgreSQL database.
```bash
docker run --name rollup-db \
  -e POSTGRES_USER=rollup \
  -e POSTGRES_PASSWORD=rolluppass \
  -e POSTGRES_DB=rollup \
  -p 5434:5432 \
  -d postgres:13 
```
(Adjust image tag and ports as needed.)

### 3. Setup and Run Apache Airflow
It's highly recommended to use the official Docker Compose setup for Airflow:
[https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

Follow the instructions to download the `docker-compose.yaml`, initialize the environment, and start Airflow.
- Place the `etl_script.py` and `pagila_weekly_summary_dag.py` files into the `dags` folder specified in your Airflow setup (e.g., `./dags`).
- **Airflow Connections:**
    - Create a Postgres connection in the Airflow UI (Admin -> Connections) named `pagila_postgres_connection` pointing to the Pagila DB (e.g., host `host.docker.internal` or your machine's IP if Airflow is in Docker, port `5433`, user `pagila`, password `pagilapass`, schema `pagila`).
    - Create another Postgres connection named `rollup_postgres_connection` pointing to the Rollup DB (e.g., host `host.docker.internal` or IP, port `5434`, user `rollup`, password `rolluppass`, schema `rollup`).

## ETL Design and Evolution

The ETL script (`etl_script.py`) is responsible for populating the `weekly_rental_summary` table. The design evolved to enhance scalability, robustness, and the ability to handle historical data changes.

### Initial Considerations (Conceptual)
Early versions might have involved simpler scripts directly querying the source and calculating summaries in Python or with less sophisticated SQL. However, to meet scalability and data consistency goals, a more robust approach was adopted.

### Two-Stage Staging Approach (Intermediate Step - Conceptual)
A previous iteration adopted a two-stage process:
1.  **Stage 1 (Extract & Stage):** Extract a broad window of raw rental data from Pagila and load it into a temporary staging table in the Rollup DB.
2.  **Stage 2 (Transform & Load):** Use pure SQL within the Rollup DB to read from this staging table, calculate weekly summaries, and upsert them into the final summary table.
This improved upon direct calculation by leveraging database power for transformations but still had limitations in handling very granular historical updates efficiently.

### Current Approach: Replica and Watermark
The current, refined ETL process utilizes a replica of the source data and watermarking for efficient incremental updates:

1.  **Database Objects in Rollup DB:**
    *   `rental_replica`: A table storing a persistent, incrementally updated copy of relevant fields from Pagila's `rental` table (`rental_id`, `rental_date`, `return_date`, `last_update_source`). `rental_id` is the primary key.
    *   `etl_watermarks`: A table storing the `last_processed_watermark_ts` (the latest `last_update` timestamp from the source `rental` table that was successfully processed). This is key for delta processing.

2.  **Stage 1: Replicate Delta to `rental_replica`**
    *   The ETL script first queries `etl_watermarks` to get the `previous_watermark`.
    *   It then extracts records from the Pagila `rental` table where `last_update` (from Pagila) is greater than `previous_watermark`.
    *   These new or updated records are `UPSERT`-ed into the `rental_replica` table in the Rollup DB. This ensures `rental_replica` stays synchronized with the source for changes.
    *   The list of `rental_id`s affected by this delta and the maximum `last_update` timestamp from this batch are recorded.

3.  **Stage 2: Recalculate Summaries from `rental_replica`**
    *   The script determines which specific weeks need their summaries recalculated. This includes:
        *   The most recent week that was previously processed (to catch any late-arriving updates to its constituent rentals).
        *   Any week impacted by the `rental_id`s that were updated in Stage 1 (determined by checking their `rental_date` and `return_date` in `rental_replica`).
        *   On the very first run (if `weekly_rental_summary` is empty), all historical weeks based on the data in `rental_replica` are processed.
    *   A SQL query (with CTEs for clarity) is then run against the `rental_replica` table and a temporary table of weeks-to-process. This query calculates the `outstanding_rentals` and `returned_rentals` for each relevant week.
    *   The calculated summaries are `UPSERT`-ed into the `weekly_rental_summary` table.

4.  **Watermark Update:**
    *   Only after both stages complete successfully is the `etl_watermarks` table updated with the new maximum `last_update` timestamp processed in Stage 1. This ensures that if the ETL fails mid-process, the next run will reprocess data from the last successful watermark.

**Benefits of this Approach:**
-   **Efficient Incremental Loads:** Only new or changed data from the source is processed in Stage 1.
-   **Handles Historical Updates:** If a `last_update` timestamp for an old rental record is modified in the source, the change is picked up and replicated, and the relevant week's summary is recalculated.
-   **Robustness:** The watermark ensures that data is not missed if a run fails.
-   **SQL-Centric Transformation:** Heavy lifting for summary calculation is done in SQL within the target database, leveraging its optimization capabilities.
-   **Clear Data Lineage for Summaries:** Stage 2 calculations are based entirely on the `rental_replica` table within the Rollup DB.

**Important Note on Hard Deletes:** This ETL relies on the `last_update` timestamp in the source `rental` table to detect changes. If records are hard-deleted from the source *without* their `last_update` timestamp being modified (or without a corresponding "deleted" record being processed), these deletions will not be automatically propagated to `rental_replica`. In such scenarios, `rental_replica` might retain records that no longer exist in the source. Strategies to handle this, if required, include periodic full reconciliation or implementing soft deletes in the source system.
