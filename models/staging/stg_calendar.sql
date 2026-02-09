-- models/staging/stg_calendar.sql
-- Purpose: Cast and clean service calendar from Bronze
-- Input: stm_bronze.gtfs_calendar
-- Output: stm_silver.stg_calendar

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_calendar') }}
),

cleaned AS (
    SELECT
        -- Primary key
        service_id,

        -- Day of week flags (1 = service runs, 0 = no service)
        CAST(monday AS INT) AS monday,
        CAST(tuesday AS INT) AS tuesday,
        CAST(wednesday AS INT) AS wednesday,
        CAST(thursday AS INT) AS thursday,
        CAST(friday AS INT) AS friday,
        CAST(saturday AS INT) AS saturday,
        CAST(sunday AS INT) AS sunday,

        -- Date range (format: 20251027 → 2025-10-27)
        TO_DATE(start_date, 'yyyyMMdd') AS start_date,
        TO_DATE(end_date, 'yyyyMMdd') AS end_date,

        -- Metadata
        _source_file,
        _ingestion_timestamp

    FROM source
    WHERE service_id IS NOT NULL
)

SELECT * FROM cleaned