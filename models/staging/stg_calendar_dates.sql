-- models/staging/stg_calendar_dates.sql
-- Purpose: Cast and clean calendar exceptions from Bronze
-- Input: stm_bronze.gtfs_calendar_dates
-- Output: stm_silver.stg_calendar_dates

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_calendar_dates') }}
),

cleaned AS (
    SELECT
        -- Foreign key
        service_id,

        -- Exception date (format: 20251027 → 2025-10-27)
        TO_DATE(date, 'yyyyMMdd') AS exception_date,

        -- Exception type (1 = service added, 2 = service removed)
        CAST(exception_type AS INT) AS exception_type,

        -- Metadata
        _source_file,
        _ingestion_timestamp

    FROM source
    WHERE service_id IS NOT NULL
)

SELECT * FROM cleaned