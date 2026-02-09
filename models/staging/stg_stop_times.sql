-- models/staging/stg_stop_times.sql
-- Purpose: Cast and clean stop times from Bronze
-- Input: stm_bronze.gtfs_stop_times
-- Output: stm_silver.stg_stop_times

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_stop_times') }}
),

cleaned AS (
    SELECT
        -- Foreign keys
        trip_id,
        stop_id,

        -- Times (keep as STRING - can be > 24:00:00 for overnight trips)
        arrival_time,
        departure_time,

        -- Sequence
        CAST(stop_sequence AS INT) AS stop_sequence,

        -- Pickup/dropoff behavior
        CAST(pickup_type AS INT) AS pickup_type,
        CAST(drop_off_type AS INT) AS drop_off_type,

        -- Metadata
        _source_file,
        _ingestion_timestamp

    FROM source
    WHERE trip_id IS NOT NULL
      AND stop_id IS NOT NULL
)

SELECT * FROM cleaned
