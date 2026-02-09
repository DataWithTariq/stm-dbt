-- models/staging/stg_trips.sql
-- Purpose: Cast and clean trip definitions from Bronze
-- Input: stm_bronze.gtfs_trips
-- Output: stm_silver.stg_trips

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_trips') }}
),

cleaned AS (
    SELECT
        -- Primary key
        trip_id,

        -- Foreign keys
        route_id,
        service_id,
        shape_id,

        -- Trip attributes
        trip_headsign,
        CAST(direction_id AS INT) AS direction_id,
        note_en,
        note_fr,

        -- Metadata
        _source_file,
        _ingestion_timestamp

    FROM source
    WHERE trip_id IS NOT NULL
)

SELECT * FROM cleaned