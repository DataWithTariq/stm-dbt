-- models/staging/stg_vehicle_positions.sql
-- Purpose: Clean vehicle positions (already parsed in Bronze)
-- Input: stm_bronze.vehicle_positions (parsed protobuf)
-- Output: stm_silver.stg_vehicle_positions (cleaned)
-- Note: Uses v2.0 column names (ingestion_file, not _source_file)

WITH source AS (
    SELECT * FROM {{ source('bronze', 'vehicle_positions') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        bus_id,
        trip_id,
        route_id,
        
        -- Trip info
        start_time,
        start_date,
        
        -- Position (already DOUBLE from Bronze)
        latitude,
        longitude,
        speed,
        bearing,
        
        -- Status codes
        current_status,
        occupancy_status,
        
        -- Timestamps (UTC to Montreal)
        CONVERT_TIMEZONE('UTC', 'America/Montreal', event_timestamp) AS event_timestamp,
        
        -- Derived fields
        CAST(CONVERT_TIMEZONE('UTC', 'America/Montreal', event_timestamp) AS DATE) AS event_date,
        HOUR(CONVERT_TIMEZONE('UTC', 'America/Montreal', event_timestamp)) AS event_hour,
        
        -- Metadata (v2.0 names)
        ingestion_file AS _source_file,
        ingestion_timestamp AS _ingestion_timestamp,
        ingestion_date AS _ingestion_date,
        batch_id AS _batch_id

    FROM source
    WHERE bus_id IS NOT NULL
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
)

SELECT * FROM cleaned