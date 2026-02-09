-- models/staging/stg_stops.sql
-- Purpose: Cast STRING columns to proper types from Bronze
-- Input: stm_bronze.gtfs_stops (all STRING)
-- Output: stm_silver.stg_stops (typed)

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_stops') }}
),

casted AS (
    SELECT
        -- Primary key
        stop_id,
        
        -- Stop attributes
        stop_code,
        stop_name,
        
        -- Cast STRING to DOUBLE for coordinates
        CAST(stop_lat AS DOUBLE) AS stop_lat,
        CAST(stop_lon AS DOUBLE) AS stop_lon,
        
        -- URL
        stop_url,
        
        -- Cast STRING to INT
        CAST(location_type AS INT) AS location_type,
        
        -- Foreign key (nullable)
        NULLIF(parent_station, '') AS parent_station,
        
        -- Cast STRING to INT
        CAST(wheelchair_boarding AS INT) AS wheelchair_boarding,
        
        -- Metadata
        _source_file,
        _ingestion_timestamp
        
    FROM source
    WHERE stop_id IS NOT NULL
)

SELECT * FROM casted
