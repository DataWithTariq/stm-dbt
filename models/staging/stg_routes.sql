-- models/staging/stg_routes.sql
-- Purpose: Cast STRING columns to proper types from Bronze
-- Input: stm_bronze.gtfs_routes (all STRING)
-- Output: stm_silver.stg_routes (typed)

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_routes') }}
),

casted AS (
    SELECT
        -- Primary key
        route_id,
        
        -- Foreign key
        agency_id,
        
        -- Route attributes
        route_short_name,
        route_long_name,
        
        -- Cast STRING to INT
        CAST(route_type AS INT) AS route_type,
        
        -- URL (keep as string)
        route_url,
        
        -- Colors - add # prefix if not null
        CASE 
            WHEN route_color IS NOT NULL AND route_color != '' 
            THEN CONCAT('#', UPPER(route_color))
            ELSE NULL 
        END AS route_color_hex,
        
        CASE 
            WHEN route_text_color IS NOT NULL AND route_text_color != '' 
            THEN CONCAT('#', UPPER(route_text_color))
            ELSE NULL 
        END AS route_text_color_hex,
        
        -- Metadata passthrough
        _source_file,
        _ingestion_timestamp
        
    FROM source
    WHERE route_id IS NOT NULL
)

SELECT * FROM casted
