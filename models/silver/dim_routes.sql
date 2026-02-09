-- models/silver/dim_routes.sql
-- Purpose: Route dimension with business descriptions
-- Input: stg_routes
-- Output: stm_silver.dim_routes

WITH staged AS (
    SELECT * FROM {{ ref('stg_routes') }}
),

with_descriptions AS (
    SELECT
        -- Surrogate key (optional - natural key works for GTFS)
        {{ dbt_utils.generate_surrogate_key(['route_id']) }} AS route_key,
        
        -- Natural key
        route_id,
        
        -- Foreign key
        agency_id,
        
        -- Attributes
        route_short_name,
        route_long_name,
        route_type,
        
        -- Business description for route_type
        CASE route_type
            WHEN 0 THEN 'Tram/Streetcar'
            WHEN 1 THEN 'Subway/Metro'
            WHEN 2 THEN 'Rail'
            WHEN 3 THEN 'Bus'
            WHEN 4 THEN 'Ferry'
            WHEN 5 THEN 'Cable Tram'
            WHEN 6 THEN 'Aerial Lift'
            WHEN 7 THEN 'Funicular'
            WHEN 11 THEN 'Trolleybus'
            WHEN 12 THEN 'Monorail'
            ELSE 'Other'
        END AS route_type_desc,
        
        -- Colors
        route_color_hex,
        route_text_color_hex,
        route_url,
        
        -- Metadata
        _source_file,
        _ingestion_timestamp,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
        
    FROM staged
)

SELECT * FROM with_descriptions
