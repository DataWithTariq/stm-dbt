-- models/silver/fact_trips.sql
-- Purpose: Trip facts enriched with route and calendar info
-- Input: stg_trips, dim_routes
-- Output: stm_silver.fact_trips

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
),

routes AS (
    SELECT * FROM {{ ref('dim_routes') }}
),

final AS (
    SELECT
        -- Primary key
        t.trip_id,

        -- Foreign keys
        t.route_id,
        t.service_id,
        t.shape_id,

        -- Trip attributes
        t.trip_headsign,
        t.direction_id,
        t.note_en,
        t.note_fr,

        -- Direction label
        CASE t.direction_id
            WHEN 0 THEN 'Outbound'
            WHEN 1 THEN 'Inbound'
            ELSE 'Unknown'
        END AS direction_label,

        -- Route info (denormalized for easy querying)
        r.route_short_name,
        r.route_long_name,
        r.route_type_desc

    FROM trips t
    LEFT JOIN routes r ON t.route_id = r.route_id
)

SELECT * FROM final