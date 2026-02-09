-- models/gold/fct_route_analytics.sql
-- Purpose: Route-level analytics with trip schedules and coverage
-- Input: fact_trips, fact_stop_times, dim_routes
-- Output: stm_gold.fct_route_analytics

WITH trips AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

stop_times AS (
    SELECT * FROM {{ ref('fact_stop_times') }}
),

routes AS (
    SELECT * FROM {{ ref('dim_routes') }}
),

-- Trips per route and direction
trip_counts AS (
    SELECT
        route_id,
        route_short_name,
        direction_label,
        COUNT(DISTINCT trip_id) AS total_trips
    FROM trips
    GROUP BY route_id, route_short_name, direction_label
),

-- Stops per route
route_stops AS (
    SELECT
        route_id,
        COUNT(DISTINCT stop_id) AS total_stops,
        ROUND(AVG(max_seq), 1) AS avg_stops_per_trip
    FROM (
        SELECT route_id, trip_id, stop_id, MAX(stop_sequence) AS max_seq
        FROM stop_times
        GROUP BY route_id, trip_id, stop_id
    )
    GROUP BY route_id
),

final AS (
    SELECT
        -- Route info
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type_desc,
        r.route_color_hex,

        -- Trip stats
        COALESCE(outb.total_trips, 0) AS outbound_trips,
        COALESCE(inb.total_trips, 0) AS inbound_trips,
        COALESCE(outb.total_trips, 0) + COALESCE(inb.total_trips, 0) AS total_trips,

        -- Stop coverage
        rs.total_stops,
        rs.avg_stops_per_trip,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM routes r
    LEFT JOIN trip_counts outb
        ON r.route_id = outb.route_id AND outb.direction_label = 'Outbound'
    LEFT JOIN trip_counts inb
        ON r.route_id = inb.route_id AND inb.direction_label = 'Inbound'
    LEFT JOIN route_stops rs
        ON r.route_id = rs.route_id
)

SELECT * FROM final