-- models/gold/fct_route_analytics.sql
-- Purpose: Route-level analytics from GTFS static schedules
-- Grain: 1 row per route_id (static — no date dimension)
-- Rule: Descriptive columns (route_short_name etc) live in dim_routes only.
-- FK: route_id → dim_routes

{{ config(
    materialized='table',
    schema='stm_gold'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

stop_times AS (
    SELECT * FROM {{ ref('fact_stop_times') }}
),

-- Trips per route and direction
trip_counts AS (
    SELECT
        route_id,
        direction_label,
        COUNT(DISTINCT trip_id) AS total_trips
    FROM trips
    GROUP BY route_id, direction_label
),

-- Distinct stops per route
route_stops AS (
    SELECT
        route_id,
        COUNT(DISTINCT stop_id) AS total_stops
    FROM stop_times
    GROUP BY route_id
),

-- Average stops per trip
avg_stops AS (
    SELECT
        route_id,
        ROUND(AVG(max_seq), 1) AS avg_stops_per_trip
    FROM (
        SELECT route_id, trip_id, MAX(stop_sequence) AS max_seq
        FROM stop_times
        GROUP BY route_id, trip_id
    )
    GROUP BY route_id
),

final AS (
    SELECT
        -- FK only
        out.route_id,

        -- Trip stats (additive)
        COALESCE(out.total_trips, 0) AS outbound_trips,
        COALESCE(inb.total_trips, 0) AS inbound_trips,
        COALESCE(out.total_trips, 0) + COALESCE(inb.total_trips, 0) AS total_trips,

        -- Stop coverage
        rs.total_stops,
        avs.avg_stops_per_trip,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM trip_counts out
    LEFT JOIN trip_counts inb
        ON out.route_id = inb.route_id AND inb.direction_label = 'Inbound'
    LEFT JOIN route_stops rs
        ON out.route_id = rs.route_id
    LEFT JOIN avg_stops avs
        ON out.route_id = avs.route_id
    WHERE out.direction_label = 'Outbound'
)

SELECT * FROM final
