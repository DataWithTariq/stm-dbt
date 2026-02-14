-- models/gold/fct_occupancy_by_hour.sql
-- Purpose: Hourly occupancy breakdown per route per day
-- Grain: 1 row per route_id × event_date × event_hour
-- Rule: Only additive measures (counts, sums). Ratios computed in DAX.
-- FKs: route_id → dim_routes, event_date → dim_date
-- Use case: Power BI heatmap (route × hour), hourly drill-down

{{ config(
    materialized='table',
    schema='stm_gold'
) }}

SELECT
    -- FKs only
    p.route_id,
    p.event_date,
    p.event_hour,

    -- Volume (additive)
    COUNT(*) AS total_positions,
    COUNT(DISTINCT p.bus_id) AS active_buses,

    -- Speed (additive — DAX computes avg = sum/count)
    SUM(p.speed) AS sum_speed,
    SUM(CASE WHEN p.speed IS NOT NULL THEN 1 ELSE 0 END) AS count_speed_readings,

    -- Occupancy counts (additive — DAX computes %)
    SUM(CASE WHEN p.occupancy_status = 0 THEN 1 ELSE 0 END) AS empty_count,
    SUM(CASE WHEN p.occupancy_status = 1 THEN 1 ELSE 0 END) AS many_seats_count,
    SUM(CASE WHEN p.occupancy_status = 2 THEN 1 ELSE 0 END) AS few_seats_count,
    SUM(CASE WHEN p.occupancy_status = 3 THEN 1 ELSE 0 END) AS standing_count,
    SUM(CASE WHEN p.occupancy_status = 5 THEN 1 ELSE 0 END) AS full_count,
    SUM(CASE WHEN p.occupancy_status IN (3, 5) THEN 1 ELSE 0 END) AS overcrowded_count,
    SUM(CASE WHEN p.occupancy_status IN (0, 1) THEN 1 ELSE 0 END) AS underused_count,

    -- Time period label (degenerate dimension)
    CASE
        WHEN p.event_hour BETWEEN 6 AND 9 THEN 'AM Peak'
        WHEN p.event_hour BETWEEN 15 AND 18 THEN 'PM Peak'
        ELSE 'Off Peak'
    END AS time_period

FROM {{ ref('stg_vehicle_positions') }} p
WHERE p.occupancy_status IS NOT NULL
GROUP BY p.route_id, p.event_date, p.event_hour
HAVING COUNT(*) >= 5
