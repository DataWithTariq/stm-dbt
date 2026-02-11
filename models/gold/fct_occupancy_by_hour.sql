-- models/gold/fct_occupancy_by_hour.sql
-- Purpose: Hourly occupancy breakdown per route for detailed analysis
-- Grain: 1 row per route × hour (max ~211 routes × 24 hours = ~5K rows)
-- Use case: Power BI heatmap showing which routes are crowded at which hours
-- Enables drill-down from fct_fleet_optimization summary

{{ config(
    materialized='table',
    schema='stm_gold'
) }}

SELECT
    p.route_id,
    r.route_short_name,
    r.route_long_name,
    p.event_hour,

    -- Volume
    COUNT(*) AS total_positions,
    COUNT(DISTINCT p.bus_id) AS active_buses,
    COUNT(DISTINCT p.event_date) AS days_observed,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT p.event_date), 0) AS avg_positions_per_day,

    -- Speed
    ROUND(AVG(p.speed), 1) AS avg_speed,

    -- Occupancy distribution
    ROUND(SUM(CASE WHEN p.occupancy_status = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_empty,
    ROUND(SUM(CASE WHEN p.occupancy_status = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_many_seats,
    ROUND(SUM(CASE WHEN p.occupancy_status = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_few_seats,
    ROUND(SUM(CASE WHEN p.occupancy_status = 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_standing,
    ROUND(SUM(CASE WHEN p.occupancy_status = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_full,

    -- Key metrics
    ROUND(SUM(CASE WHEN p.occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS overcrowded_pct,
    ROUND(SUM(CASE WHEN p.occupancy_status IN (0, 1) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS underused_pct,

    -- Time period label
    CASE
        WHEN p.event_hour BETWEEN 6 AND 9 THEN 'AM_Peak'
        WHEN p.event_hour BETWEEN 15 AND 18 THEN 'PM_Peak'
        ELSE 'Off_Peak'
    END AS time_period

FROM {{ ref('stg_vehicle_positions') }} p
LEFT JOIN {{ ref('dim_routes') }} r ON p.route_id = r.route_id
WHERE p.occupancy_status IS NOT NULL
GROUP BY p.route_id, r.route_short_name, r.route_long_name, p.event_hour
HAVING COUNT(*) >= 100
