-- models/gold/fct_fleet_optimization.sql
-- Purpose: Daily fleet optimization metrics per route
-- Grain: 1 row per route_id × event_date
-- Rule: Only additive measures (counts, sums). All ratios and recommendations in DAX.
-- FKs: route_id → dim_routes, event_date → dim_date

{{ config(
    materialized='table',
    schema='stm_gold'
) }}

-- Dominant weather per day
WITH weather_counts AS (
    SELECT
        observation_date,
        weather_category,
        COUNT(*) AS cnt
    FROM {{ ref('stg_weather') }}
    GROUP BY observation_date, weather_category
),

daily_weather AS (
    SELECT
        observation_date,
        weather_category AS dominant_weather
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY observation_date ORDER BY cnt DESC) AS rn
        FROM weather_counts
    )
    WHERE rn = 1
),

snow_check AS (
    SELECT
        observation_date,
        SUM(CASE WHEN weather_category = 'Snow' THEN 1 ELSE 0 END) AS snow_hours
    FROM {{ ref('stg_weather') }}
    GROUP BY observation_date
),

-- Enrich each position with time period, day type
enriched AS (
    SELECT
        p.route_id,
        p.bus_id,
        p.event_date,
        p.event_hour,
        p.occupancy_status,
        p.speed,
        p.latitude,
        p.longitude,
        COALESCE(w.dominant_weather, 'Unknown') AS dominant_weather,
        COALESCE(sc.snow_hours, 0) AS snow_hours,
        CASE
            WHEN DAYOFWEEK(p.event_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE
            WHEN p.event_hour BETWEEN 6 AND 9 THEN 'AM Peak'
            WHEN p.event_hour BETWEEN 15 AND 18 THEN 'PM Peak'
            ELSE 'Off Peak'
        END AS time_period
    FROM {{ ref('stg_vehicle_positions') }} p
    LEFT JOIN daily_weather w ON p.event_date = w.observation_date
    LEFT JOIN snow_check sc ON p.event_date = sc.observation_date
    WHERE p.occupancy_status IS NOT NULL
),

-- Aggregate per route × day
final AS (
    SELECT
        -- FKs only
        route_id,
        event_date,

        -- Weather (degenerate dimension)
        dominant_weather,
        snow_hours,

        -- Day type
        day_type,

        -- ===========================================
        -- VOLUME COUNTS (all additive)
        -- ===========================================
        COUNT(*) AS total_positions,
        COUNT(DISTINCT bus_id) AS distinct_buses,

        -- ===========================================
        -- OCCUPANCY COUNTS (additive — DAX computes %)
        -- ===========================================
        SUM(CASE WHEN occupancy_status = 0 THEN 1 ELSE 0 END) AS empty_count,
        SUM(CASE WHEN occupancy_status = 1 THEN 1 ELSE 0 END) AS many_seats_count,
        SUM(CASE WHEN occupancy_status = 2 THEN 1 ELSE 0 END) AS few_seats_count,
        SUM(CASE WHEN occupancy_status = 3 THEN 1 ELSE 0 END) AS standing_count,
        SUM(CASE WHEN occupancy_status = 5 THEN 1 ELSE 0 END) AS full_count,
        SUM(CASE WHEN occupancy_status IN (3, 5) THEN 1 ELSE 0 END) AS overcrowded_count,
        SUM(CASE WHEN occupancy_status IN (0, 1) THEN 1 ELSE 0 END) AS underused_count,

        -- ===========================================
        -- PEAK HOUR COUNTS (additive — DAX computes %)
        -- ===========================================
        SUM(CASE WHEN time_period = 'AM Peak' THEN 1 ELSE 0 END) AS am_peak_count,
        SUM(CASE WHEN time_period = 'AM Peak' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) AS am_peak_overcrowded_count,
        SUM(CASE WHEN time_period = 'PM Peak' THEN 1 ELSE 0 END) AS pm_peak_count,
        SUM(CASE WHEN time_period = 'PM Peak' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) AS pm_peak_overcrowded_count,
        SUM(CASE WHEN time_period = 'Off Peak' THEN 1 ELSE 0 END) AS off_peak_count,
        SUM(CASE WHEN time_period = 'Off Peak' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) AS off_peak_overcrowded_count,

        -- ===========================================
        -- SPEED (additive — DAX computes avg = sum/count)
        -- ===========================================
        SUM(speed) AS sum_speed,
        SUM(CASE WHEN speed IS NOT NULL THEN 1 ELSE 0 END) AS count_speed_readings,
        SUM(CASE WHEN time_period = 'AM Peak' THEN speed ELSE 0 END) AS sum_speed_am_peak,
        SUM(CASE WHEN time_period = 'AM Peak' AND speed IS NOT NULL THEN 1 ELSE 0 END) AS count_speed_am_peak,
        SUM(CASE WHEN time_period = 'PM Peak' THEN speed ELSE 0 END) AS sum_speed_pm_peak,
        SUM(CASE WHEN time_period = 'PM Peak' AND speed IS NOT NULL THEN 1 ELSE 0 END) AS count_speed_pm_peak,

        -- ===========================================
        -- GEOGRAPHIC COVERAGE (for bounding box in DAX)
        -- ===========================================
        MIN(latitude) AS min_latitude,
        MAX(latitude) AS max_latitude,
        MIN(longitude) AS min_longitude,
        MAX(longitude) AS max_longitude

    FROM enriched
    GROUP BY route_id, event_date, dominant_weather, snow_hours, day_type
    HAVING COUNT(*) >= 10
)

SELECT * FROM final
