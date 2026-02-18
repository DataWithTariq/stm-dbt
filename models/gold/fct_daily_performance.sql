-- models/gold/fct_daily_performance.sql
-- Purpose: Daily route performance metrics with weather context
-- Grain: 1 row per route_id × event_date
-- Rule: Only additive measures (counts, sums). Ratios computed in Power BI DAX.
-- FKs: route_id → dim_routes, event_date → dim_date

{{ config(
    materialized='table',
    schema='stm_gold'
) }}

WITH positions AS (
    SELECT * FROM {{ ref('fact_vehicle_positions') }}
),

-- Dominant weather per day (most frequent category)
weather_counts AS (
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

-- Daily weather averages
weather_agg AS (
    SELECT
        observation_date,
        ROUND(AVG(temperature_c), 1) AS avg_temperature_c,
        ROUND(AVG(humidity_pct), 0) AS avg_humidity_pct,
        ROUND(AVG(wind_speed_kmh), 1) AS avg_wind_speed_kmh,
        ROUND(SUM(precipitation_mm), 1) AS total_precipitation_mm
    FROM {{ ref('stg_weather') }}
    GROUP BY observation_date
),

snow_check AS (
    SELECT
        observation_date,
        SUM(CASE WHEN weather_category = 'Snow' THEN 1 ELSE 0 END) AS snow_hours
    FROM {{ ref('stg_weather') }}
    GROUP BY observation_date
),

-- Aggregate positions by route × day
daily_positions AS (
    SELECT
        event_date,
        route_id,

        -- Volume counts (additive)
        COUNT(*) AS total_positions,
        COUNT(DISTINCT bus_id) AS unique_vehicles,
        COUNT(DISTINCT trip_id) AS unique_trips,

        -- Speed (additive: sum + count → DAX does DIVIDE)
        SUM(speed) AS sum_speed,
        SUM(CASE WHEN speed IS NOT NULL THEN 1 ELSE 0 END) AS count_speed_readings,
        ROUND(MAX(speed), 1) AS max_speed,

        -- Occupancy counts (additive)
        SUM(CASE WHEN occupancy_status = 0 THEN 1 ELSE 0 END) AS empty_count,
        SUM(CASE WHEN occupancy_status = 1 THEN 1 ELSE 0 END) AS many_seats_count,
        SUM(CASE WHEN occupancy_status = 2 THEN 1 ELSE 0 END) AS few_seats_count,
        SUM(CASE WHEN occupancy_status = 3 THEN 1 ELSE 0 END) AS standing_count,
        SUM(CASE WHEN occupancy_status = 5 THEN 1 ELSE 0 END) AS full_count,
        SUM(CASE WHEN occupancy_status IN (3, 5) THEN 1 ELSE 0 END) AS overcrowded_count,
        SUM(CASE WHEN occupancy_status IN (0, 1) THEN 1 ELSE 0 END) AS underused_count,

        -- Time window
        MIN(event_timestamp) AS first_position_at,
        MAX(event_timestamp) AS last_position_at

    FROM positions
    GROUP BY event_date, route_id
),

final AS (
    SELECT
        -- FKs only (no descriptive columns)
        dp.event_date,
        dp.route_id,

        -- Volume (additive)
        dp.total_positions,
        dp.unique_vehicles,
        dp.unique_trips,

        -- Speed (additive — DAX computes avg)
        dp.sum_speed,
        dp.count_speed_readings,
        dp.max_speed,

        -- Occupancy (additive — DAX computes %)
        dp.empty_count,
        dp.many_seats_count,
        dp.few_seats_count,
        dp.standing_count,
        dp.full_count,
        dp.overcrowded_count,
        dp.underused_count,

        -- Service window
        dp.first_position_at,
        dp.last_position_at,
        ROUND(
            CAST(UNIX_TIMESTAMP(dp.last_position_at) - UNIX_TIMESTAMP(dp.first_position_at) AS DOUBLE) / 3600,
            1
        ) AS service_hours,

        -- Weather context (degenerate dimension — low cardinality)
        COALESCE(dw.dominant_weather, 'Unknown') AS dominant_weather,
        wa.avg_temperature_c,
        wa.avg_humidity_pct,
        wa.avg_wind_speed_kmh,
        wa.total_precipitation_mm,
        COALESCE(sc.snow_hours, 0) AS snow_hours,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM daily_positions dp
    LEFT JOIN daily_weather dw ON dp.event_date = dw.observation_date
    LEFT JOIN weather_agg wa ON dp.event_date = wa.observation_date
    LEFT JOIN snow_check sc ON dp.event_date = sc.observation_date
)

SELECT * FROM final
