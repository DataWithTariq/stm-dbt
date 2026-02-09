-- models/gold/fct_daily_performance.sql
-- Purpose: Daily route performance metrics WITH weather context
-- Input: fact_vehicle_positions, dim_routes, stg_weather
-- Output: stm_gold.fct_daily_performance

WITH positions AS (
    SELECT * FROM {{ ref('fact_vehicle_positions') }}
),

routes AS (
    SELECT * FROM {{ ref('dim_routes') }}
),

-- Aggregate weather to daily level
daily_weather AS (
    SELECT
        observation_date,
        ROUND(AVG(temperature_c), 1) AS avg_temperature_c,
        ROUND(MIN(temperature_c), 1) AS min_temperature_c,
        ROUND(MAX(temperature_c), 1) AS max_temperature_c,
        ROUND(AVG(feels_like_c), 1) AS avg_feels_like_c,
        ROUND(AVG(humidity_pct), 0) AS avg_humidity_pct,
        ROUND(AVG(wind_speed_kmh), 1) AS avg_wind_speed_kmh,
        ROUND(SUM(precipitation_mm), 1) AS total_precipitation_mm
    FROM {{ ref('stg_weather') }}
    GROUP BY observation_date
),

-- Most common weather for each day
dominant AS (
    SELECT DISTINCT
        observation_date,
        FIRST_VALUE(weather_category) OVER (
            PARTITION BY observation_date
            ORDER BY precipitation_mm DESC
        ) AS dominant_weather
    FROM {{ ref('stg_weather') }}
),

-- Aggregate positions by route and day
daily_positions AS (
    SELECT
        event_date,
        route_id,
        COUNT(*) AS total_positions,
        COUNT(DISTINCT bus_id) AS unique_vehicles,
        COUNT(DISTINCT trip_id) AS unique_trips,
        ROUND(AVG(speed), 1) AS avg_speed,
        ROUND(MAX(speed), 1) AS max_speed,
        MIN(event_timestamp) AS first_position_at,
        MAX(event_timestamp) AS last_position_at
    FROM positions
    GROUP BY event_date, route_id
),

final AS (
    SELECT
        -- Dimensions
        dp.event_date,
        dp.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type_desc,

        -- Position metrics
        dp.total_positions,
        dp.unique_vehicles,
        dp.unique_trips,
        dp.avg_speed,
        dp.max_speed,
        dp.first_position_at,
        dp.last_position_at,

        -- Service hours (approx)
        ROUND(
            CAST(UNIX_TIMESTAMP(dp.last_position_at) - UNIX_TIMESTAMP(dp.first_position_at) AS DOUBLE) / 3600,
            1
        ) AS service_hours,

        -- Weather context
        w.avg_temperature_c,
        w.min_temperature_c,
        w.max_temperature_c,
        w.avg_feels_like_c,
        w.avg_humidity_pct,
        w.avg_wind_speed_kmh,
        w.total_precipitation_mm,
        d.dominant_weather,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM daily_positions dp
    LEFT JOIN routes r ON dp.route_id = r.route_id
    LEFT JOIN daily_weather w ON dp.event_date = w.observation_date
    LEFT JOIN dominant d ON dp.event_date = d.observation_date
)

SELECT * FROM final