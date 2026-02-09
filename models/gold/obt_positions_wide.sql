-- models/gold/obt_positions_wide.sql
-- Purpose: One Big Table - fully denormalized for BI dashboards
-- Input: fact_vehicle_positions, dim_routes, stg_weather
-- Output: stm_gold.obt_positions_wide
--
-- This is the "easy button" table for Power BI / analysts.
-- Every position record with route info and weather joined in.

WITH positions AS (
    SELECT * FROM {{ ref('fact_vehicle_positions') }}
),

routes AS (
    SELECT * FROM {{ ref('dim_routes') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

final AS (
    SELECT
        -- Position data
        p.bus_id,
        p.trip_id,
        p.route_id,
        p.latitude,
        p.longitude,
        p.speed,
        p.bearing,
        p.current_status,
        p.occupancy_status,
        p.event_timestamp,
        p.event_date,
        p.event_hour,

        -- Route context
        r.route_short_name,
        r.route_long_name,
        r.route_type_desc,
        r.route_color_hex,

        -- Weather context (matched by date + hour)
        w.temperature_c,
        w.feels_like_c,
        w.humidity_pct,
        w.wind_speed_kmh,
        w.precipitation_mm,
        w.weather_description,
        w.weather_category

    FROM positions p
    LEFT JOIN routes r ON p.route_id = r.route_id
    LEFT JOIN weather w
        ON p.event_date = w.observation_date
        AND p.event_hour = w.observation_hour
)

SELECT * FROM final
