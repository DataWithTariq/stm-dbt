-- models/gold/fct_fleet_optimization.sql
-- Purpose: Fleet optimization recommendations based on occupancy, speed, weather, and coverage
-- Grain: 1 row per route
-- Business questions answered:
--   #1 Fleet Reallocation: Which routes need more/fewer buses?
--   #2 Peak Hour Overcrowding: When are routes most crowded?
--   #3 Speed Bottlenecks: Which routes are slowest?
--   #4 Weather Impact: Does weather affect crowding?
--   #5 Weekday vs Weekend: Service mismatch by day type?
--   #6 Service Coverage: Geographic spread per route
--   #7 Frequency vs Demand: High trips but empty? Low trips but packed?

{{ config(
    materialized='table',
    schema='stm_gold'
) }}

-- Step 1: Get dominant weather per day (most frequent category)
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
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY observation_date ORDER BY cnt DESC) AS rn
        FROM weather_counts
    )
    WHERE rn = 1
),

-- Step 2: Enrich each position with route, weather, day type, time period
base AS (
    SELECT
        p.route_id,
        p.bus_id,
        p.event_date,
        p.event_hour,
        p.occupancy_status,
        p.speed,
        p.latitude,
        p.longitude,
        r.route_short_name,
        r.route_long_name,
        r.route_type_desc,
        COALESCE(w.dominant_weather, 'Unknown') AS dominant_weather,
        CASE
            WHEN DAYOFWEEK(p.event_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE
            WHEN p.event_hour BETWEEN 6 AND 9 THEN 'AM_Peak'
            WHEN p.event_hour BETWEEN 15 AND 18 THEN 'PM_Peak'
            ELSE 'Off_Peak'
        END AS time_period
    FROM {{ ref('stg_vehicle_positions') }} p
    LEFT JOIN {{ ref('dim_routes') }} r ON p.route_id = r.route_id
    LEFT JOIN daily_weather w ON p.event_date = w.observation_date
    WHERE p.occupancy_status IS NOT NULL
),

-- Step 3: Aggregate everything per route
route_stats AS (
    SELECT
        route_id,
        route_short_name,
        route_long_name,
        route_type_desc,
        COUNT(*) AS total_positions,
        COUNT(DISTINCT bus_id) AS distinct_buses,
        COUNT(DISTINCT event_date) AS days_observed,

        -- ===========================================
        -- #1 FLEET REALLOCATION — Occupancy breakdown
        -- ===========================================
        ROUND(SUM(CASE WHEN occupancy_status = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_empty,
        ROUND(SUM(CASE WHEN occupancy_status = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_many_seats,
        ROUND(SUM(CASE WHEN occupancy_status = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_few_seats,
        ROUND(SUM(CASE WHEN occupancy_status = 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_standing,
        ROUND(SUM(CASE WHEN occupancy_status = 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_full,
        ROUND(SUM(CASE WHEN occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS overcrowded_pct,
        ROUND(SUM(CASE WHEN occupancy_status IN (0, 1) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS underused_pct,

        -- ===========================================
        -- #2 PEAK HOUR OVERCROWDING
        -- ===========================================
        ROUND(SUM(CASE WHEN time_period = 'AM_Peak' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN time_period = 'AM_Peak' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_am_peak,
        ROUND(SUM(CASE WHEN time_period = 'PM_Peak' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN time_period = 'PM_Peak' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_pm_peak,
        ROUND(SUM(CASE WHEN time_period = 'Off_Peak' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN time_period = 'Off_Peak' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_off_peak,

        -- ===========================================
        -- #3 SPEED BOTTLENECKS
        -- ===========================================
        ROUND(AVG(speed), 1) AS avg_speed,
        ROUND(AVG(CASE WHEN time_period = 'AM_Peak' THEN speed END), 1) AS avg_speed_am_peak,
        ROUND(AVG(CASE WHEN time_period = 'PM_Peak' THEN speed END), 1) AS avg_speed_pm_peak,
        ROUND(AVG(CASE WHEN time_period = 'Off_Peak' THEN speed END), 1) AS avg_speed_off_peak,

        -- ===========================================
        -- #4 WEATHER IMPACT ON CROWDING
        -- ===========================================
        ROUND(SUM(CASE WHEN dominant_weather = 'Snow' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN dominant_weather = 'Snow' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_snow,
        ROUND(SUM(CASE WHEN dominant_weather = 'Clear' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN dominant_weather = 'Clear' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_clear,
        ROUND(SUM(CASE WHEN dominant_weather = 'Cloudy' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN dominant_weather = 'Cloudy' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_cloudy,

        -- ===========================================
        -- #5 WEEKDAY vs WEEKEND
        -- ===========================================
        ROUND(SUM(CASE WHEN day_type = 'Weekday' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN day_type = 'Weekday' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_weekday,
        ROUND(SUM(CASE WHEN day_type = 'Weekend' AND occupancy_status IN (3, 5) THEN 1 ELSE 0 END) * 100.0 /
              NULLIF(SUM(CASE WHEN day_type = 'Weekend' THEN 1 ELSE 0 END), 0), 1) AS overcrowded_pct_weekend,
        ROUND(SUM(CASE WHEN day_type = 'Weekday' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_positions_weekday,
        ROUND(SUM(CASE WHEN day_type = 'Weekend' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_positions_weekend,

        -- ===========================================
        -- #6 SERVICE COVERAGE (bounding box)
        -- ===========================================
        MIN(latitude) AS min_latitude,
        MAX(latitude) AS max_latitude,
        MIN(longitude) AS min_longitude,
        MAX(longitude) AS max_longitude

    FROM base
    GROUP BY route_id, route_short_name, route_long_name, route_type_desc
    HAVING COUNT(*) >= 500
),

-- Step 4: Add coverage area, route analytics, and recommendations
final AS (
    SELECT
        rs.*,

        -- #6 Coverage area (bounding box approximation in km²)
        ROUND(
            (rs.max_latitude - rs.min_latitude) * 111.0 *
            (rs.max_longitude - rs.min_longitude) * 111.0 *
            COS(RADIANS((rs.min_latitude + rs.max_latitude) / 2.0)),
            2
        ) AS coverage_area_km2,

        -- #7 FREQUENCY vs DEMAND (from route analytics)
        ra.total_trips,
        ra.total_stops,
        ra.outbound_trips,
        ra.inbound_trips,
        ra.avg_stops_per_trip,

        -- ===========================================
        -- FLEET RECOMMENDATION
        -- ===========================================
        CASE
            WHEN rs.overcrowded_pct >= 30 THEN 'ADD_BUSES'
            WHEN rs.overcrowded_pct >= 20 THEN 'MONITOR'
            WHEN rs.underused_pct >= 90 THEN 'REDUCE_BUSES'
            WHEN rs.underused_pct >= 80 THEN 'REVIEW_SCHEDULE'
            ELSE 'OPTIMAL'
        END AS fleet_recommendation,

        -- Priority score (higher = needs attention first)
        CASE
            WHEN rs.overcrowded_pct >= 30 THEN rs.overcrowded_pct
            WHEN rs.underused_pct >= 90 THEN rs.underused_pct
            ELSE 0
        END AS priority_score,

        -- #7 Demand-frequency mismatch detection
        CASE
            WHEN rs.overcrowded_pct >= 25 AND ra.total_trips < 2000
                THEN 'HIGH_DEMAND_LOW_FREQUENCY'
            WHEN rs.underused_pct >= 80 AND ra.total_trips > 3000
                THEN 'LOW_DEMAND_HIGH_FREQUENCY'
            ELSE 'BALANCED'
        END AS demand_frequency_match,

        -- Weather sensitivity score (snow overcrowding minus clear overcrowding)
        ROUND(COALESCE(rs.overcrowded_pct_snow, 0) - COALESCE(rs.overcrowded_pct_clear, 0), 1)
            AS weather_sensitivity_pts

    FROM route_stats rs
    LEFT JOIN {{ ref('fct_route_analytics') }} ra ON rs.route_id = ra.route_id
)

SELECT * FROM final
ORDER BY priority_score DESC
