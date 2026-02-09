-- models/staging/stg_weather.sql
-- Purpose: Cast STRING columns to proper types from Bronze
-- Input: stm_bronze.weather (ALL STRING)
-- Output: stm_silver.stg_weather (typed)

WITH source AS (
    SELECT * FROM {{ source('bronze', 'weather') }}
),

casted AS (
    SELECT
        -- Timestamp: "2026-01-15T08:00" → TIMESTAMP
        CAST(observation_time AS TIMESTAMP) AS observation_time,

        -- Derived date/hour for easy joins with vehicle_positions
        CAST(observation_time AS DATE) AS observation_date,
        HOUR(CAST(observation_time AS TIMESTAMP)) AS observation_hour,

        -- Temperature (Celsius)
        CAST(temperature AS DOUBLE) AS temperature_c,
        CAST(feels_like AS DOUBLE) AS feels_like_c,

        -- Humidity (%)
        CAST(humidity AS INT) AS humidity_pct,

        -- Wind
        CAST(wind_speed AS DOUBLE) AS wind_speed_kmh,
        CAST(wind_direction AS INT) AS wind_direction_deg,

        -- Precipitation (mm)
        CAST(precipitation AS DOUBLE) AS precipitation_mm,

        -- Weather condition
        CAST(weather_code AS INT) AS weather_code,
        weather_description,

        -- Simplified weather category for easy grouping in Gold
        CASE
            WHEN CAST(weather_code AS INT) IN (0, 1)          THEN 'Clear'
            WHEN CAST(weather_code AS INT) IN (2, 3)          THEN 'Cloudy'
            WHEN CAST(weather_code AS INT) IN (45, 48)        THEN 'Fog'
            WHEN CAST(weather_code AS INT) BETWEEN 51 AND 57  THEN 'Drizzle'
            WHEN CAST(weather_code AS INT) BETWEEN 61 AND 67  THEN 'Rain'
            WHEN CAST(weather_code AS INT) BETWEEN 71 AND 77  THEN 'Snow'
            WHEN CAST(weather_code AS INT) BETWEEN 80 AND 82  THEN 'Rain Showers'
            WHEN CAST(weather_code AS INT) BETWEEN 85 AND 86  THEN 'Snow Showers'
            WHEN CAST(weather_code AS INT) >= 95              THEN 'Thunderstorm'
            ELSE 'Unknown'
        END AS weather_category,

        -- Metadata passthrough
        _source_file,
        _ingestion_timestamp,
        _ingestion_date

    FROM source
    WHERE observation_time IS NOT NULL
)

SELECT * FROM casted