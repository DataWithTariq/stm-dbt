-- models/silver/fact_stop_times.sql
-- Purpose: Stop times enriched with stop and trip context
-- Input: stg_stop_times, dim_stops, fact_trips
-- Output: stm_silver.fact_stop_times

WITH stop_times AS (
    SELECT * FROM {{ ref('stg_stop_times') }}
),

stops AS (
    SELECT * FROM {{ ref('dim_stops') }}
),

trips AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

final AS (
    SELECT
        st.trip_id,
        st.stop_id,
        st.arrival_time,
        st.departure_time,
        st.stop_sequence,
        st.pickup_type,
        st.drop_off_type,

        -- Stop info (denormalized)
        s.stop_name,
        s.stop_lat,
        s.stop_lon,

        -- Trip/route info (denormalized)
        t.route_id,
        t.route_short_name,
        t.direction_label,
        t.trip_headsign

    FROM stop_times st
    LEFT JOIN stops s ON st.stop_id = s.stop_id
    LEFT JOIN trips t ON st.trip_id = t.trip_id
)

SELECT * FROM final
