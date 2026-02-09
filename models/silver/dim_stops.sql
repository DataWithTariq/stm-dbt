-- models/silver/dim_stops.sql
-- Purpose: Stop dimension with clean attributes
-- Input: stg_stops
-- Output: stm_silver.dim_stops

WITH stops AS (
    SELECT * FROM {{ ref('stg_stops') }}
),

final AS (
    SELECT
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        stop_code,
        wheelchair_boarding,

        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['stop_id']) }} AS stop_sk

    FROM stops
)

SELECT * FROM final
