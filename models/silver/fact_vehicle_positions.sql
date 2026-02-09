-- models/silver/fact_vehicle_positions.sql
-- Purpose: Vehicle positions fact table with dimension keys
-- Input: stg_vehicle_positions, dim_routes, dim_stops
-- Output: stm_silver.fact_vehicle_positions

WITH positions AS (
    SELECT * FROM {{ ref('stg_vehicle_positions') }}
),

routes AS (
    SELECT route_key, route_id 
    FROM {{ ref('dim_routes') }}
),

-- Deduplicate: same bus, same timestamp = same record
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY bus_id, event_timestamp 
            ORDER BY _ingestion_timestamp DESC
        ) AS row_num
    FROM positions
),

with_keys AS (
    SELECT
        -- Surrogate key for this fact
        {{ dbt_utils.generate_surrogate_key(['p.bus_id', 'p.event_timestamp']) }} AS position_key,
        
        -- Natural keys
        p.bus_id,
        p.trip_id,
        p.route_id,
        
        -- Dimension keys
        r.route_key,
        
        -- Trip attributes
        p.start_time,
        p.start_date,
        
        -- Measures
        p.latitude,
        p.longitude,
        p.speed,
        p.bearing,
        
        -- Status
        p.current_status,
        CASE p.current_status
            WHEN 0 THEN 'INCOMING_AT'
            WHEN 1 THEN 'STOPPED_AT'
            WHEN 2 THEN 'IN_TRANSIT_TO'
            ELSE 'UNKNOWN'
        END AS current_status_desc,
        
        p.occupancy_status,
        CASE p.occupancy_status
            WHEN 0 THEN 'EMPTY'
            WHEN 1 THEN 'MANY_SEATS'
            WHEN 2 THEN 'FEW_SEATS'
            WHEN 3 THEN 'STANDING_ONLY'
            WHEN 4 THEN 'CRUSHED'
            WHEN 5 THEN 'FULL'
            WHEN 6 THEN 'NOT_ACCEPTING'
            ELSE 'UNKNOWN'
        END AS occupancy_status_desc,
        
        -- Timestamps
        p.event_timestamp,
        DATE(p.event_timestamp) AS event_date,
        HOUR(p.event_timestamp) AS event_hour,
        
        -- Metadata
        p._source_file,
        p._ingestion_timestamp,
        p._batch_id,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
        
    FROM deduplicated p
    LEFT JOIN routes r ON p.route_id = r.route_id
    WHERE p.row_num = 1
)

SELECT * FROM with_keys
