-- models/staging/stg_shapes.sql
-- Purpose: Cast and clean route shapes from Bronze
-- Input: stm_bronze.gtfs_shapes
-- Output: stm_silver.stg_shapes

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_shapes') }}
),

cleaned AS (
    SELECT
        -- Shape identifier
        shape_id,

        -- GPS coordinates
        CAST(shape_pt_lat AS DOUBLE) AS shape_pt_lat,
        CAST(shape_pt_lon AS DOUBLE) AS shape_pt_lon,

        -- Sequence
        CAST(shape_pt_sequence AS INT) AS shape_pt_sequence,

        -- Metadata
        _source_file,
        _ingestion_timestamp

    FROM source
    WHERE shape_id IS NOT NULL
)

SELECT * FROM cleaned