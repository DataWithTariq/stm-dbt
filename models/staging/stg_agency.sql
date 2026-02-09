-- models/staging/stg_agency.sql
-- Purpose: Clean agency data from Bronze
-- Input: stm_bronze.gtfs_agency
-- Output: stm_silver.stg_agency

WITH source AS (
    SELECT * FROM {{ source('bronze', 'gtfs_agency') }}
),

cleaned AS (
    SELECT
        agency_id,
        agency_name,
        agency_url,
        agency_timezone,
        agency_lang,
        agency_phone,

        -- Metadata
        _source_file,
        _ingestion_timestamp

    FROM source
    WHERE agency_id IS NOT NULL
)

SELECT * FROM cleaned
