-- models/silver/dim_calendar.sql
-- Purpose: Calendar dimension combining calendar + calendar_dates
-- Input: stg_calendar, stg_calendar_dates
-- Output: stm_silver.dim_calendar

WITH calendar AS (
    SELECT * FROM {{ ref('stg_calendar') }}
),

exceptions AS (
    SELECT * FROM {{ ref('stg_calendar_dates') }}
),

final AS (
    SELECT
        c.service_id,
        c.monday,
        c.tuesday,
        c.wednesday,
        c.thursday,
        c.friday,
        c.saturday,
        c.sunday,
        c.start_date,
        c.end_date,

        -- Count of exceptions for this service
        COALESCE(added.additions, 0) AS service_additions,
        COALESCE(removed.removals, 0) AS service_removals,

        -- Is this a weekday-only, weekend-only, or all-week service?
        CASE
            WHEN c.monday + c.tuesday + c.wednesday + c.thursday + c.friday = 5
                 AND c.saturday + c.sunday = 0
            THEN 'Weekday'
            WHEN c.monday + c.tuesday + c.wednesday + c.thursday + c.friday = 0
                 AND c.saturday + c.sunday > 0
            THEN 'Weekend'
            WHEN c.monday + c.tuesday + c.wednesday + c.thursday + c.friday + c.saturday + c.sunday = 7
            THEN 'Daily'
            ELSE 'Custom'
        END AS service_pattern

    FROM calendar c

    LEFT JOIN (
        SELECT service_id, COUNT(*) AS additions
        FROM exceptions
        WHERE exception_type = 1
        GROUP BY service_id
    ) added ON c.service_id = added.service_id

    LEFT JOIN (
        SELECT service_id, COUNT(*) AS removals
        FROM exceptions
        WHERE exception_type = 2
        GROUP BY service_id
    ) removed ON c.service_id = removed.service_id
)

SELECT * FROM final
