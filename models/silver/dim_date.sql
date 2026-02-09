-- models/silver/dim_date.sql
-- Equivalent of "Calendarfornia Dreamin" Power Query calendar in dbt/Databricks SQL

{{ config(materialized='table') }}

{#- ========== SETUP ========== -#}
{%- set start_date = "'2025-12-01'" -%}
{%- set end_date = "'2027-12-31'" -%}
{%- set fiscal_start_month = 7 -%}
{#- First day of week: 1=Sunday, 2=Monday (Databricks DAYOFWEEK convention) -#}
{%- set first_day_of_week = 1 -%}

WITH date_spine AS (

    SELECT EXPLODE(SEQUENCE(
        DATE {{ start_date }},
        DATE {{ end_date }},
        INTERVAL 1 DAY
    )) AS date_key

),

today AS (

    SELECT CURRENT_DATE() AS sys_date

),

base AS (

    SELECT
        d.date_key,
        t.sys_date,

        -- ========== DATE ==========
        DATEDIFF(d.date_key, t.sys_date)                       AS date_relative_num,

        -- ========== YEAR ==========
        YEAR(d.date_key)                                        AS year,
        DATE_TRUNC('YEAR', d.date_key)                          AS year_start_date,
        YEAR(d.date_key) - YEAR(t.sys_date)                     AS year_relative_num,

        -- ========== FISCAL YEAR ==========
        -- Fiscal ref date: shift by (12 - fiscal_end_month) months
        ADD_MONTHS(d.date_key, 12 - {{ (fiscal_start_month + 10) % 12 + 1 }})  AS fis_ref_date,

        -- ========== QUARTER ==========
        QUARTER(d.date_key)                                     AS qtr_num,
        CONCAT('Q', QUARTER(d.date_key))                        AS qtr,
        CONCAT('Q', QUARTER(d.date_key), '-', DATE_FORMAT(d.date_key, 'yy'))  AS qtr_label,
        DATE_TRUNC('QUARTER', d.date_key)                       AS qtr_start_date,
        (YEAR(d.date_key) * 4 + QUARTER(d.date_key))
          - (YEAR(t.sys_date) * 4 + QUARTER(t.sys_date))        AS qtr_relative_num,

        -- ========== MONTH ==========
        MONTH(d.date_key)                                       AS month_num,
        DATE_FORMAT(d.date_key, 'MMM')                          AS month_short,
        DATE_FORMAT(d.date_key, 'MMMM')                         AS month_name,
        DATE_FORMAT(d.date_key, 'MMM-yyyy')                     AS month_label,
        DATE_TRUNC('MONTH', d.date_key)                         AS month_start_date,
        (YEAR(d.date_key) * 12 + MONTH(d.date_key))
          - (YEAR(t.sys_date) * 12 + MONTH(t.sys_date))         AS month_relative_num,

        -- ========== WEEK ==========
        WEEKOFYEAR(d.date_key)                                  AS week_of_year,
        DATE_TRUNC('WEEK', d.date_key)                          AS week_start_date,
        FLOOR(DATEDIFF(
            DATE_TRUNC('WEEK', d.date_key),
            DATE_TRUNC('WEEK', t.sys_date)
        ) / 7)                                                  AS week_relative_num,

        -- ========== DAY ==========
        DAY(d.date_key)                                         AS day_of_month,
        DAYOFWEEK(d.date_key)                                   AS day_of_week_num,
        DATE_FORMAT(d.date_key, 'EEEE')                         AS day_name,
        DATE_FORMAT(d.date_key, 'EEE')                          AS day_short,
        DAYOFYEAR(d.date_key)                                   AS day_of_year,

        -- ========== FLAGS ==========
        CASE
            WHEN DAYOFWEEK(d.date_key) IN (1, 7) THEN TRUE
            ELSE FALSE
        END                                                     AS is_weekend,
        CASE
            WHEN DAYOFWEEK(d.date_key) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END                                                     AS weekday_label

    FROM date_spine d
    CROSS JOIN today t

)

SELECT
    -- ========== DATE ==========
    date_key,
    date_relative_num,

    -- ========== YEAR ==========
    year,
    year_start_date,
    year_relative_num,

    -- ========== FISCAL YEAR ==========
    YEAR(fis_ref_date)                                          AS fis_year,
    CONCAT('FY', DATE_FORMAT(fis_ref_date, 'yy'))               AS fis_year_label,
    ADD_MONTHS(
        DATE_TRUNC('YEAR', fis_ref_date),
        -1 * (12 - {{ (fiscal_start_month + 10) % 12 + 1 }})
    )                                                           AS fis_year_start_date,

    -- ========== FISCAL QUARTER ==========
    QUARTER(fis_ref_date)                                       AS fis_qtr_num,
    CONCAT('Q', QUARTER(fis_ref_date))                          AS fis_qtr,
    CONCAT('Q', QUARTER(fis_ref_date), '-', DATE_FORMAT(fis_ref_date, 'yy'))  AS fis_qtr_label,

    -- ========== FISCAL MONTH ==========
    MONTH(fis_ref_date)                                         AS fis_month_num,
    DATE_FORMAT(date_key, 'MMM')                                AS fis_month,
    CONCAT(DATE_FORMAT(date_key, 'MMM'), '-', DATE_FORMAT(fis_ref_date, 'yyyy'))  AS fis_month_label,
    DATE_TRUNC('MONTH', date_key)                               AS fis_month_start_date,

    -- ========== QUARTER ==========
    qtr_num,
    qtr,
    qtr_label,
    qtr_start_date,
    qtr_relative_num,

    -- ========== MONTH ==========
    month_num,
    month_short,
    month_name,
    month_label,
    month_start_date,
    month_relative_num,

    -- ========== WEEK ==========
    week_of_year,
    week_start_date,
    week_relative_num,

    -- ========== DAY ==========
    day_of_month,
    day_of_week_num,
    day_name,
    day_short,
    day_of_year,

    -- ========== FLAGS ==========
    is_weekend,
    weekday_label

FROM base
