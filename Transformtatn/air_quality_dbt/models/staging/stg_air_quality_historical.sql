{{ config(materialized='view') }}

-- Staging model for historical air quality data
-- Combines all country-specific external tables into one unified view

WITH unioned_historical AS (
    -- Nigeria
    SELECT
        ingestion_timestamp,
        country,
        city,
        lat,
        lon,
        dt,
        aqi,
        components.co,
        components.`no`,
        components.`no2`,
        components.o3,
        components.so2,
        components.pm2_5,
        components.pm10,
        components.nh3,
        'historical' as data_type
    FROM {{ source('bigquery', 'ext_air_quality_raw_Nigeria') }}

    UNION ALL

    -- USA
    SELECT
        ingestion_timestamp,
        country,
        city,
        lat,
        lon,
        dt,
        aqi,
        components.co,
        components.`no`,
        components.`no2`,
        components.o3,
        components.so2,
        components.pm2_5,
        components.pm10,
        components.nh3,
        'historical' as data_type
    FROM {{ source('bigquery', 'ext_air_quality_raw_USA') }}

    UNION ALL

    -- UK
    SELECT
        ingestion_timestamp,
        country,
        city,
        lat,
        lon,
        dt,
        aqi,
        components.co,
        components.`no`,
        components.`no2`,
        components.o3,
        components.so2,
        components.pm2_5,
        components.pm10,
        components.nh3,
        'historical' as data_type
    FROM {{ source('bigquery', 'ext_air_quality_raw_UK') }}

    UNION ALL

    -- India
    SELECT
        ingestion_timestamp,
        country,
        city,
        lat,
        lon,
        dt,
        aqi,
        components.co,
        components.`no`,
        components.`no2`,
        components.o3,
        components.so2,
        components.pm2_5,
        components.pm10,
        components.nh3,
        'historical' as data_type
    FROM {{ source('bigquery', 'ext_air_quality_raw_India') }}
)

SELECT
    -- Primary key
    ingestion_timestamp,
    country,
    city,
    dt as measurement_timestamp,

    -- Location data
    lat,
    lon,

    -- Air quality index
    aqi,

    -- Pollutant concentrations (μg/m³)
    co,
    `no`,
    `no2`,
    o3,
    so2,
    pm2_5,
    pm10,
    nh3,

    -- Metadata
    data_type,

    -- Data quality flags
    CASE
        WHEN aqi IS NULL THEN 1
        WHEN lat IS NULL OR lon IS NULL THEN 1
        WHEN dt IS NULL THEN 1
        ELSE 0
    END as is_data_quality_issue,

    -- Derived timestamps
    TIMESTAMP_SECONDS(dt) as measurement_datetime,
    DATE(TIMESTAMP_SECONDS(dt)) as measurement_date,
    EXTRACT(hour FROM TIMESTAMP_SECONDS(dt)) as measurement_hour,
    EXTRACT(dayofweek FROM TIMESTAMP_SECONDS(dt)) as measurement_day_of_week,
    EXTRACT(month FROM TIMESTAMP_SECONDS(dt)) as measurement_month,
    EXTRACT(year FROM TIMESTAMP_SECONDS(dt)) as measurement_year

FROM unioned_historical

-- Remove obvious data quality issues
WHERE dt IS NOT NULL
  AND lat IS NOT NULL
  AND lon IS NOT NULL
  AND aqi IS NOT NULL