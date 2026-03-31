{{ config(materialized='view') }}

-- Staging model for current air quality data

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
    components.co,
    components.`no`,
    components.`no2`,
    components.o3,
    components.so2,
    components.pm2_5,
    components.pm10,
    components.nh3,

    -- Metadata
    'current' as data_type,

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

FROM {{ source('bigquery', 'ext_air_quality_current_all') }}

-- Remove obvious data quality issues
WHERE dt IS NOT NULL
  AND lat IS NOT NULL
  AND lon IS NOT NULL
  AND aqi IS NOT NULL