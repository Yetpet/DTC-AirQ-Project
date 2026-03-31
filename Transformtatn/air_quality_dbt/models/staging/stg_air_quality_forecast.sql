{{ config(materialized='view') }}

-- Staging model for forecast air quality data

SELECT
    -- Primary key
    ingestion_timestamp,
    country,
    city,
    dt as forecast_timestamp,

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
    'forecast' as data_type,

    -- Data quality flags
    CASE
        WHEN aqi IS NULL THEN 1
        WHEN lat IS NULL OR lon IS NULL THEN 1
        WHEN dt IS NULL THEN 1
        ELSE 0
    END as is_data_quality_issue,

    -- Derived timestamps
    TIMESTAMP_SECONDS(dt) as forecast_datetime,
    DATE(TIMESTAMP_SECONDS(dt)) as forecast_date,
    EXTRACT(hour FROM TIMESTAMP_SECONDS(dt)) as forecast_hour,
    EXTRACT(dayofweek FROM TIMESTAMP_SECONDS(dt)) as forecast_day_of_week,
    EXTRACT(month FROM TIMESTAMP_SECONDS(dt)) as forecast_month,
    EXTRACT(year FROM TIMESTAMP_SECONDS(dt)) as forecast_year,

    -- Forecast-specific fields
    DATE(ingestion_timestamp) as forecast_created_date,
    TIMESTAMP_SECONDS(dt) - ingestion_timestamp as hours_ahead

FROM {{ source('bigquery', 'ext_air_quality_forecast_all') }}

-- Remove obvious data quality issues
WHERE dt IS NOT NULL
  AND lat IS NOT NULL
  AND lon IS NOT NULL
  AND aqi IS NOT NULL