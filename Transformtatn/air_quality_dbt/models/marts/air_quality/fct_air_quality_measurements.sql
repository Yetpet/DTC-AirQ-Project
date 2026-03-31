{{ config(materialized='table') }}

-- Fact table: Air Quality Measurements
-- Core dimensional model for air quality analysis

SELECT
    -- Primary Key (composite)
    CONCAT(country, '_', city, '_', CAST(measurement_datetime AS STRING)) as measurement_id,

    -- Dimensions
    country,
    city,
    measurement_datetime,
    measurement_date,
    measurement_hour,
    measurement_month,
    measurement_year,
    measurement_day_of_week,
    time_of_day,
    data_type,

    -- Location (could be moved to dimension table if needed)
    lat,
    lon,

    -- Facts
    aqi,
    aqi_category,
    aqi_category_code,
    health_impact_level,
    pollution_level,
    primary_pollutant,

    -- Pollutant Measurements
    co, `no`, `no2`, o3, so2, pm2_5, pm10, nh3,

    -- Metadata
    ingestion_timestamp,
    is_data_quality_issue,

    -- Derived Metrics
    CASE WHEN aqi > 100 THEN 1 ELSE 0 END as is_unhealthy,
    CASE WHEN aqi > 150 THEN 1 ELSE 0 END as is_very_unhealthy,
    CASE WHEN pm2_5 > 35 THEN 1 ELSE 0 END as is_pm2_5_exceeded,
    CASE WHEN pm10 > 150 THEN 1 ELSE 0 END as is_pm10_exceeded,

    -- Time-based flags
    CASE WHEN measurement_day_of_week IN (1, 7) THEN 1 ELSE 0 END as is_weekend,
    CASE WHEN measurement_hour BETWEEN 7 AND 19 THEN 1 ELSE 0 END as is_business_hours

FROM {{ ref('int_air_quality_unified') }}

-- Only include high-quality data for reporting
WHERE is_data_quality_issue = 0