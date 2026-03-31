{{ config(materialized='view') }}

-- Intermediate model: Unified air quality data with categorization
-- Combines historical, current, and forecast data with standardized categorization

WITH unified_data AS (
    -- Historical data
    SELECT
        ingestion_timestamp,
        country,
        city,
        measurement_timestamp as timestamp,
        lat,
        lon,
        aqi,
        co, `no`, `no2`, o3, so2, pm2_5, pm10, nh3,
        data_type,
        is_data_quality_issue,
        measurement_datetime,
        measurement_date,
        measurement_hour,
        measurement_day_of_week,
        measurement_month,
        measurement_year
    FROM {{ ref('stg_air_quality_historical') }}

    UNION ALL

    -- Current data
    SELECT
        ingestion_timestamp,
        country,
        city,
        measurement_timestamp as timestamp,
        lat,
        lon,
        aqi,
        co, `no`, `no2`, o3, so2, pm2_5, pm10, nh3,
        data_type,
        is_data_quality_issue,
        measurement_datetime,
        measurement_date,
        measurement_hour,
        measurement_day_of_week,
        measurement_month,
        measurement_year
    FROM {{ ref('stg_air_quality_current') }}

    UNION ALL

    -- Forecast data (renamed columns for consistency)
    SELECT
        ingestion_timestamp,
        country,
        city,
        forecast_timestamp as timestamp,
        lat,
        lon,
        aqi,
        co, `no`, `no2`, o3, so2, pm2_5, pm10, nh3,
        data_type,
        is_data_quality_issue,
        forecast_datetime as measurement_datetime,
        forecast_date as measurement_date,
        forecast_hour as measurement_hour,
        forecast_day_of_week as measurement_day_of_week,
        forecast_month as measurement_month,
        forecast_year as measurement_year
    FROM {{ ref('stg_air_quality_forecast') }}
)

SELECT
    *,
    -- AQI Category based on EPA standards
    CASE
        WHEN aqi BETWEEN 0 AND 50 THEN 'Good'
        WHEN aqi BETWEEN 51 AND 100 THEN 'Moderate'
        WHEN aqi BETWEEN 101 AND 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN aqi BETWEEN 151 AND 200 THEN 'Unhealthy'
        WHEN aqi BETWEEN 201 AND 300 THEN 'Very Unhealthy'
        WHEN aqi > 300 THEN 'Hazardous'
        ELSE 'Unknown'
    END as aqi_category,

    -- AQI Category Code for easier analysis
    CASE
        WHEN aqi BETWEEN 0 AND 50 THEN 1
        WHEN aqi BETWEEN 51 AND 100 THEN 2
        WHEN aqi BETWEEN 101 AND 150 THEN 3
        WHEN aqi BETWEEN 151 AND 200 THEN 4
        WHEN aqi BETWEEN 201 AND 300 THEN 5
        WHEN aqi > 300 THEN 6
        ELSE 0
    END as aqi_category_code,

    -- Health Impact Level
    CASE
        WHEN aqi BETWEEN 0 AND 50 THEN 'Low'
        WHEN aqi BETWEEN 51 AND 100 THEN 'Moderate'
        WHEN aqi BETWEEN 101 AND 150 THEN 'High for Sensitive Groups'
        WHEN aqi BETWEEN 151 AND 200 THEN 'High'
        WHEN aqi BETWEEN 201 AND 300 THEN 'Very High'
        WHEN aqi > 300 THEN 'Extreme'
        ELSE 'Unknown'
    END as health_impact_level,

    -- Primary Pollutant (highest concentration relative to safe levels)
    CASE
        WHEN pm2_5 > 35 THEN 'PM2.5'
        WHEN pm10 > 150 THEN 'PM10'
        WHEN o3 > 100 THEN 'O3'
        WHEN `no2` > 100 THEN 'NO2'
        WHEN so2 > 75 THEN 'SO2'
        WHEN co > 10000 THEN 'CO'
        WHEN nh3 > 200 THEN 'NH3'
        ELSE 'Multiple/None'
    END as primary_pollutant,

    -- Pollution Level
    CASE
        WHEN aqi <= 50 THEN 'Clean'
        WHEN aqi <= 100 THEN 'Acceptable'
        WHEN aqi <= 150 THEN 'Concerning'
        WHEN aqi <= 200 THEN 'Poor'
        WHEN aqi <= 300 THEN 'Very Poor'
        ELSE 'Critical'
    END as pollution_level,

    -- Time of Day Category
    CASE
        WHEN measurement_hour BETWEEN 0 AND 5 THEN 'Midnight'
        WHEN measurement_hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN measurement_hour BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN measurement_hour BETWEEN 18 AND 21 THEN 'Evening'
        WHEN measurement_hour BETWEEN 22 AND 5 THEN 'Night'
        ELSE 'Unknown'
    END as time_of_day

FROM unified_data