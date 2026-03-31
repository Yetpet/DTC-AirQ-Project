{{ config(materialized='table') }}

-- Fact table: Daily Air Quality Summary
-- Aggregated daily metrics for trend analysis and reporting

SELECT
    -- Primary Key
    CONCAT(country, '_', city, '_', CAST(measurement_date AS STRING)) as daily_summary_id,

    -- Dimensions
    country,
    city,
    measurement_date,
    data_type,

    -- Daily Aggregates
    avg_aqi_rounded as daily_avg_aqi,
    min_aqi as daily_min_aqi,
    max_aqi as daily_max_aqi,
    aqi_variability,
    dominant_category,
    daily_air_quality_rating,

    -- Pollutant Daily Averages
    avg_co, avg_no, avg_no2, avg_o3, avg_so2, avg_pm2_5, avg_pm10, avg_nh3,

    -- Category Distribution
    hours_good, hours_moderate, hours_unhealthy_sensitive,
    hours_unhealthy, hours_very_unhealthy, hours_hazardous,

    -- Time Distribution
    morning_hours, afternoon_hours, evening_hours, night_hours,

    -- Derived Metrics
    pollution_load_index,
    data_completeness_pct,

    -- Health Impact Indicators
    CASE WHEN avg_aqi_rounded > 100 THEN 1 ELSE 0 END as had_unhealthy_days,
    CASE WHEN max_aqi > 150 THEN 1 ELSE 0 END as had_very_unhealthy_hours,
    CASE WHEN hours_hazardous > 0 THEN 1 ELSE 0 END as had_hazardous_hours,

    -- Trend Indicators
    CASE
        WHEN pollution_load_index > 100 THEN 'High Pollution Day'
        WHEN pollution_load_index > 50 THEN 'Moderate Pollution Day'
        ELSE 'Low Pollution Day'
    END as pollution_intensity,

    -- Data Quality
    total_measurements,
    quality_issues

FROM {{ ref('int_air_quality_daily_summary') }}

ORDER BY country, city, measurement_date