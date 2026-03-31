{{ config(materialized='view') }}

-- Intermediate model: Time-series aggregations for trend analysis

WITH daily_aggregations AS (
    SELECT
        country,
        city,
        measurement_date,
        measurement_year,
        measurement_month,
        data_type,

        -- Air Quality Metrics
        AVG(aqi) as avg_aqi,
        MIN(aqi) as min_aqi,
        MAX(aqi) as max_aqi,
        STDDEV(aqi) as stddev_aqi,

        -- Pollutant Averages
        AVG(co) as avg_co,
        AVG(`no`) as avg_no,
        AVG(`no2`) as avg_no2,
        AVG(o3) as avg_o3,
        AVG(so2) as avg_so2,
        AVG(pm2_5) as avg_pm2_5,
        AVG(pm10) as avg_pm10,
        AVG(nh3) as avg_nh3,

        -- Category Distribution
        COUNT(CASE WHEN aqi_category = 'Good' THEN 1 END) as hours_good,
        COUNT(CASE WHEN aqi_category = 'Moderate' THEN 1 END) as hours_moderate,
        COUNT(CASE WHEN aqi_category = 'Unhealthy for Sensitive Groups' THEN 1 END) as hours_unhealthy_sensitive,
        COUNT(CASE WHEN aqi_category = 'Unhealthy' THEN 1 END) as hours_unhealthy,
        COUNT(CASE WHEN aqi_category = 'Very Unhealthy' THEN 1 END) as hours_very_unhealthy,
        COUNT(CASE WHEN aqi_category = 'Hazardous' THEN 1 END) as hours_hazardous,

        -- Data Quality
        COUNT(*) as total_measurements,
        COUNT(CASE WHEN is_data_quality_issue = 1 THEN 1 END) as quality_issues,

        -- Time Distribution
        COUNT(CASE WHEN time_of_day = 'Morning' THEN 1 END) as morning_hours,
        COUNT(CASE WHEN time_of_day = 'Afternoon' THEN 1 END) as afternoon_hours,
        COUNT(CASE WHEN time_of_day = 'Evening' THEN 1 END) as evening_hours,
        COUNT(CASE WHEN time_of_day = 'Night' THEN 1 END) as night_hours

    FROM {{ ref('int_air_quality_unified') }}
    GROUP BY country, city, measurement_date, measurement_year, measurement_month, data_type
),

hourly_aggregations AS (
    SELECT
        country,
        city,
        measurement_date,
        measurement_hour,
        data_type,

        AVG(aqi) as hourly_avg_aqi,
        MIN(aqi) as hourly_min_aqi,
        MAX(aqi) as hourly_max_aqi,
        COUNT(*) as measurements_in_hour

    FROM {{ ref('int_air_quality_unified') }}
    GROUP BY country, city, measurement_date, measurement_hour, data_type
)

SELECT
    d.*,

    -- Daily Statistics
    ROUND(d.avg_aqi, 2) as avg_aqi_rounded,
    ROUND(d.stddev_aqi, 2) as aqi_variability,

    -- Dominant Category (most frequent)
    CASE
        WHEN d.hours_hazardous = GREATEST(d.hours_good, d.hours_moderate, d.hours_unhealthy_sensitive, d.hours_unhealthy, d.hours_very_unhealthy, d.hours_hazardous) THEN 'Hazardous'
        WHEN d.hours_very_unhealthy = GREATEST(d.hours_good, d.hours_moderate, d.hours_unhealthy_sensitive, d.hours_unhealthy, d.hours_very_unhealthy, d.hours_hazardous) THEN 'Very Unhealthy'
        WHEN d.hours_unhealthy = GREATEST(d.hours_good, d.hours_moderate, d.hours_unhealthy_sensitive, d.hours_unhealthy, d.hours_very_unhealthy, d.hours_hazardous) THEN 'Unhealthy'
        WHEN d.hours_unhealthy_sensitive = GREATEST(d.hours_good, d.hours_moderate, d.hours_unhealthy_sensitive, d.hours_unhealthy, d.hours_very_unhealthy, d.hours_hazardous) THEN 'Unhealthy for Sensitive Groups'
        WHEN d.hours_moderate = GREATEST(d.hours_good, d.hours_moderate, d.hours_unhealthy_sensitive, d.hours_unhealthy, d.hours_very_unhealthy, d.hours_hazardous) THEN 'Moderate'
        ELSE 'Good'
    END as dominant_category,

    -- Air Quality Trend Indicators
    CASE
        WHEN d.avg_aqi <= 50 THEN 'Excellent'
        WHEN d.avg_aqi <= 100 THEN 'Good'
        WHEN d.avg_aqi <= 150 THEN 'Moderate'
        WHEN d.avg_aqi <= 200 THEN 'Poor'
        ELSE 'Very Poor'
    END as daily_air_quality_rating,

    -- Pollution Load (weighted average of pollutants)
    ROUND(
        (COALESCE(d.avg_pm2_5, 0) * 1.0) +
        (COALESCE(d.avg_pm10, 0) * 0.5) +
        (COALESCE(d.avg_o3, 0) * 0.8) +
        (COALESCE(d.avg_no2, 0) * 0.7) +
        (COALESCE(d.avg_so2, 0) * 0.6) +
        (COALESCE(d.avg_co, 0) * 0.1) +
        (COALESCE(d.avg_nh3, 0) * 0.3), 2
    ) as pollution_load_index,

    -- Data Completeness
    ROUND((d.total_measurements - d.quality_issues) / d.total_measurements * 100, 2) as data_completeness_pct

FROM daily_aggregations d