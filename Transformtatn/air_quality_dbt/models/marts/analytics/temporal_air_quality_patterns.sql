{{ config(materialized='table') }}

-- Analytics: Seasonal and Temporal Air Quality Patterns
-- Analysis of air quality patterns by season, time of day, and day of week

WITH temporal_patterns AS (
    SELECT
        country,
        city,
        measurement_year,
        measurement_month,

        -- Seasonal categorization
        CASE
            WHEN measurement_month IN (12, 1, 2) THEN 'Winter'
            WHEN measurement_month IN (3, 4, 5) THEN 'Spring'
            WHEN measurement_month IN (6, 7, 8) THEN 'Summer'
            WHEN measurement_month IN (9, 10, 11) THEN 'Fall'
        END as season,

        -- Time-based groupings
        measurement_hour,
        measurement_day_of_week,
        time_of_day,

        -- Air Quality Metrics
        AVG(aqi) as avg_aqi,
        MIN(aqi) as min_aqi,
        MAX(aqi) as max_aqi,
        STDDEV(aqi) as aqi_stddev,

        -- Pollutant Patterns
        AVG(pm2_5) as avg_pm25,
        AVG(pm10) as avg_pm10,
        AVG(o3) as avg_o3,
        AVG(`no2`) as avg_no2,

        -- Category Distribution
        COUNT(*) as total_measurements,
        COUNT(CASE WHEN aqi_category = 'Good' THEN 1 END) as good_measurements,
        COUNT(CASE WHEN aqi_category = 'Moderate' THEN 1 END) as moderate_measurements,
        COUNT(CASE WHEN aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 END) as unhealthy_measurements

    FROM {{ ref('int_air_quality_unified') }}
    WHERE data_type = 'historical'
    GROUP BY country, city, measurement_year, measurement_month, measurement_hour, measurement_day_of_week, time_of_day
),

seasonal_analysis AS (
    SELECT
        country,
        city,
        season,
        measurement_year,

        AVG(avg_aqi) as seasonal_avg_aqi,
        AVG(aqi_stddev) as seasonal_aqi_variability,
        AVG(avg_pm25) as seasonal_avg_pm25,
        AVG(avg_pm10) as seasonal_avg_pm10,

        SUM(total_measurements) as seasonal_total_measurements,
        SUM(good_measurements) as seasonal_good_measurements,
        SUM(moderate_measurements) as seasonal_moderate_measurements,
        SUM(unhealthy_measurements) as seasonal_unhealthy_measurements,

        -- Seasonal Health Impact
        ROUND(SUM(unhealthy_measurements) / NULLIF(SUM(total_measurements), 0) * 100, 2) as seasonal_unhealthy_percentage

    FROM temporal_patterns
    GROUP BY country, city, season, measurement_year
),

hourly_patterns AS (
    SELECT
        country,
        city,
        measurement_hour,
        time_of_day,

        AVG(avg_aqi) as hourly_avg_aqi,
        AVG(avg_pm25) as hourly_avg_pm25,
        AVG(avg_o3) as hourly_avg_o3,

        SUM(total_measurements) as hourly_total_measurements,
        ROUND(AVG(good_measurements / NULLIF(total_measurements, 0) * 100), 2) as hourly_good_percentage

    FROM temporal_patterns
    GROUP BY country, city, measurement_hour, time_of_day
),

weekday_patterns AS (
    SELECT
        country,
        city,
        measurement_day_of_week,
        CASE
            WHEN measurement_day_of_week IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,

        AVG(avg_aqi) as weekday_avg_aqi,
        AVG(avg_pm25) as weekday_avg_pm25,
        AVG(aqi_stddev) as weekday_aqi_variability

    FROM temporal_patterns
    GROUP BY country, city, measurement_day_of_week
),

peak_hours AS (
    SELECT
        country,
        city,
        MAX(hourly_avg_aqi) as max_hourly_avg_aqi,
        ARRAY_AGG(measurement_hour ORDER BY hourly_avg_aqi DESC LIMIT 1)[OFFSET(0)] as peak_pollution_hour,
        ARRAY_AGG(measurement_hour ORDER BY hourly_avg_aqi ASC LIMIT 1)[OFFSET(0)] as best_air_quality_hour
    FROM hourly_patterns
    GROUP BY country, city
),

weekday_weekend_comparison AS (
    SELECT
        country,
        city,
        MAX(CASE WHEN day_type = 'Weekday' THEN weekday_avg_aqi END) as weekday_avg_aqi,
        MAX(CASE WHEN day_type = 'Weekend' THEN weekday_avg_aqi END) as weekend_avg_aqi
    FROM weekday_patterns
    GROUP BY country, city
)

SELECT
    -- Seasonal Analysis
    s.country,
    s.city,
    s.season,
    s.measurement_year,
    s.seasonal_avg_aqi,
    s.seasonal_aqi_variability,
    s.seasonal_avg_pm25,
    s.seasonal_avg_pm10,
    s.seasonal_unhealthy_percentage,

    -- Peak Pollution Hours (from hourly patterns)
    p.peak_pollution_hour,
    p.best_air_quality_hour,

    -- Weekday vs Weekend Comparison
    c.weekday_avg_aqi,
    c.weekend_avg_aqi,

    -- Seasonal Ranking
    ROW_NUMBER() OVER (PARTITION BY s.country, s.measurement_year
                      ORDER BY s.seasonal_avg_aqi ASC) as seasonal_cleanliness_rank

FROM seasonal_analysis s
LEFT JOIN peak_hours p
  ON p.country = s.country
  AND p.city = s.city
LEFT JOIN weekday_weekend_comparison c
  ON c.country = s.country
  AND c.city = s.city
ORDER BY s.country, s.city, s.measurement_year,
         CASE s.season WHEN 'Winter' THEN 1 WHEN 'Spring' THEN 2 WHEN 'Summer' THEN 3 WHEN 'Fall' THEN 4 END