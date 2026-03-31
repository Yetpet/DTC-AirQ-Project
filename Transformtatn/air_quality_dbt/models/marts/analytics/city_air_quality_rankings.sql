{{ config(materialized='table') }}

-- Analytics: City Air Quality Rankings and Comparisons
-- Monthly rankings of cities by air quality performance

WITH monthly_city_stats AS (
    SELECT
        country,
        city,
        EXTRACT(year FROM measurement_date) as measurement_year,
        EXTRACT(month FROM measurement_date) as measurement_month,
        DATE_TRUNC(measurement_date, MONTH) as month_start_date,

        -- Air Quality Metrics
        AVG(avg_aqi_rounded) as monthly_avg_aqi,
        AVG(max_aqi) as monthly_max_aqi,
        AVG(aqi_variability) as monthly_aqi_variability,

        -- Pollution Metrics
        AVG(pollution_load_index) as monthly_pollution_load,
        AVG(avg_pm2_5) as monthly_avg_pm25,
        AVG(avg_pm10) as monthly_avg_pm10,

        -- Health Impact Days
        SUM(CASE WHEN dominant_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) as unhealthy_days,
        COUNT(*) as total_days,
        SUM(CASE WHEN hours_hazardous > 0 THEN 1 ELSE 0 END) as hazardous_days,

        -- Data Quality
        AVG(data_completeness_pct) as avg_data_completeness

    FROM {{ ref('int_air_quality_daily_summary') }}
    WHERE data_type = 'historical'  -- Only ranking on historical data
    GROUP BY country, city, measurement_year, measurement_month, DATE_TRUNC(measurement_date, MONTH)
),

city_rankings AS (
    SELECT
        *,
        -- Monthly Rankings within Country
        ROW_NUMBER() OVER (PARTITION BY country, measurement_year, measurement_month
                          ORDER BY monthly_avg_aqi ASC) as country_aqi_rank,
        ROW_NUMBER() OVER (PARTITION BY country, measurement_year, measurement_month
                          ORDER BY monthly_pollution_load ASC) as country_pollution_rank,

        -- Global Rankings
        ROW_NUMBER() OVER (PARTITION BY measurement_year, measurement_month
                          ORDER BY monthly_avg_aqi ASC) as global_aqi_rank,
        ROW_NUMBER() OVER (PARTITION BY measurement_year, measurement_month
                          ORDER BY monthly_pollution_load ASC) as global_pollution_rank,

        -- Performance Categories
        CASE
            WHEN monthly_avg_aqi <= 50 THEN 'Excellent'
            WHEN monthly_avg_aqi <= 100 THEN 'Good'
            WHEN monthly_avg_aqi <= 150 THEN 'Moderate'
            WHEN monthly_avg_aqi <= 200 THEN 'Poor'
            ELSE 'Very Poor'
        END as air_quality_performance,

        -- Health Risk Assessment
        CASE
            WHEN unhealthy_days / NULLIF(total_days, 0) > 0.5 THEN 'High Risk'
            WHEN unhealthy_days / NULLIF(total_days, 0) > 0.25 THEN 'Moderate Risk'
            WHEN unhealthy_days / NULLIF(total_days, 0) > 0.1 THEN 'Low Risk'
            ELSE 'Minimal Risk'
        END as health_risk_level,

        -- Pollution Trend Indicators
        ROUND(unhealthy_days / NULLIF(total_days, 0) * 100, 2) as unhealthy_days_percentage,
        ROUND(hazardous_days / NULLIF(total_days, 0) * 100, 2) as hazardous_days_percentage

    FROM monthly_city_stats
)

SELECT
    *,
    -- City Comparison Metrics
    CASE
        WHEN country_aqi_rank = 1 THEN 'Best in Country'
        WHEN country_aqi_rank <= 3 THEN 'Top 3 in Country'
        WHEN country_aqi_rank >= (SELECT COUNT(*) FROM monthly_city_stats m2
                                  WHERE m2.country = city_rankings.country
                                    AND m2.measurement_year = city_rankings.measurement_year
                                    AND m2.measurement_month = city_rankings.measurement_month) - 2
        THEN 'Worst in Country'
        ELSE 'Average in Country'
    END as country_performance_status,

    -- Year-over-Year Change Indicators (placeholder - would need previous month data)
    monthly_avg_aqi as current_month_aqi,
    monthly_pollution_load as current_month_pollution

FROM city_rankings
ORDER BY measurement_year DESC, measurement_month DESC, global_aqi_rank ASC