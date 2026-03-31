{{ config(materialized='table') }}

-- Analytics: Forecast Accuracy Analysis
-- Compare forecast predictions with actual measurements

WITH forecast_actual_comparison AS (
    -- Get forecasts and their corresponding actual measurements
    SELECT
        f.country,
        f.city,
        f.forecast_date,
        f.forecast_hour,
        f.aqi as forecasted_aqi,
        CASE
            WHEN f.aqi BETWEEN 0 AND 50 THEN 'Good'
            WHEN f.aqi BETWEEN 51 AND 100 THEN 'Moderate'
            WHEN f.aqi BETWEEN 101 AND 150 THEN 'Unhealthy for Sensitive Groups'
            WHEN f.aqi BETWEEN 151 AND 200 THEN 'Unhealthy'
            WHEN f.aqi BETWEEN 201 AND 300 THEN 'Very Unhealthy'
            WHEN f.aqi > 300 THEN 'Hazardous'
            ELSE 'Unknown'
        END as forecasted_category,

        -- Find the actual measurement for the same time
        a.aqi as actual_aqi,
        CASE
            WHEN a.aqi BETWEEN 0 AND 50 THEN 'Good'
            WHEN a.aqi BETWEEN 51 AND 100 THEN 'Moderate'
            WHEN a.aqi BETWEEN 101 AND 150 THEN 'Unhealthy for Sensitive Groups'
            WHEN a.aqi BETWEEN 151 AND 200 THEN 'Unhealthy'
            WHEN a.aqi BETWEEN 201 AND 300 THEN 'Very Unhealthy'
            WHEN a.aqi > 300 THEN 'Hazardous'
            ELSE 'Unknown'
        END as actual_category,

        -- Forecast metadata
        f.forecast_created_date,
        f.hours_ahead,
        TIMESTAMP_DIFF(f.forecast_datetime, TIMESTAMP(f.forecast_created_date), HOUR) as forecast_lead_time_hours,

        -- Accuracy metrics
        ABS(f.aqi - a.aqi) as aqi_absolute_error,
        CASE WHEN 
            CASE
                WHEN f.aqi BETWEEN 0 AND 50 THEN 'Good'
                WHEN f.aqi BETWEEN 51 AND 100 THEN 'Moderate'
                WHEN f.aqi BETWEEN 101 AND 150 THEN 'Unhealthy for Sensitive Groups'
                WHEN f.aqi BETWEEN 151 AND 200 THEN 'Unhealthy'
                WHEN f.aqi BETWEEN 201 AND 300 THEN 'Very Unhealthy'
                WHEN f.aqi > 300 THEN 'Hazardous'
                ELSE 'Unknown'
            END =
            CASE
                WHEN a.aqi BETWEEN 0 AND 50 THEN 'Good'
                WHEN a.aqi BETWEEN 51 AND 100 THEN 'Moderate'
                WHEN a.aqi BETWEEN 101 AND 150 THEN 'Unhealthy for Sensitive Groups'
                WHEN a.aqi BETWEEN 151 AND 200 THEN 'Unhealthy'
                WHEN a.aqi BETWEEN 201 AND 300 THEN 'Very Unhealthy'
                WHEN a.aqi > 300 THEN 'Hazardous'
                ELSE 'Unknown'
            END
        THEN 1 ELSE 0 END as category_accuracy,
        CASE
            WHEN ABS(f.aqi - a.aqi) <= 10 THEN 'Excellent'
            WHEN ABS(f.aqi - a.aqi) <= 25 THEN 'Good'
            WHEN ABS(f.aqi - a.aqi) <= 50 THEN 'Fair'
            ELSE 'Poor'
        END as forecast_accuracy_rating

    FROM {{ ref('stg_air_quality_forecast') }} f
    LEFT JOIN {{ ref('stg_air_quality_current') }} a
        ON f.country = a.country
        AND f.city = a.city
        AND DATE(f.forecast_datetime) = DATE(a.measurement_datetime)
        AND EXTRACT(hour FROM f.forecast_datetime) = EXTRACT(hour FROM a.measurement_datetime)

    WHERE DATE(f.forecast_datetime) >= CURRENT_DATE()
      AND a.measurement_datetime IS NOT NULL
),

accuracy_metrics AS (
    SELECT
        country,
        city,
        forecast_lead_time_hours,

        COUNT(*) as total_forecasts,
        AVG(aqi_absolute_error) as avg_absolute_error,
        STDDEV(aqi_absolute_error) as error_stddev,
        MIN(aqi_absolute_error) as min_error,
        MAX(aqi_absolute_error) as max_error,

        -- Accuracy by category
        AVG(CASE WHEN forecasted_category = 'Good' THEN aqi_absolute_error END) as good_forecast_error,
        AVG(CASE WHEN forecasted_category = 'Moderate' THEN aqi_absolute_error END) as moderate_forecast_error,
        AVG(CASE WHEN forecasted_category = 'Unhealthy' THEN aqi_absolute_error END) as unhealthy_forecast_error,

        -- Overall accuracy
        AVG(category_accuracy) * 100 as category_accuracy_percentage,
        COUNT(CASE WHEN forecast_accuracy_rating = 'Excellent' THEN 1 END) as excellent_forecasts,
        COUNT(CASE WHEN forecast_accuracy_rating = 'Good' THEN 1 END) as good_forecasts,
        COUNT(CASE WHEN forecast_accuracy_rating = 'Fair' THEN 1 END) as fair_forecasts,
        COUNT(CASE WHEN forecast_accuracy_rating = 'Poor' THEN 1 END) as poor_forecasts

    FROM forecast_actual_comparison
    GROUP BY country, city, forecast_lead_time_hours
)

SELECT
    *,
    -- Forecast Reliability Score (0-100, higher is better)
    ROUND(
        (category_accuracy_percentage * 0.6) +
        ((excellent_forecasts + good_forecasts) / NULLIF(total_forecasts, 0) * 100 * 0.4), 2
    ) as forecast_reliability_score,

    -- Error Analysis
    ROUND(avg_absolute_error, 2) as avg_aqi_error,
    ROUND(error_stddev, 2) as error_variability,

    -- Forecast Quality Rating
    CASE
        WHEN ROUND((category_accuracy_percentage * 0.6) + ((excellent_forecasts + good_forecasts) / NULLIF(total_forecasts, 0) * 100 * 0.4), 2) >= 80 THEN 'Highly Reliable'
        WHEN ROUND((category_accuracy_percentage * 0.6) + ((excellent_forecasts + good_forecasts) / NULLIF(total_forecasts, 0) * 100 * 0.4), 2) >= 70 THEN 'Reliable'
        WHEN ROUND((category_accuracy_percentage * 0.6) + ((excellent_forecasts + good_forecasts) / NULLIF(total_forecasts, 0) * 100 * 0.4), 2) >= 60 THEN 'Moderately Reliable'
        WHEN ROUND((category_accuracy_percentage * 0.6) + ((excellent_forecasts + good_forecasts) / NULLIF(total_forecasts, 0) * 100 * 0.4), 2) >= 50 THEN 'Low Reliability'
        ELSE 'Unreliable'
    END as forecast_quality_rating,

    -- Lead Time Impact
    CASE
        WHEN forecast_lead_time_hours <= 24 THEN 'Short-term (1 day)'
        WHEN forecast_lead_time_hours <= 72 THEN 'Medium-term (2-3 days)'
        ELSE 'Long-term (4+ days)'
    END as forecast_horizon

FROM accuracy_metrics
ORDER BY country, city, forecast_lead_time_hours