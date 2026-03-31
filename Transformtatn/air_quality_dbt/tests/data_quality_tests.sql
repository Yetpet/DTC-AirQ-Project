-- Test: Ensure AQI values are within valid range (0-500)
SELECT *
FROM {{ ref('stg_air_quality_historical') }}
WHERE aqi < 0 OR aqi > 500

-- Test: Ensure all measurements have valid coordinates
SELECT *
FROM {{ ref('stg_air_quality_historical') }}
WHERE lat < -90 OR lat > 90 OR lon < -180 OR lon > 180

-- Test: Ensure no duplicate measurements for same location and time
SELECT
    country,
    city,
    dt,
    COUNT(*) as duplicate_count
FROM {{ ref('stg_air_quality_historical') }}
GROUP BY country, city, dt
HAVING COUNT(*) > 1