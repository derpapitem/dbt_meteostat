WITH daily_weather AS (
    SELECT *
    --FROM prep_weather_daily
    FROM {{ref('prep_weather_daily')}}
)
SELECT
    airport_code,
    date_year,
    cw AS calendar_week,
    -- temperatura
    ROUND(AVG(avg_temp_c), 2)        AS avg_temp_c,
    MIN(min_temp_c)                  AS min_temp_c,
    MAX(max_temp_c)                  AS max_temp_c,
    -- precipitación y nieve
    SUM(precipitation_mm)            AS total_precipitation_mm,
    MAX(max_snow_mm)                 AS max_snow_mm,
    -- viento
    MODE() WITHIN GROUP (ORDER BY avg_wind_direction) AS mode_wind_direction,
    ROUND(AVG(avg_wind_speed), 2)    AS avg_wind_speed,
    MAX(avg_peakgust)                AS max_peakgust,
    -- presión y sol
    ROUND(AVG(avg_pressure_hpa), 2)  AS avg_pressure_hpa,
    SUM(sun_minutes)                 AS total_sun_minutes
FROM daily_weather
GROUP BY airport_code, date_year, cw
ORDER BY airport_code, date_year, cw