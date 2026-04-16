
--- Start CTE
WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
weather_codes AS (
    SELECT *
    FROM {{ref('weather_codes')}}
),
--- Un CTE (Common Table Expression) es básicamente una tabla temporal que defines al inicio de una query con WITH 
---y puedes usar más adelante en el mismo código. 
---Se llama "expresión de tabla común" porque es una forma de nombrar y reutilizar una subquery.
add_features AS (
    SELECT *
    , timestamp::DATE AS date -- only date (year-month-day) as DATE data type
    , timestamp::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , TO_CHAR(timestamp, 'FMday') AS weekday        -- weekday name as TEXT            
        , DATE_PART('day', timestamp) AS date_day
        , DATE_PART('month', timestamp) AS date_month
        , DATE_PART('year', timestamp) AS date_year
        , DATE_PART('week', timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        ,(CASE 
            WHEN time BETWEEN '00:00:00' AND '05:59:00' THEN 'night'
            WHEN time BETWEEN '06:00:00' AND '18:00:00' THEN 'day'
            WHEN time BETWEEN '18:00:00' AND '23:59:00' THEN 'evening'
        END) AS day_part
    FROM add_features
)
----------------  Para ordenar las columnas:
SELECT
    h.airport_code,
    h.station_id,
    h.timestamp,
    h.temp_c,
    h.dewpoint_c,
    h.humidity_perc,
    h.precipitation_mm,
    h.snow_mm,
    h.wind_direction,
    h.wind_speed_kmh,
    h.wind_peakgust_kmh,
    h.pressure_hpa,
    h.sun_minutes,
    h.condition_code,
    w.description,
    h.date,
    h.time,
    h.hour,
    h.month_name,
    h.weekday,
    h.date_day,
    h.date_month,
    h.date_year,
    h.cw,
    h.day_part
FROM add_more_features h
LEFT JOIN weather_codes w
    ON h.condition_code = w.condition_code
/*
--- Comienza el JOIN nueva columna
SELECT --- para agregar "description"
    h.*,
    w.description
FROM add_more_features h
LEFT JOIN weather_codes w
    ON h.condition_code = w.condition_code

*/


