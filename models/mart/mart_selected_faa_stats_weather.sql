/*SELECT MIN(date), MAX(date)
FROM prep_weather_daily;*/

WITH weather AS (
    SELECT *
    FROM {{ref('prep_weather_daily')}}
    --FROM prep_weather_daily
),
flights AS (
    SELECT *
    FROM {{ref('prep_flights')}}
    --FROM prep_flights
),
airports AS (
    SELECT *
    FROM {{ref('prep_airports')}}
	--FROM prep_airports
),
-- vuelos de salida por aeropuerto y día
departures AS (
    SELECT
        origin AS airport_code,
        flight_date AS date,
        COUNT(*) AS total_departures,
        COUNT(DISTINCT dest) AS unique_departure_connections,
        SUM(cancelled) AS cancelled_departures,
        SUM(diverted) AS diverted_departures,
        COUNT(DISTINCT tail_number) AS unique_airplanes_dep,
        COUNT(DISTINCT airline) AS unique_airlines_dep
    FROM flights
    GROUP BY origin, flight_date
),
-- vuelos de llegada por aeropuerto y día
arrivals AS (
    SELECT
        dest AS airport_code,
        flight_date AS date,
        COUNT(*) AS total_arrivals,
        COUNT(DISTINCT origin) AS unique_arrival_connections,
        SUM(cancelled) AS cancelled_arrivals,
        SUM(diverted) AS diverted_arrivals
    FROM flights
    GROUP BY dest, flight_date
),
-- unir salidas y llegadas
flights_combined AS (
    SELECT
        COALESCE(d.airport_code, a.airport_code) AS airport_code,
        COALESCE(d.date, a.date) AS date,
        COALESCE(d.total_departures, 0) AS total_departures,
        COALESCE(a.total_arrivals, 0) AS total_arrivals,
        COALESCE(d.total_departures, 0) + COALESCE(a.total_arrivals, 0) AS total_flights_planned,
        COALESCE(d.cancelled_departures, 0) + COALESCE(a.cancelled_arrivals, 0) AS total_cancelled,
        COALESCE(d.diverted_departures, 0) + COALESCE(a.diverted_arrivals, 0) AS total_diverted,
        COALESCE(d.unique_departure_connections, 0) AS unique_departure_connections,
        COALESCE(a.unique_arrival_connections, 0) AS unique_arrival_connections,
        COALESCE(d.unique_airplanes_dep, 0) AS unique_airplanes,
        COALESCE(d.unique_airlines_dep, 0) AS unique_airlines
    FROM departures d
    FULL OUTER JOIN arrivals a
        ON d.airport_code = a.airport_code
        AND d.date = a.date
)
SELECT
    f.airport_code,
    f.date,
    -- info del aeropuerto
    ap.name AS airport_name,
    ap.city AS airport_city,
    ap.country AS airport_country,
    -- vuelos
    f.unique_departure_connections,
    f.unique_arrival_connections,
    f.total_flights_planned,
    f.total_cancelled,
    f.total_diverted,
    f.total_flights_planned - f.total_cancelled AS total_flights_occurred,
    f.unique_airplanes,
    f.unique_airlines,
    -- clima
    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed,
    w.avg_peakgust 
FROM flights_combined f
LEFT JOIN {{ref('prep_airports')}} ap
--LEFT JOIN prep_airports ap
    ON f.airport_code = ap.faa
INNER JOIN weather w
    ON w.airport_code = f.airport_code
    AND w.date = f.date

    
    
    