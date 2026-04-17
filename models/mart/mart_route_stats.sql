WITH flights AS (
    SELECT *
    FROM {{ref('prep_flights')}}
    --FROM prep_flights
),
airports AS (
    SELECT *
    FROM {{ref('prep_airports')}}
    --FROM prep_airports
),
routes AS (
    SELECT
        f.origin,
        f.dest,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT f.tail_number) AS unique_airplanes,
        COUNT(DISTINCT f.airline) AS unique_airlines,
        ROUND(AVG(f.actual_elapsed_time), 2) AS avg_elapsed_time_min,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay_min,
        MAX(f.arr_delay) AS max_arrival_delay_min,
        MIN(f.arr_delay) AS min_arrival_delay_min,
        SUM(f.cancelled) AS total_cancelled,
        SUM(f.diverted) AS total_diverted
    FROM flights f
    GROUP BY f.origin, f.dest
)
SELECT
    r.*,
    -- origen
    ao.name AS origin_name,
    ao.city AS origin_city,
    ao.country AS origin_country,
    -- destino
    ad.name AS dest_name,
    ad.city AS dest_city,
    ad.country AS dest_country
FROM routes r
LEFT JOIN airports ao ON r.origin = ao.faa
LEFT JOIN airports ad ON r.dest = ad.faa
	
	



/*- origin airport code
- destination airport code 
- total flights on this route
- unique airplanes
- unique airlines
- on average what is the actual elapsed time
- on average what is the delay on arrival
- what was the max delay?
- what was the min delay?
- total number of cancelled 
- total number of diverted
- add city, country and name for both, origin and destination, airports*/