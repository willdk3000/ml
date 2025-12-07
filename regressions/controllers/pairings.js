//No variability

`
WITH base AS (
    SELECT
        date,
        -- Extract the real block identifier before the underscore
        (regexp_split_to_array(block_id, '_'))[1] AS block_key,
        block_id,
        trip_id,
        route_id,
		direction_id,
        firstlast,
        plannedduration,
        realduration,
        on_time_pct::int
    FROM rtl.tripsreport where date between '2025-11-01' and '2025-11-30'
	AND EXTRACT(DOW FROM date) IN (1,2,3,4,5)
),
ordered AS (
    SELECT
        date,
        block_key,
        block_id,
        trip_id,
        route_id,
		direction_id,
        -- Convert planned start to seconds
        firstlast[1]::int AS planned_start_sec,
        plannedduration::int AS planned_dur_sec,
        realduration::int AS real_dur_sec,
        on_time_pct::int,

        LEAD(trip_id) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_trip_id,
        LEAD(route_id) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_route_id,
		LEAD(direction_id) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_direction_id,
        LEAD(firstlast[1]::int) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_planned_start_sec,
        LEAD(plannedduration::int) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_planned_dur_sec,
        LEAD(on_time_pct) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_on_time_pct
    FROM base
),
pairs AS (
    SELECT
        date,
        block_key,
        block_id,

        -- current trip
        trip_id AS trip_a_id,
        route_id AS route_a,
		direction_id AS direction_a,
		
        on_time_pct AS on_time_a,
        planned_start_sec AS planned_start_a,
        planned_dur_sec AS planned_dur_a,

        -- next trip
        next_trip_id AS trip_b_id,
        next_route_id AS route_b,
		next_direction_id AS direction_b,
        next_on_time_pct AS on_time_b,
        next_planned_start_sec AS planned_start_b,
        next_planned_dur_sec AS planned_dur_b,

        -- layover in seconds
        CASE
            WHEN next_planned_start_sec IS NOT NULL THEN
                next_planned_start_sec - (planned_start_sec + planned_dur_sec)
        END AS planned_layover_sec,
		planned_start_sec,
		next_planned_start_sec
    FROM ordered
),
d AS (
SELECT *
FROM pairs
WHERE trip_b_id IS NOT NULL
ORDER BY date, block_key, planned_start_a
)
SELECT

	CASE WHEN on_time_b >= 0.85 THEN 1 ELSE 0 END AS y_on_time_b,
	CASE WHEN on_time_a >= 0.85 THEN 1 ELSE 0 END AS on_time_a,

    planned_dur_a,
    planned_dur_b,

    CASE WHEN planned_layover_sec>0 THEN planned_layover_sec
	ELSE 0
	END as planned_layover_sec,

	--engineered features
	--planned_start_sec/3600 as start_hour_a,
	--next_planned_start_sec/3660 as start_hour_b,

	CASE WHEN planned_start_sec>21600 AND planned_start_sec<=32400
	THEN 1 ELSE 0
	END as ampeak_a,

	CASE WHEN planned_start_sec>55800 AND planned_start_sec<=66600
	THEN 1 ELSE 0
	END as pmpeak_a,

    CONCAT(route_a, '_', direction_a, '_', route_b, '_', direction_b) AS route_pair
	
FROM d 
WHERE on_time_a IS NOT NULL and on_time_b IS NOT NULL
AND planned_layover_sec < 900

`



//With variability
`
WITH orig AS (
    SELECT
        date,
		service_id,
        -- Extract the real block identifier before the underscore
        (regexp_split_to_array(block_id, '_'))[1] AS block_key,
        block_id,
        trip_id,
        route_id,
		direction_id,
        firstlast,
        plannedduration,
        realduration,
        on_time_pct::int
    FROM rtl.tripsreport where date between '2025-10-01' and '2025-11-30'
	AND EXTRACT(DOW FROM date) IN (1,2,3,4,5) AND (service_id=4 OR service_id=8)
),
var as (
	SELECT
        date, trip_id, (realduration-plannedduration)::integer as var
    FROM rtl.tripsreport where date between '2025-10-01' and '2025-11-30'
	AND EXTRACT(DOW FROM date) IN (1,2,3,4,5) AND (service_id=4 OR service_id=8)
	AND (realduration-plannedduration) IS NOT NULL
),
tripvar as (
select trip_id, avg(var)::integer as avg_var
from var
group by trip_id
),
base as (
SELECT orig.*,
tripvar.avg_var
FROM orig
LEFT JOIN tripvar ON tripvar.trip_id = orig.trip_id
),
ordered AS (
    SELECT
        date,
        block_key,
        block_id,
        trip_id,
        route_id,
		direction_id,
		avg_var,
        -- Convert planned start to seconds
        firstlast[1]::int AS planned_start_sec,
        plannedduration::int AS planned_dur_sec,
        realduration::int AS real_dur_sec,
        on_time_pct::int,

        LEAD(base.trip_id) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_trip_id,
        LEAD(route_id) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_route_id,
		LEAD(direction_id) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_direction_id,
        LEAD(firstlast[1]::int) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_planned_start_sec,
        LEAD(plannedduration::int) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_planned_dur_sec,
        LEAD(on_time_pct) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_on_time_pct,
		LEAD(avg_var) OVER (PARTITION BY date, block_key ORDER BY firstlast[1]::int) AS next_avg_var
    FROM base
),
pairs AS (
    SELECT
        date,
        block_key,
        block_id,

        -- current trip
        trip_id AS trip_a_id,
        route_id AS route_a,
		direction_id AS direction_a,
		
        on_time_pct AS on_time_a,
        planned_start_sec AS planned_start_a,
        planned_dur_sec AS planned_dur_a,
		avg_var as avg_var_a,

        -- next trip
        next_trip_id AS trip_b_id,
        next_route_id AS route_b,
		next_direction_id AS direction_b,
        next_on_time_pct AS on_time_b,
        next_planned_start_sec AS planned_start_b,
        next_planned_dur_sec AS planned_dur_b,
		next_avg_var AS avg_var_b,

        -- layover in seconds
        CASE
            WHEN next_planned_start_sec IS NOT NULL THEN
                next_planned_start_sec - (planned_start_sec + planned_dur_sec)
        END AS planned_layover_sec,
		planned_start_sec,
		next_planned_start_sec
    FROM ordered
),
d AS (
SELECT *
FROM pairs
WHERE trip_b_id IS NOT NULL
ORDER BY date, block_key, planned_start_a
)
SELECT

	CASE WHEN on_time_b >= 0.85 THEN 1 ELSE 0 END AS y_on_time_b,
	CASE WHEN on_time_a >= 0.85 THEN 1 ELSE 0 END AS on_time_a,

    planned_dur_a,
    planned_dur_b,

	avg_var_a,
	avg_var_b,

    CASE WHEN planned_layover_sec>0 THEN planned_layover_sec
	ELSE 0
	END as planned_layover_sec,

	--engineered features
	--planned_start_sec/3600 as start_hour_a,
	--next_planned_start_sec/3660 as start_hour_b,

	CASE WHEN planned_start_sec>21600 AND planned_start_sec<=32400
	THEN 1 ELSE 0
	END as ampeak_a,

	CASE WHEN planned_start_sec>55800 AND planned_start_sec<=66600
	THEN 1 ELSE 0
	END as pmpeak_a,

    CONCAT(route_a, '_', direction_a, '_', route_b, '_', direction_b) AS route_pair
	
FROM d 
WHERE on_time_a IS NOT NULL and on_time_b IS NOT NULL 
AND avg_var_a IS NOT NULL AND avg_var_b IS NOT NULL
AND planned_layover_sec < 900
`