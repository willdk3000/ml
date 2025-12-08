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
        firstlast[1] as planstart,
        plannedduration,
        realduration,
        on_time_pct::int
    FROM rtl.tripsreport where date between '2025-10-01' and '2025-11-30'
	AND EXTRACT(DOW FROM date) IN (1,2,3,4,5) AND (service_id=4 OR service_id=8)
),
var1 as (
	SELECT
    date, route_id, direction_id, firstlast[1] as planstart, realduration,
	(realduration-plannedduration)::integer as var
    FROM rtl.tripsreport where date between '2025-10-01' and '2025-11-30'
	AND EXTRACT(DOW FROM date) IN (1,2,3,4,5) AND (service_id=4 OR service_id=8)
	AND (realduration-plannedduration) IS NOT NULL
),
tripvar as (
	select route_id, direction_id, planstart, 
	avg(var)::integer as avg_var,
	percentile_cont(0.85) WITHIN GROUP (ORDER BY realduration) AS p85_realduration,
	percentile_cont(0.75) WITHIN GROUP (ORDER BY realduration) AS p75_realduration,
	percentile_cont(0.25) WITHIN GROUP (ORDER BY realduration) AS p25_realduration
	from var1
	group by route_id, direction_id, planstart
),
base as (
	SELECT orig.*,
	tripvar.avg_var,
	tripvar.p85_realduration,
	(p75_realduration-p25_realduration) as range7525
	FROM orig
	LEFT JOIN tripvar ON tripvar.route_id = orig.route_id 
	AND tripvar.direction_id=orig.direction_id 
	AND tripvar.planstart = orig.planstart
),
ordered AS (
    SELECT
        date,
        block_key,
        block_id,
        route_id,
		direction_id,
		planstart,
		avg_var,
		p85_realduration,
		range7525,
        -- Convert planned start to seconds
        --firstlast[1]::int AS planned_start_sec,
        plannedduration::int AS planned_dur_sec,
        realduration::int AS real_dur_sec,
        on_time_pct::int,

        LEAD(route_id) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_route_id,
		LEAD(direction_id) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_direction_id,
        LEAD(planstart::int) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_planned_start_sec,
        LEAD(plannedduration::int) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_planned_dur_sec,
        LEAD(on_time_pct) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_on_time_pct,
		LEAD(planstart) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_planstart,
		LEAD(avg_var) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_avg_var,
		LEAD(p85_realduration) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_p85_realduration,
		LEAD(range7525) OVER (PARTITION BY date, block_key ORDER BY planstart::int) AS next_range7525
    FROM base
),
pairs AS (
    SELECT
        date,
        block_key,
        block_id,

        -- current trip
        --trip_id AS trip_a_id,
        route_id AS route_a,
		direction_id AS direction_a,
		planstart::integer AS planstart_a,
		
        on_time_pct AS on_time_a,
        --planned_start_sec AS planned_start_a,
        planned_dur_sec AS planned_dur_a,
		avg_var as avg_var_a,
		p85_realduration as p85_realduration_a,
		range7525 as range7525_a,

        -- next trip
        --next_trip_id AS trip_b_id,
        next_route_id AS route_b,
		next_direction_id AS direction_b,
        next_on_time_pct AS on_time_b,
        next_planstart::integer AS planstart_b,
        next_planned_dur_sec AS planned_dur_b,
		next_avg_var AS avg_var_b,
		next_p85_realduration AS p85_realduration_b,
		next_range7525 AS range7525_b,

        -- layover in seconds
        CASE
            WHEN next_planned_start_sec IS NOT NULL THEN
                next_planned_start_sec - (planstart::integer + planned_dur_sec)
        END AS planned_layover_sec,
		planstart,
		next_planned_start_sec
    FROM ordered
),
d AS (
SELECT *
	FROM pairs
	ORDER BY date, block_key, planstart
)
SELECT

	CASE WHEN on_time_b >= 0.85 THEN 1 ELSE 0 END AS y_on_time_b,
	CASE WHEN on_time_a >= 0.85 THEN 1 ELSE 0 END AS on_time_a,

    planned_dur_a,
    planned_dur_b,

	--planstart_a,
	--planstart_b,

	--avg_var_a,
	--avg_var_b,

	--p85_realduration_a,
	p85_realduration_b,
	p85_realduration_b/planned_dur_b as p85_pct_b,

	--range7525_a,
	range7525_b,

    CASE WHEN planned_layover_sec>0 THEN planned_layover_sec
	ELSE 0
	END as planned_layover_sec,

	--engineered features
	--planstart_a/3600 as start_hour_a,
	--planstart_b/3660 as start_hour_b,

	CASE WHEN planstart_a>21600 AND planstart_a<=32400
	THEN 1 ELSE 0
	END as ampeak_a,

	CASE WHEN planstart_a>55800 AND planstart_a<=66600
	THEN 1 ELSE 0
	END as pmpeak_a,

    CONCAT(route_a, '_', direction_a, '_', route_b, '_', direction_b) AS route_pair
	
FROM d 
WHERE on_time_a IS NOT NULL and on_time_b IS NOT NULL 
AND avg_var_a IS NOT NULL AND avg_var_b IS NOT NULL
AND planned_layover_sec < 900
`