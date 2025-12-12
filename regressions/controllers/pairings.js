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




//V3 LSTM
`
WITH orig AS (
    SELECT
        date,
		service_id,
        -- Extract the real block identifier before the underscore
        (regexp_split_to_array(block_id, '_'))[1] AS block_key,
		(regexp_split_to_array(block_id, '_'))[2] AS tripcode,
		ROW_NUMBER() OVER (PARTITION BY date, (regexp_split_to_array(block_id, '_'))[1] ORDER BY firstlast[1]) AS trip,
        block_id,
        trip_id,
        route_id,
		direction_id,
		--timestamp_to_seconds(realstarttime) as realstart,
        firstlast[1]::integer as planstart,
        plannedduration,
        realduration,
        on_time_pct::int
    FROM rtl.tripsreport where date between '2025-10-01' and '2025-11-30'
	AND EXTRACT(DOW FROM date) IN (1,2,3,4,5) AND (service_id=4 OR service_id=8)
),
var1 as (
	SELECT
    date, route_id, direction_id, firstlast[1]::integer as planstart, realduration, 
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
raw as (
	SELECT 
	--orig.*,
	orig.date,
	orig.block_key,
	orig.trip::integer,
	orig.planstart,
	orig.plannedduration,
	--orig.realstart,
	orig.realduration,
	orig.on_time_pct,
	orig.route_id,
	orig.direction_id,
	--concat(orig.route_id, '_', orig.direction_id) as routedir,
	--tripvar.avg_var,
	--tripvar.p85_realduration as p85_realduration,
	(tripvar.p85_realduration/orig.plannedduration) as p85_pct,
	(p75_realduration-p25_realduration) as range7525,
	CASE WHEN on_time_pct >= 85 THEN 1 ELSE 0 END AS on_time_class,
	
	CASE WHEN orig.planstart::integer>21600 AND orig.planstart::integer<=32400
		THEN 1 ELSE 0
	END as ampeak,

	CASE WHEN orig.planstart::integer>55800 AND orig.planstart::integer<=66600
	THEN 1 ELSE 0
	END as pmpeak
	
	FROM orig
	LEFT JOIN tripvar ON tripvar.route_id = orig.route_id 
	AND tripvar.direction_id=orig.direction_id 
	AND tripvar.planstart = orig.planstart
	WHERE realduration IS NOT NULL
),
numbered as (
    SELECT
        date,
        block_key,
        trip,
        planstart,
        plannedduration,
        realduration,
        p85_pct,
        range7525,
        on_time_class,
        ampeak,
        pmpeak,
		route_id,
		direction_id,
        --routedir,

        ROW_NUMBER() OVER (
            PARTITION BY date, block_key
            ORDER BY trip
        ) AS rn
    FROM raw
),

grouped AS (
    SELECT
        *,
        -- group_index: 1 for trips 1–5, 2 for trips 6–10, etc.
        CEIL(rn / 5.0) AS group_index
    FROM numbered
),

max_trip AS (
    SELECT
        date,
        block_key,
        group_index,
        MAX(rn) - ( (group_index - 1) * 5 ) AS trips_in_group
    FROM grouped
    GROUP BY date, block_key, group_index
),

all_combinations AS (
    SELECT
        m.date,
        m.block_key,
        m.group_index,
        gs.trip_in_group
    FROM max_trip m
    CROSS JOIN LATERAL generate_series(1, 5) AS gs(trip_in_group)  -- always 5 slots
),
res AS (
SELECT
    ac.date,
    ac.block_key,
    ac.group_index AS new_block_key,
    ac.trip_in_group AS trip_within_group,

    COALESCE(g.planstart, 0) AS planstart,
    COALESCE(g.plannedduration, 0) AS plannedduration,
    COALESCE(g.p85_pct, 0) AS p85_pct,
    COALESCE(g.realduration, 0) AS realduration,
    COALESCE(g.on_time_class, 0) AS on_time_pct,
    COALESCE(g.range7525, 0) AS range7525,
    COALESCE(g.on_time_class, 0) AS on_time_class,
    COALESCE(g.ampeak, 0) AS ampeak,
    COALESCE(g.pmpeak, 0) AS pmpeak,
	COALESCE(g.route_id::integer, 0) as route_id,
	COALESCE(g.direction_id, 0) as direction_id,
	COALESCE(LAG(planstart+plannedduration) OVER (PARTITION BY ac.date, ac.block_key ORDER BY planstart),0) AS prev_plan_arrival

    --COALESCE(NULLIF(g.routedir, ''), '0') AS routedir

FROM all_combinations ac
LEFT JOIN grouped g
    ON ac.date = g.date
    AND ac.block_key = g.block_key
    AND ac.group_index = g.group_index
    AND ac.trip_in_group = (g.rn - ( (g.group_index - 1) * 5 ))

ORDER BY ac.date, ac.block_key, ac.group_index, ac.trip_in_group
)
SELECT res.*,
CASE
	WHEN planstart-prev_plan_arrival > 900 OR planstart-prev_plan_arrival< 0 THEN 0
	ELSE planstart-prev_plan_arrival
END as planned_layover
FROM res

`