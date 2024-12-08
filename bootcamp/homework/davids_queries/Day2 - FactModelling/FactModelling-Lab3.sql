
-- DELETE FROM array_metrics;
-- SELECT * FROM array_metrics;
CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY (user_id, month_start, metric_name)
);

INSERT INTO array_metrics
WITH daily_aggergate as (
    SELECT
        user_id
        , event_time::date as date
        , COUNT(1) as num_site_hits
    FROM events
        WHERE event_time::DATE = '2023-01-05'::DATE
            AND user_id is not null
    GROUP BY user_id, event_time::date
)
, yesterday_array as (
    SELECT *
    FROM array_metrics da
        where month_start='2023-01-01'::DATE
)
SELECT
    COALESCE(da.user_id, ya.user_id) as user_id
    , COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) as month_start
    , 'site_hits' as metric_name
    , CASE WHEN ya.metric_array IS NOT NULL THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
--         WHEN ya.month_start is NULL THEN ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.metric_array is NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE_TRUNC('month', date)::date, 0)]) || ARRAY [COALESCE(da.num_site_hits, 0)]
    END as metric_array
FROM daily_aggergate da
    FULL OUTER JOIN yesterday_array ya
        ON da.user_id=ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO
    UPDATE SET metric_array = EXCLUDED.metric_array;
;

SELECT *
FROM array_metrics;

-- each should have number of days that passed
SELECT cardinality(metric_array)
    , COUNT(1)
FROM array_metrics
GROUP BY metric_array;


-- explode out array again
WITH agg as (
    SELECT metric_name
        , month_start
        , ARRAY[SUM(metric_array[1])
                , SUM(metric_array[2])
                , SUM(metric_array[3])
                , SUM(metric_array[4])
                , SUM(metric_array[5])] as summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
SELECT metric_name
    , month_start + CAST(CAST(index - 1 AS TEXT) || 'day' as interval)
    , elem as value
FROM agg
    CROSS JOIN UNNEST(agg.summed_array)
        WITH ordinality as a(elem, index);
