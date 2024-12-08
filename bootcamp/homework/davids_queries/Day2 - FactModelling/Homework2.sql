--DAY 2 HW

-- The homework this week will be using the devices and events dataset
-- Construct the following eight queries:
-- 1) A query to deduplicate game_details from Day 1 so there's no duplicates
WITH dedup as (
    SELECT *
        , row_number() over (PARTITION BY game_id, team_id, player_id) as row_num
    FROM game_details
)
SELECT *
FROM dedup
    WHERE row_num=1;

-- A DDL for an user_devices_cumulated table that has:
-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)

SELECT * FROM devices;
SELECT * FROM events;
SELECT * FROM users;

-- Design:
    -- user_id | {browser_type_1, {date_active}, browser_type_2, {date_active}}
    -- where date_active=date where user had 1 website hit with a certain browser
DROP TYPE device_activity_datelist;
CREATE TYPE device_activity_datelist AS (
    browser_type TEXT,
    days_active DATE[]
);
DROP TABLE user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    date DATE,
    device_activity_datelist device_activity_datelist[]
);
SELECT * FROM user_devices_cumulated
     WHERE user_id = 13580626093054200000
;
INSERT INTO user_devices_cumulated
WITH daily_agg AS (
    SELECT e.user_id
        , browser_type
        , DATE_TRUNC('DAY', event_time::date)::Date as date_active
    FROM events e
        JOIN devices d ON e.device_id=d.device_id
            WHERE user_id is not null
              AND user_id = 13580626093054200000
                AND event_time::date='2023-01-02'
    GROUP BY 1,2,3
)
, yesterday_agg as (
    SELECT *
    FROM user_devices_cumulated
        where date::date='2023-01-01'
            AND user_id = 13580626093054200000
)
-- , combined_agg AS (
    SELECT
        COALESCE(da.user_id, ya.user_id) AS user_id
        , COALESCE(da.date_active, ya.date + INTERVAL '1 DAY')::date as date -- COALESCE(da.date, ya.date) AS date,
        , CASE
            WHEN ya.device_activity_datelist is NULL THEN ARRAY_AGG(ROW(da.browser_type, ARRAY[da.date_active])::device_activity_datelist) --json_build_object('browser_type', browser_type,'days_active', da.date_active)--
            WHEN da.date_active is NULL THEN ya.device_activity_datelist
            ELSE ARRAY_AGG(ROW(da.browser_type, ARRAY[da.date_active])::device_activity_datelist || ya.device_activity_datelist)
        END as dates_active
    FROM daily_agg da
    FULL OUTER JOIN yesterday_agg ya
        ON da.user_id = ya.user_id
    WHERE COALESCE(da.user_id, ya.user_id) = 13580626093054200000 --70132547320211180
    GROUP BY COALESCE(da.user_id, ya.user_id), COALESCE(da.date_active, ya.date + INTERVAL '1 DAY'), ya.device_activity_datelist, da.date_active;

-- A DDL for hosts_cumulated table a host_activity_datelist which logs to see which dates each host is experiencing any activity

DROP table hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT,
    date DATE,
    host_activity_datelist DATE[]
);
INSERT INTO hosts_cumulated
WITH daily_agg as (
    SELECT
        host
        , DATE_TRUNC('day', event_time::DATE)::DATE as host_activity
    FROM events
        WHERE DATE_TRUNC('day', event_time::DATE)::DATE = '2023-01-04'
)
, yesterday_agg as (
    SELECT *
    FROM hosts_cumulated
        WHERE date='2023-01-03'
)
SELECT
    COALESCE(da.host, ya.host) as host
    , COALESCE(da.host_activity, ya.date + INTERVAL '1 DAY')::date as date
    , CASE
        WHEN ya.date is NULL THEN ARRAY[da.host_activity]
        WHEN da.host_activity is NULL THEN ya.host_activity_datelist
        ELSE ARRAY[da.host_activity] || ya.host_activity_datelist
    END as host_activity_datelist
FROM daily_agg da
    FULL OUTER JOIN yesterday_agg ya ON ya.host=da.host
GROUP BY 1,2, ya.date, da.host_activity, host_activity_datelist;


SELECT * FROM hosts_cumulated;
