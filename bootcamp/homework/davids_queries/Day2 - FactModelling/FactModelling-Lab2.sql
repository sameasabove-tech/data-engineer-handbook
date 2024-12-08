
SELECT * FROM events;

-- have to think about what events make a user active
INSERT INTO users_cumulated
WITH yesterday as (
    SELECT *
    FROM users_cumulated
        WHERE date=DATE('2023-01-30')
)
, today as (
    SELECT
        CAST(user_id as TEXT) as user_id
        , DATE(CAST(event_time as timestamp)) as date_active
    FROM events
        WHERE CAST(event_time as TIMESTAMP)::DATE = ('2023-01-31')::DATE
            AND user_id is not NULL
    GROUP BY user_id, DATE(CAST(event_time as TIMESTAMP))
)
SELECT COALESCE(t.user_id,y.user_id) as user_id
    , CASE
        WHEN y.dates_active is NULL THEN ARRAY[t.date_active]
        WHEN t.date_active is NULL THEN y.dates_active
        ELSE ARRAY [t.date_active] || y.dates_active
    END as dates_active
    , COALESCE(t.date_active, y.date + INTERVAL '1 DAY') as date
FROM today t
    FULL OUTER JOIN yesterday y
       ON y.user_id=t.user_id;

DROP TABLE users_cumulated;
CREATE TABLE users_cumulated (
    user_id TEXT,
    dates_active DATE[], -- last of dates in the past where the user was active
    date DATE, --current date for the user
    PRIMARY KEY (user_id, DATE)
);

SELECT * FROM users_cumulated
    WHERE date='2023-01-18';


-- SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 DAY');

WITH users as (
    SELECT *
    FROM users_cumulated
        WHERE date='2023-01-31'::DATE
)
, series as (
    SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 DAY') as series_date
)
, placeholder_int_value as (
    SELECT
    --     date - series_date::DATE
    --     dates_active @> ARRAY[series_date::DATE]
        --CAST(
        CASE WHEN dates_active @> ARRAY[series_date::DATE]
            THEN CAST(POW(2, 32 - (date - series_date::DATE)) AS BIGINT)
            ELSE 0
        END
            --as BIT(32))
        as placeholder_int_value
        , *
    FROM users
        CROSS JOIN series
--     WHERE user_id='137925124111668560'
)

-- SELECT user_id
-- --      , placeholder_int_value
--     , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
-- FROM placeholder_int_value
-- GROUP BY user_id; --, placeholder_int_value;

SELECT
    user_id
    , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
    , BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_monthly_active
    , BIT_COUNT(CAST('11111110000000000000010000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_weekly_active
    , BIT_COUNT(CAST('10000000000000000000010000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_daily_active
FROM placeholder_int_value
GROUP BY user_id;
