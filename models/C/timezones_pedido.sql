-- models/orders_with_localtime_and_nation.sql

WITH timezone_data AS (
    SELECT
        STORE_ID,
        CUSTOMER_ID,
        STORE_TIMEZONE,
        CUSTOMER_TIMEZONE
    FROM {{ ref('timezone') }}
),

orders_with_timezones AS (
    SELECT
        o.*,
        t.STORE_TIMEZONE,
        t.CUSTOMER_TIMEZONE
    FROM {{ ref('orders') }} o
    LEFT JOIN timezone_data t
    ON o.STORE_ID = t.STORE_ID
    AND o.CUSTOMER_ID = t.CUSTOMER_ID
),

orders_with_localtime AS (
    SELECT
        *,
        CONVERT_TIMEZONE('UTC', STORE_TIMEZONE, O_ORDERDATE) AS O_STORE_LOCALTIME,
        CONVERT_TIMEZONE('UTC', CUSTOMER_TIMEZONE, O_ORDERDATE) AS O_CUSTOMER_LOCALTIME
    FROM orders_with_timezones
),

orders_with_nation AS (
    SELECT
        o.*,
        n.N_NAME AS NATION_NAME
    FROM orders_with_localtime o
    LEFT JOIN {{ ref('nation') }} n
    ON o.NATIONKEY = n.N_NATIONKEY
)

SELECT
    *
FROM orders_with_nation