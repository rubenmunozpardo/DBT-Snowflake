-- models/order_country_utc.sql

WITH orders AS (
    SELECT 
        o.O_ORDERKEY, 
        o.O_PURCHASEDATETIME, 
        n.N_NAME AS COUNTRY, 
        t.TIMEZONE AS ORDER_TIMEZONE, 
        t.UTC_OFFSET
    FROM {{ ref('ORDERS') }} o
    JOIN {{ ref('CUSTOMER') }} c ON o.O_CUSTKEY = c.C_CUSTKEY
    JOIN {{ ref('NATION') }} n ON c.C_NATIONKEY = n.N_NATIONKEY
    JOIN {{ ref('TIMEZONEs') }} t ON n.N_NATIONKEY = t.N_NATIONKEY
)

SELECT * FROM orders;