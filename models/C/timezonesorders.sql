WITH orders AS (
    SELECT 
        o.O_ORDERKEY, 
        n.N_NAME AS COUNTRY, 
        t.TIMEZONE AS ORDER_TIMEZONE, 
        DATEADD('second', 
                (CAST(SUBSTR(t.UTC_OFFSET, 2, 2) AS INT) * 3600) + 
                (CAST(SUBSTR(t.UTC_OFFSET, 5, 2) AS INT) * 60) * 
                CASE WHEN SUBSTR(t.UTC_OFFSET, 1, 1) = '+' THEN 1 ELSE -1 END, 
                TO_TIMESTAMP(o.O_PURCHASEDATETIME)) AS FORMATTED_ORDER_TIME
    FROM {{ ref('orders') }} o
    JOIN {{ ref('customer') }} c ON o.O_CUSTKEY = c.C_CUSTKEY
    JOIN {{ ref('nation') }} n ON c.C_NATIONKEY = n.N_NATIONKEY
    JOIN {{ ref('timezones') }} t ON n.N_NAME = t.N_NAME
)

SELECT 
    O_ORDERKEY, 
    COUNTRY, 
    ORDER_TIMEZONE, 
    FORMATTED_ORDER_TIME 
FROM orders