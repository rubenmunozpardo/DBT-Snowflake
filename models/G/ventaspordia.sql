
WITH ventaspordia AS (
    SELECT o_orderdate,
           SUM(o_totalprice) AS total_sales
    FROM {{ ref('orders') }}
    GROUP BY o_orderdate
)

SELECT * FROM ventaspordia

/* revisar

EXPLAIN
WITH ventaspordia AS (
    SELECT o_orderdate,
           SUM(o_totalprice) AS total_sales
    FROM {{ ref('orders') }}
    GROUP BY o_orderdate
)

SELECT * FROM ventaspordia

*/