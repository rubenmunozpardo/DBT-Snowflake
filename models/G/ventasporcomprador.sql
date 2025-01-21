
WITH ventasporcomprador AS (
    SELECT 
           c.c_name,
           SUM(o.o_totalprice) AS total_spent
    FROM {{ ref('customer') }} c
    JOIN {{ ref('orders') }} o ON c.c_custkey = o.o_orderkey
    GROUP BY  c.c_name
)

SELECT * FROM ventasporcomprador

/*

EXPLAIN
WITH ventasporcomprador AS (
    SELECT 
           c.c_name,
           SUM(o.o_totalprice) AS total_spent
    FROM {{ ref('customer') }} c
    JOIN {{ ref('orders') }} o ON c.c_custkey = o.o_orderkey
    GROUP BY  c.c_name
)

SELECT * FROM ventasporcomprador

*/