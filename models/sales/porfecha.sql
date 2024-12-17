WITH limpiar AS (
    SELECT
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERSTATUS,
        O_TOTALPRICE,
        O_ORDERDATE,
        O_ORDERPRIORITY,
        O_CLERK,
        O_SHIPPRIORITY,
        O_COMMENT,
        CAST(REGEXP_SUBSTR(O_CLERK, '[0-9]+') AS INTEGER) AS clerk_id
    FROM {{ ref('orders') }}
    WHERE O_ORDERSTATUS IN ('F', 'O')
)
SELECT
    c.C_CUSTKEY,
    c.C_NAME,
    p.P_PARTKEY,
    p.P_NAME,
    l.L_QUANTITY,
    l.L_EXTENDEDPRICE,
    limpiar.O_ORDERDATE,
    limpiar.clerk_id
FROM
    limpiar
JOIN
    {{ ref('lineitem') }} l ON limpiar.O_ORDERKEY = l.L_ORDERKEY
JOIN
    {{ ref('customer') }} c ON limpiar.O_CUSTKEY = c.C_CUSTKEY
JOIN
    {{ ref('part') }} p ON l.L_PARTKEY = p.P_PARTKEY