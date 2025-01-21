WITH evento AS (
    SELECT
        o_orderkey,
        o_orderdate,
        CASE
            WHEN to_varchar(o_orderdate, 'MM-DD') BETWEEN '11-01' AND '11-30'
            THEN 'Black Friday'
            WHEN to_varchar(o_orderdate, 'MM-DD') BETWEEN '12-01' AND '12-31'
            THEN 'Campaña Navidad'
            ELSE 'Sin Evento'
        END AS id_evento
    FROM dbt_project.orders
),
pais_tienda AS (
    SELECT
        o.o_orderkey,
        o.o_orderdate,
        CASE
            WHEN uniform(0, 1, random()) < 0.33
            THEN 'Tienda A'
            WHEN uniform(0, 1, random()) < 0.66
            THEN 'Tienda B'
            ELSE 'Tienda C'
        END AS tienda,
        CASE
            WHEN uniform(0, 1, random()) < 0.33
            THEN 'America/New_York'
            WHEN uniform(0, 1, random()) < 0.66
            THEN 'Asia/Tehran'
            ELSE 'America/Toronto'
        END AS pais
    FROM dbt_project.orders o
),
cambio_moneda AS (
    SELECT 'America/New_York' AS pais, 1.09 AS tasa_cambio
    UNION ALL
    SELECT 'Asia/Tehran' AS pais, 20.0 AS tasa_cambio
    UNION ALL
    SELECT 'America/Toronto' AS pais, 1.3 AS tasa_cambio
),
moneda_local AS (
    SELECT DISTINCT
        o.o_orderkey,
        cm.pais,
        cm.tasa_cambio,
        o.o_totalprice,
        o.o_totalprice * cm.tasa_cambio AS total,
        n.n_name AS pais_cliente
    FROM cambio_moneda AS cm
    JOIN dbt_project.orders AS o ON o.o_orderkey = o.o_orderkey
    JOIN dbt_project.lineitem AS l ON l.l_orderkey = o.o_orderkey
    JOIN dbt_project.nation AS n ON n.n_nationkey = o.o_custkey
),
hora_local AS (
    SELECT 
        o.O_ORDERKEY, 
        n.N_NAME AS pais, 
        t.TIMEZONE AS ORDER_TIMEZONE, 
        DATEADD('second', 
                (CAST(SUBSTR(t.UTC_OFFSET, 2, 2) AS INT) * 3600) + 
                (CAST(SUBSTR(t.UTC_OFFSET, 5, 2) AS INT) * 60) * 
                CASE WHEN SUBSTR(t.UTC_OFFSET, 1, 1) = '+' THEN 1 ELSE -1 END, 
                TO_TIMESTAMP(o.O_ORDERDATE)) AS hora_local
    FROM dbt_project.orders o
    JOIN dbt_project.customer c ON o.O_CUSTKEY = c.C_CUSTKEY
    JOIN dbt_project.nation n ON c.C_NATIONKEY = n.N_NATIONKEY
    JOIN dbt_project.timezones t ON n.N_NAME = t.N_NAME
),
plazo_entrega AS (
    SELECT
        o.o_orderkey,
        o.o_orderdate,
        o.o_custkey,
        l.l_partkey,
        SUM(l.l_quantity) AS total_unidades,
        SUM(l.l_extendedprice) AS total_importe,
        CAST(REGEXP_SUBSTR(o.o_clerk, '[0-9]+') AS INTEGER) AS clerk_id,
        l.l_receiptdate AS fecha_recibido
    FROM dbt_project.orders o
    JOIN dbt_project.lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE o_orderstatus IN ('F', 'O')
    GROUP BY o.o_orderkey, o.o_orderdate, o.o_custkey, l.l_partkey, o.o_clerk, l.l_receiptdate
    ORDER BY o.o_orderdate
),
tipo_operacion AS (
    SELECT
        o_orderkey,
        CASE
            WHEN uniform(0, 1, random()) < 0.5 THEN 'Venta' ELSE 'Devolución'
        END AS tipo_operacion
    FROM dbt_project.orders
)
SELECT
    t.o_orderkey,
    t.id_evento,
    pt.tienda,
    pt.pais,
    ml.tasa_cambio,
    ml.total AS total_moneda_local,
    hl.hora_local,
    pe.total_unidades,
    pe.total_importe,
    pe.clerk_id,
    pe.fecha_recibido,
    tp.tipo_operacion
FROM evento t
JOIN pais_tienda pt ON t.o_orderkey = pt.o_orderkey
JOIN moneda_local ml ON t.o_orderkey = ml.o_orderkey
JOIN hora_local hl ON t.o_orderkey = hl.o_orderkey
JOIN plazo_entrega pe ON t.o_orderkey = pe.o_orderkey
JOIN tipo_operacion tp ON t.o_orderkey = tp.o_orderkey