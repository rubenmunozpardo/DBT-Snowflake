-- models/vista_cliente.sql

{{ config(
    materialized='view'
) }}

SELECT 
    C.C_CUSTKEY AS CLIENTE_ID,
    SUM(L.L_EXTENDEDPRICE * M.TASA_CAMBIO) AS TOTAL_GASTO_MONEDA_LOCAL,
    COUNT(E.ID_EVENTO) AS NUMERO_EVENTOS
FROM 
    {{ ref('CUSTOMER') }} C
JOIN 
    {{ ref('LINEITEM') }} L ON C.C_CUSTKEY = L.L_CUSTKEY
JOIN 
    {{ ref('MONEDA_LOCAL') }} M ON L.L_ORDERKEY = M.O_ORDERKEY
LEFT JOIN 
    {{ ref('EVENTO') }} E ON L.L_ORDERKEY = E.O_ORDERKEY
GROUP BY 
    C.C_CUSTKEY