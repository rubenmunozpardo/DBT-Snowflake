-- models/vista_tienda.sql

{{ config(
    materialized='view'
) }}

SELECT 
    L.L_SUPPKEY AS TIENDA_ID,
    SUM(L.L_EXTENDEDPRICE * M.TASA_CAMBIO) AS TOTAL_VENTAS_MONEDA_LOCAL,
    COUNT(E.ID_EVENTO) AS NUMERO_EVENTOS
FROM 
    {{ ref('LINEITEM') }} L
JOIN 
    {{ ref('MONEDA_LOCAL') }} M ON L.L_ORDERKEY = M.O_ORDERKEY
LEFT JOIN 
    {{ ref('EVENTO') }} E ON L.L_ORDERKEY = E.O_ORDERKEY
GROUP BY 
    L.L_SUPPKEY