
{{ config(
    materialized='incremental',
    unique_key='TIENDA'
) }}

SELECT 
    TIENDA, 
    SUM(TOTAL_MONEDA_LOCAL) AS TOTAL_VENTAS
FROM 
    {{ ref('parteC') }}
GROUP BY 
    TIENDA