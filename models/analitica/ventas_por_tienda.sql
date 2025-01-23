
{{ config(
    materialized='incremental',
    unique_key='TIENDA'
) }}

SELECT 
    TIENDA, 
    SUM(TOTAL_MONEDA_LOCAL) AS TOTAL_VENTAS
FROM 
    {{ ref('hechos') }}
GROUP BY 
    TIENDA