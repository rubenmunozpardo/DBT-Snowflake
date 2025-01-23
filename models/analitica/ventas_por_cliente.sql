
{{ config(
    materialized='incremental',
    unique_key='CLERK_ID'
) }}

SELECT 
    CLERK_ID, 
    SUM(TOTAL_MONEDA_LOCAL) AS TOTAL_VENTAS
FROM 
    {{ ref('hechos') }}
GROUP BY 
    CLERK_ID