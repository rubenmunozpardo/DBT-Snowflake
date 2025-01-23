{{ config(materialized='table') }}

select 'America/New_York' as pais, 1.09 as tasa_cambio
union all
select 'Asia/Tehran' as pais, 20.0 as tasa_cambio
union all
select 'America/Toronto' as pais, 1.3 as tasa_cambio