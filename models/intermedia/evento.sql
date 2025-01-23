{{ config(materialized='table') }}

select
    o_orderkey,
    o_orderdate,
    case
        when to_varchar(o_orderdate, 'MM-DD') between '11-01' and '11-30'
        then 'Black Friday'
        when to_varchar(o_orderdate, 'MM-DD') between '12-01' and '12-31'
        then 'Campaña Navidad'
        else 'Sin Evento'
    end as id_evento
from {{ ref('orders') }}