{{ config(materialized='table') }}

select
    o_orderkey,
    case
        when uniform(0, 1, random()) < 0.5 then 'Venta' else 'Devolución'
    end as tipo_operacion
from {{ ref('orders') }}