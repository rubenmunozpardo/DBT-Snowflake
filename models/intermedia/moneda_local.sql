{{ config(materialized='table') }}

select distinct
    o.o_orderkey,
    cm.pais,
    cm.tasa_cambio,
    o.o_totalprice,
    o.o_totalprice * cm.tasa_cambio as total,
    n.n_name as pais_cliente
from {{ ref('cambio_moneda') }} as cm
join {{ ref('orders') }} as o on o.o_orderkey = o.o_orderkey
join {{ ref('lineitem') }} as l on l.l_orderkey = o.o_orderkey
join {{ ref('nation') }} as n on n.n_nationkey = o.o_custkey