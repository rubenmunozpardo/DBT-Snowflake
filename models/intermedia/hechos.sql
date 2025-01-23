{{ config(materialized='table') }}

select
    t.o_orderkey,
    t.id_evento,
    pt.tienda,
    pt.pais,
    ml.tasa_cambio,
    ml.total as total_moneda_local,
    hl.hora_local,
    pe.total_unidades,
    pe.total_importe,
    pe.clerk_id,
    pe.fecha_recibido,
    tp.tipo_operacion
from {{ ref('evento') }} t
join {{ ref('pais_tienda') }} pt on t.o_orderkey = pt.o_orderkey
join {{ ref('moneda_local') }} ml on t.o_orderkey = ml.o_orderkey
join {{ ref('hora_local') }} hl on t.o_orderkey = hl.o_orderkey
join {{ ref('plazo_entrega') }} pe on t.o_orderkey = pe.o_orderkey
join {{ ref('tipo_operacion') }} tp on t.o_orderkey = tp.o_orderkey