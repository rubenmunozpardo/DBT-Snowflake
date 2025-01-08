-- models/vista_tienda.sql
{{ config(materialized="view") }}

with
    ventas_moneda_local as (
        select l.l_suppkey, l.l_extendedprice * m.tasa_cambio as venta_moneda_local
        from {{ ref("lineitem") }} l
        join {{ ref("moneda_local") }} m on l.l_orderkey = m.o_orderkey
    ),
    eventos_por_tienda as (
        select l.l_suppkey, count(e.id_evento) as numero_eventos
        from {{ ref("lineitem") }} l
        left join {{ ref("evento") }} e on l.l_orderkey = e.o_orderkey
        group by l.l_suppkey
    )

select
    v.l_suppkey as tienda_id,
    sum(v.venta_moneda_local) as total_ventas_moneda_local,
    e.numero_eventos
from ventas_moneda_local v
join eventos_por_tienda e on v.l_suppkey = e.l_suppkey
group by v.l_suppkey, e.numero_eventos

