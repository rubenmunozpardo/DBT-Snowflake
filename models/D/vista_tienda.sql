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

/*
Configuración de Materialización

{{ config(materialized="view") }}

Esto indica que el modelo se materializará como una vista en la base de datos.

CTE: ventas_moneda_local

with
    ventas_moneda_local as (
        select l.l_suppkey, l.l_extendedprice * m.tasa_cambio as venta_moneda_local
        from {{ ref("lineitem") }} l
        join {{ ref("moneda_local") }} m on l.l_orderkey = m.o_orderkey
    ),

Objetivo: Calcular las ventas en moneda local.
Operaciones:
Se selecciona l.l_suppkey (clave del proveedor) y l.l_extendedprice * m.tasa_cambio (precio extendido multiplicado por la tasa de cambio) como venta_moneda_local.
Se realiza un JOIN entre las tablas lineitem y moneda_local utilizando l.l_orderkey y m.o_orderkey.
CTE: eventos_por_tienda

eventos_por_tienda as (
    select l.l_suppkey, count(e.id_evento) as numero_eventos
    from {{ ref("lineitem") }} l
    left join {{ ref("evento") }} e on l.l_orderkey = e.o_orderkey
    group by l.l_suppkey
)

Objetivo: Contar el número de eventos por tienda.
Operaciones:
Se selecciona l.l_suppkey y el conteo de e.id_evento como numero_eventos.
Se realiza un LEFT JOIN entre las tablas lineitem y evento utilizando l.l_orderkey y e.o_orderkey.
Se agrupa por l.l_suppkey.
Selección Final

select
    v.l_suppkey as tienda_id,
    sum(v.venta_moneda_local) as total_ventas_moneda_local,
    e.numero_eventos
from ventas_moneda_local v
join eventos_por_tienda e on v.l_suppkey = e.l_suppkey
group by v.l_suppkey, e.numero_eventos

Objetivo: Combinar los resultados de las CTEs anteriores y calcular el total de ventas en moneda local por tienda.
Operaciones:
Se selecciona v.l_suppkey como tienda_id, la suma de v.venta_moneda_local como total_ventas_moneda_local, y e.numero_eventos.
Se realiza un JOIN entre ventas_moneda_local y eventos_por_tienda utilizando l_suppkey.
Se agrupa por v.l_suppkey y e.numero_eventos.
*/