/*• Un campo en el que se especifique qué tipo de operación se ha realizado (si es una
venta o una devolución, NO nos interesa contemplar los artículos anulados en esta
tabla).*/
{{ config(materialized='table') }}

with tipo_operacion as (
select
    o.o_orderkey,
    l.l_returnflag,
    l.l_quantity,
    l.l_extendedprice,
    case
        when l.l_returnflag = 'A'
        then 'Devolución'
        when l.l_returnflag = 'N'
        then 'Venta'
    end as tipo_operacion
    from {{ ref ("orders") }} o
    join {{ ref("lineitem") }} l on o.o_orderkey = l.l_orderkey
)
select * from tipo_operacion