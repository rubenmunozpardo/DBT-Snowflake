--informe ventas a nivel de fecha
--Unir ORDERS y LINEITEM .
--xtrae parte numérica O_CLERK.
--Agrupamos los datos
{{ config(materialized='table') }}

with
    ventas_fecha as (
        select
            o.o_orderdate,
            o.o_custkey,
            l.l_partkey,
            sum(l.l_quantity) as total_unidades,
            sum(l.l_extendedprice) as total_importe,
            cast(regexp_substr(o.o_clerk, '[0-9]+') as integer) as clerk_id
        from {{ ref("orders") }} o
        join {{ ref("lineitem") }} l on o.o_orderkey = l.l_orderkey
        where o_orderstatus in ('F', 'O')
        group by o.o_orderdate, o.o_custkey, l.l_partkey, o.o_clerk
        order by o.o_orderdate
    )
select *
from ventas_fecha
