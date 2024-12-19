{{ config(materialized="table") }}

with
    utc as (
        select
            o.o_orderkey,
            o.o_orderdate,
            o.o_totalprice,
            l.l_extendedprice,
            n.n_name as nation,
            pt.pais
        from {{ ref("orders") }} as o
        join {{ ref("lineitem") }} as l on l.l_orderkey = o.o_orderkey
        join {{ ref("nation") }} as n on n.n_nationkey = o.o_custkey
        join {{ ref("pais_tienda") }} as pt on pt.o_orderkey = o.o_orderkey
        join {{ ref("customer") }} as c on c.c_nation = n.n_nationkey
    )
select
    utc.*,
    convert_timezone(
        coalesce(tm_tienda.timezone, 'UTC'), 'UTC', utc.o_orderdate
    ) as order_date_tienda,
    convert_timezone(
        coalesce(tm_cliente.timezone, 'UTC'), 'UTC', utc.o_orderdate
    ) as order_date_cliente
from utc
left join {{ ref("timezones") }} as tm_tienda on tm_tienda.pais = utc.pais
left join {{ ref("timezones") }} as tm_cliente on tm_cliente.pais = utc.nation
;
