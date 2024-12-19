{{ config(materialized="table") }}

with
    UTC as (
        select
            o.o_orderkey,
            o.o_orderdate,
            o.o_totalprice,
            l.l_extendedprice,
            n.n_name as pais_cliente,
            t.pais_tienda
        from {{ ref("orders") }} as o
        join {{ ref("lineitem") }} as l on l.l_orderkey = o.o_orderkey
        join {{ ref("nation") }} as n on n.n_nationkey = o.o_custkey
        join {{ ref("pais_tienda") }} as t on t.c_custkey = o.o_custkey
    )
select
    UTC.*,
    CONVERT_TIMEZONE(utc.pais_tienda, 'UTC', UTC.o_orderdate) as order_date_tienda,
    CONVERT_TIMEZONE(utc.nation, 'UTC', UTC.o_orderdate) as order_date_cliente
from UTC;