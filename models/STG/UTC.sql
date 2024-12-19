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
    )
select utc.* from utc
