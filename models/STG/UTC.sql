with
    utc as (
        select
            o.o_orderkey,
            o.o_orderdate,
            o.o_totalprice,
            l.l_extendedprice,
            n.n_name as nation,
            ml.pais as store_pais,
            ml.timezone as store_timezone,
            c.pais as customer_pais,
            c.timezone as customer_timezone
        from {{ ref("ORDERS") }} as o
        join {{ ref("LINEITEM") }} as l on l.l_orderkey = o.o_orderkey
        join {{ ref("NATION") }} as n on n.n_nationkey = o.o_custkey
        join {{ ref("MONEDA_LOCAL") }} as ml on ml.o_orderkey = o.o_orderkey
        join {{ ref("CUSTOMER") }} as c on o.customer_id = c.customer_id
    )
select
    utc.*,
    convert_timezone(utc.store_timezone, 'UTC', utc.o_orderdate) as store_local_time,
    convert_timezone(
        utc.customer_timezone, 'UTC', utc.o_orderdate
    ) as customer_local_time
from utc

