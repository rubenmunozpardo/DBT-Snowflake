{{ config(materialized='table') }}

select
    o.o_orderkey,
    o.o_orderdate,
    o.o_custkey,
    l.l_partkey,
    sum(l.l_quantity) as total_unidades,
    sum(l.l_extendedprice) as total_importe,
    cast(regexp_substr(o.o_clerk, '[0-9]+') as integer) as clerk_id,
    l.l_receiptdate as fecha_recibido
from {{ ref('orders') }} o
join {{ ref('lineitem') }} l on o.o_orderkey = l.l_orderkey
where o_orderstatus in ('F', 'O')
group by
    o.o_orderkey,
    o.o_orderdate,
    o.o_custkey,
    l.l_partkey,
    o.o_clerk,
    l.l_receiptdate
order by o.o_orderdate