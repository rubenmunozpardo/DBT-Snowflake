with
    ventas_fecha as (
        select
            o_orderkey,
            o_custkey,
            o_orderstatus,
            o_totalprice,
            o_orderdate,
            o_orderpriority,
            o_clerk,
            o_shippriority,
            o_comment,
            cast(regexp_substr(o_clerk, '[0-9]+') as integer) as clerk_id
        from {{ ref("orders") }}
        where o_orderstatus in ('F', 'O')
    )
select *
from ventas_fecha
