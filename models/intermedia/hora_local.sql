{{ config(materialized='table') }}

select
    o.o_orderkey,
    n.n_name as pais,
    t.timezone as order_timezone,
    dateadd(
        'second',
        (cast(substr(t.utc_offset, 2, 2) as int) * 3600)
        + (cast(substr(t.utc_offset, 5, 2) as int) * 60)
        * case when substr(t.utc_offset, 1, 1) = '+' then 1 else -1 end,
        to_timestamp(o.o_orderdate)
    ) as hora_local
from {{ ref('orders') }} o
join {{ ref('customer') }} c on o.o_custkey = c.c_custkey
join {{ ref('nation') }} n on c.c_nationkey = n.n_nationkey
join {{ ref('timezones') }} t on n.n_name = t.n_name