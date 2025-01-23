{{ config(materialized='table') }}

select
    o.o_orderkey,
    o.o_orderdate,
    case
        when uniform(0, 1, random()) < 0.33
        then 'Tienda A'
        when uniform(0, 1, random()) < 0.66
        then 'Tienda B'
        else 'Tienda C'
    end as tienda,
    case
        when uniform(0, 1, random()) < 0.33
        then 'America/New_York'
        when uniform(0, 1, random()) < 0.66
        then 'Asia/Tehran'
        else 'America/Toronto'
    end as pais
from {{ ref('orders') }} o