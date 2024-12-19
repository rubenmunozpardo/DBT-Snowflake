-- Crea una dimensión de tienda con su país relacionado y añade el campo TIENDA a la
-- tabla. Estos valores puedes generarlos y asignarlos a cada pedido de forma aleatoria

{{ config(materialized="table") }}

with
    pais_tienda as (
        select
            o.o_orderkey,
            --tienda,
            --pais,
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
        from {{ ref("orders") }} o
    )
select *
from pais_tienda
