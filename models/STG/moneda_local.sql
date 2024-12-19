/*• El importe que aparece en las tablas orders y lineitems viene en dólares. Saca un
campo con este importe en moneda local en función del país de la tienda y otro en
función del país del cliente. Para ello busca datos sobre el cambio diario de alguna
fuente externa.*/

-- nota, no aparece ninguno de UNITED STATES

{{ config(materialized="table") }}

with
    cambio_moneda as (
        select 'UNITED STATES' as pais, 1.09 as tasa_cambio
        union all
        select 'IRAN' as pais, 20.0 as tasa_cambio
        union all
        select 'CANADA' as pais, 1.3 as tasa_cambio
    )
select distinct
    o.o_orderkey,
    cm.*,
    o.o_totalprice,
    o.o_totalprice * cm.tasa_cambio as total,
    n.n_name as pais_cliente
from cambio_moneda as cm
join {{ ref("orders") }} as o on o.o_orderkey = o.o_orderkey
join {{ ref("lineitem") }} as l on l.l_orderkey = o.o_orderkey
join {{ ref("nation") }} as n on n.n_nationkey = o.o_custkey
where n.n_name = cm.pais
