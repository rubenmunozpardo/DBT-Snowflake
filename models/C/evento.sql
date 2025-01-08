/* • Un
campo llamado ID_EVENTO: Idea las promociones/eventos/temporadas de rebajas que
quieras. Los pedidos que se hagan en ese intervalo temporal se asociarán al evento
correspondiente. Además, puede estar asociado también a países (ejemplo: el “Buen Fin”
en México).*/

{{ config(materialized="table") }}


with evento as (
        select
            o_orderkey,
            case
            when to_varchar(o_orderdate, 'MM-DD') between '11-01' and '11-30' then 'Black Friday'
            when to_varchar(o_orderdate, 'MM-DD') between '12-01' and '12-31' then 'Campaña Navidad'
            else 'Sin Evento'
            end as id_evento
        from {{ ref("orders") }} o
    )
    select * from evento