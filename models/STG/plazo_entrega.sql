/*Un campo llamado PLAZO_ENTREGA. Ese campo debe indicar si el pedido se ha
entregado en plazo, si la entrega se ha retrasado como mucho 10 días o si lo ha hecho
más:
ID_PLAZO_ENTREGA DESCRIPCION
0 Fuera de plazo, el pedido se ha entregado con un retraso mayor a 10 días
1 En plazo, el pedido se ha entregado como muy tarde en la fecha
estimada(COMMITDATE)
2 Entrega tardía, el pedido se ha entregado como mucho 10 días después de
la fecha estimada*/
{{ config(materialized="table") }}

with plazo_entrega as (
        select
            l_orderkey,
            case
                when datediff(day, l_commitdate, l_receiptdate) > 10
                then 0 
                when datediff(day, l_commitdate, l_receiptdate) <= 0
                then 1
                else 2
            end as id_plazo_entrega
        from {{ ref("lineitem") }} l
    )
    select * from plazo_entrega