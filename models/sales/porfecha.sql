with
    limpiar as (
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
    ),

    tienda as (
        select
            o_orderkey,
            case
                when uniform(0, 1, random()) < 0.33
                then 'Tienda A'
                when uniform(0, 1, random()) < 0.66
                then 'Tienda B'
                else 'Tienda C'
            end as tienda,
            case
                when uniform(0, 1, random()) < 0.33
                then 'USA'
                when uniform(0, 1, random()) < 0.66
                then 'Mexico'
                else 'Canada'
            end as pais_tienda
        from {{ ref("orders") }}
    ),

    evento as (
        select
            o_orderkey,
            case
                when o_orderdate between '2023-11-01' and '2023-11-30'
                then 'Black Friday'
                when o_orderdate between '2023-12-01' and '2023-12-31'
                then 'Navidad'
                else 'Sin Evento'
            end as id_evento
        from {{ ref("orders") }}
    ),

    cambio_moneda as (
        select 'USA' as pais, 1.0 as tasa_cambio
        union all
        select 'Mexico' as pais, 20.0 as tasa_cambio
        union all
        select 'Canada' as pais, 1.3 as tasa_cambio
    ),

    plazo_entrega as (
        select
            l.l_orderkey,
            case
                when datediff(day, l.l_commitdate, l.l_receiptdate) > 10
                then 0
                when datediff(day, l.l_commitdate, l.l_receiptdate) <= 0
                then 1
                else 2
            end as id_plazo_entrega
        from {{ ref("lineitem") }} l
    ),

    nation as (select n_nationkey, n_name as nation_name from {{ ref("nation") }})

select
    c.c_custkey,
    c.c_name,
    p.p_partkey,
    p.p_name,
    l.l_quantity,
    l.l_extendedprice,
    limpiar.o_orderdate,
    limpiar.clerk_id,
    case
        when l.l_returnflag = 'A'
        then 'Devolución'
        when l.l_returnflag = 'N'
        then 'Venta'
    end as tipo_operacion,
    t.tienda,
    t.pais_tienda,
    e.id_evento,
    l.l_extendedprice * cm_tienda.tasa_cambio as total_amount_tienda,
    l.l_extendedprice * cm_cliente.tasa_cambio as total_amount_cliente,
    convert_timezone(t.pais_tienda, 'UTC', limpiar.o_orderdate) as order_date_tienda,
    convert_timezone(n.nation_name, 'UTC', limpiar.o_orderdate) as order_date_cliente,
    pz.id_plazo_entrega
from limpiar
join {{ ref("lineitem") }} l on limpiar.o_orderkey = l.l_orderkey
join {{ ref("customer") }} c on limpiar.o_custkey = c.c_custkey
join {{ ref("part") }} p on l.l_partkey = p.p_partkey
join tienda t on limpiar.o_orderkey = t.o_orderkey
join evento e on limpiar.o_orderkey = e.o_orderkey
join cambio_moneda cm_tienda on t.pais_tienda = cm_tienda.pais
join nation n on c.c_nationkey = n.n_nationkey
join cambio_moneda cm_cliente on n.nation_name = cm_cliente.pais
join plazo_entrega pz on l.l_orderkey = pz.l_orderkey
