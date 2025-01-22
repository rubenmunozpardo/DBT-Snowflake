{{ config(materialized='table') }}

with
    evento as (
        select
            o_orderkey,
            o_orderdate,
            case
                when to_varchar(o_orderdate, 'MM-DD') between '11-01' and '11-30'
                then 'Black Friday'
                when to_varchar(o_orderdate, 'MM-DD') between '12-01' and '12-31'
                then 'Campaña Navidad'
                else 'Sin Evento'
            end as id_evento
        from dbt_project.orders
    ),
    pais_tienda as (
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
        from dbt_project.orders o
    ),
    cambio_moneda as (
        select 'America/New_York' as pais, 1.09 as tasa_cambio
        union all
        select 'Asia/Tehran' as pais, 20.0 as tasa_cambio
        union all
        select 'America/Toronto' as pais, 1.3 as tasa_cambio
    ),
    moneda_local as (
        select distinct
            o.o_orderkey,
            cm.pais,
            cm.tasa_cambio,
            o.o_totalprice,
            o.o_totalprice * cm.tasa_cambio as total,
            n.n_name as pais_cliente
        from cambio_moneda as cm
        join dbt_project.orders as o on o.o_orderkey = o.o_orderkey
        join dbt_project.lineitem as l on l.l_orderkey = o.o_orderkey
        join dbt_project.nation as n on n.n_nationkey = o.o_custkey
    ),
    hora_local as (
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
        from dbt_project.orders o
        join dbt_project.customer c on o.o_custkey = c.c_custkey
        join dbt_project.nation n on c.c_nationkey = n.n_nationkey
        join dbt_project.timezones t on n.n_name = t.n_name
    ),
    plazo_entrega as (
        select
            o.o_orderkey,
            o.o_orderdate,
            o.o_custkey,
            l.l_partkey,
            sum(l.l_quantity) as total_unidades,
            sum(l.l_extendedprice) as total_importe,
            cast(regexp_substr(o.o_clerk, '[0-9]+') as integer) as clerk_id,
            l.l_receiptdate as fecha_recibido
        from dbt_project.orders o
        join dbt_project.lineitem l on o.o_orderkey = l.l_orderkey
        where o_orderstatus in ('F', 'O')
        group by
            o.o_orderkey,
            o.o_orderdate,
            o.o_custkey,
            l.l_partkey,
            o.o_clerk,
            l.l_receiptdate
        order by o.o_orderdate
    ),
    tipo_operacion as (
        select
            o_orderkey,
            case
                when uniform(0, 1, random()) < 0.5 then 'Venta' else 'Devolución'
            end as tipo_operacion
        from dbt_project.orders
    )
select
    t.o_orderkey,
    t.id_evento,
    pt.tienda,
    pt.pais,
    ml.tasa_cambio,
    ml.total as total_moneda_local,
    hl.hora_local,
    pe.total_unidades,
    pe.total_importe,
    pe.clerk_id,
    pe.fecha_recibido,
    tp.tipo_operacion
from evento t
join pais_tienda pt on t.o_orderkey = pt.o_orderkey
join moneda_local ml on t.o_orderkey = ml.o_orderkey
join hora_local hl on t.o_orderkey = hl.o_orderkey
join plazo_entrega pe on t.o_orderkey = pe.o_orderkey
join tipo_operacion tp on t.o_orderkey = tp.o_orderkey
