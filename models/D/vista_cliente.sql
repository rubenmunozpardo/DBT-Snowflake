with
    customer_data as (
        select 
            c_custkey as clave_cliente, 
            c_name as nombre_cliente, 
            c_address as direccion_cliente, 
            c_phone as telefono_cliente, 
            c_acctbal as saldo_cuenta_cliente
        from dbt_project.customer
    ),
    lineitem_data as (
        select
            l_orderkey as clave_pedido,
            l_partkey as clave_parte,
            l_suppkey as clave_proveedor,
            l_quantity as cantidad,
            l_extendedprice as precio_extendido,
            l_discount as descuento,
            l_tax as impuesto,
            l_returnflag as bandera_devolucion,
            l_linestatus as estado_linea,
            l_shipdate as fecha_envio,
            l_commitdate as fecha_compromiso,
            l_receiptdate as fecha_recepcion,
            l_shipinstruct as instrucciones_envio,
            l_shipmode as modo_envio,
            l_comment as comentario
        from dbt_project.lineitem
    )
select
    l.clave_pedido,
    LISTAGG(l.clave_parte::string, ', ') as claves_partes,
    LISTAGG(l.clave_proveedor::string, ', ') as claves_proveedores,
    LISTAGG(l.cantidad::string, ', ') as cantidades,
    LISTAGG(l.precio_extendido::string, ', ') as precios_extendidos,
    LISTAGG(l.descuento::string, ', ') as descuentos,
    LISTAGG(l.impuesto::string, ', ') as impuestos,
    LISTAGG(l.bandera_devolucion, ', ') as banderas_devolucion,
    LISTAGG(l.estado_linea, ', ') as estados_linea,
    LISTAGG(l.fecha_envio::string, ', ') as fechas_envio,
    LISTAGG(l.fecha_compromiso::string, ', ') as fechas_compromiso,
    LISTAGG(l.fecha_recepcion::string, ', ') as fechas_recepcion,
    LISTAGG(l.instrucciones_envio, ', ') as instrucciones_envio,
    LISTAGG(l.modo_envio, ', ') as modos_envio,
    LISTAGG(l.comentario, ', ') as comentarios,
    c.clave_cliente,
    c.nombre_cliente,
    c.direccion_cliente,
    c.telefono_cliente,
    c.saldo_cuenta_cliente
from lineitem_data l
join customer_data c on l.clave_pedido = c.clave_cliente
group by 
    l.clave_pedido,
    c.clave_cliente,
    c.nombre_cliente,
    c.direccion_cliente,
    c.telefono_cliente,
    c.saldo_cuenta_cliente

    /*
    CTE: customer_data

with
    customer_data as (
        select 
            c_custkey as clave_cliente, 
            c_name as nombre_cliente, 
            c_address as direccion_cliente, 
            c_phone as telefono_cliente, 
            c_acctbal as saldo_cuenta_cliente
        from dbt_project.customer
    ),

Objetivo: Seleccionar y renombrar columnas de la tabla customer.
Operaciones:
Se seleccionan las columnas c_custkey, c_name, c_address, c_phone y c_acctbal.
Se asignan alias en español para mejorar la legibilidad.
CTE: lineitem_data

lineitem_data as (
    select
        l_orderkey as clave_pedido,
        l_partkey as clave_parte,
        l_suppkey as clave_proveedor,
        l_quantity as cantidad,
        l_extendedprice as precio_extendido,
        l_discount as descuento,
        l_tax as impuesto,
        l_returnflag as bandera_devolucion,
        l_linestatus as estado_linea,
        l_shipdate as fecha_envio,
        l_commitdate as fecha_compromiso,
        l_receiptdate as fecha_recepcion,
        l_shipinstruct as instrucciones_envio,
        l_shipmode as modo_envio,
        l_comment as comentario
    from dbt_project.lineitem
)

Objetivo: Seleccionar y renombrar columnas de la tabla lineitem.
Operaciones:
Se seleccionan las columnas l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode y l_comment.
Se asignan alias en español para mejorar la legibilidad.
Selección Final y Concatenación

select
    l.clave_pedido,
    LISTAGG(l.clave_parte::string, ', ') as claves_partes,
    LISTAGG(l.clave_proveedor::string, ', ') as claves_proveedores,
    LISTAGG(l.cantidad::string, ', ') as cantidades,
    LISTAGG(l.precio_extendido::string, ', ') as precios_extendidos,
    LISTAGG(l.descuento::string, ', ') as descuentos,
    LISTAGG(l.impuesto::string, ', ') as impuestos,
    LISTAGG(l.bandera_devolucion, ', ') as banderas_devolucion,
    LISTAGG(l.estado_linea, ', ') as estados_linea,
    LISTAGG(l.fecha_envio::string, ', ') as fechas_envio,
    LISTAGG(l.fecha_compromiso::string, ', ') as fechas_compromiso,
    LISTAGG(l.fecha_recepcion::string, ', ') as fechas_recepcion,
    LISTAGG(l.instrucciones_envio, ', ') as instrucciones_envio,
    LISTAGG(l.modo_envio, ', ') as modos_envio,
    LISTAGG(l.comentario, ', ') as comentarios,
    c.clave_cliente,
    c.nombre_cliente,
    c.direccion_cliente,
    c.telefono_cliente,
    c.saldo_cuenta_cliente
from lineitem_data l
join customer_data c on l.clave_pedido = c.clave_cliente
group by 
    l.clave_pedido,
    c.clave_cliente,
    c.nombre_cliente,
    c.direccion_cliente,
    c.telefono_cliente,
    c.saldo_cuenta_cliente

Objetivo: Combinar datos de las tablas customer y lineitem, y concatenar los resultados del mismo pedido en una sola fila.
Operaciones:
Se seleccionan las columnas de lineitem_data y se concatenan utilizando LISTAGG para cada columna relevante.
Se seleccionan las columnas de customer_data sin concatenar.
Se realiza un JOIN entre lineitem_data y customer_data utilizando clave_pedido y clave_cliente.
Se agrupa por clave_pedido y las columnas de customer_data.
*/