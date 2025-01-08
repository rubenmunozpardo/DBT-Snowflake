-- models/vista_cliente.sql

WITH customer_data AS (
    SELECT
        C_CUSTKEY,
        C_NAME,
        C_ADDRESS,
        C_PHONE,
        C_ACCTBAL
    FROM
        DBT_PROJECT.CUSTOMER
),
lineitem_data AS (
    SELECT
        L_ORDERKEY,
        L_PARTKEY,
        L_SUPPKEY,
        L_QUANTITY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_TAX,
        L_RETURNFLAG,
        L_LINESTATUS,
        L_SHIPDATE,
        L_COMMITDATE,
        L_RECEIPTDATE,
        L_SHIPINSTRUCT,
        L_SHIPMODE,
        L_COMMENT
    FROM
        DBT_PROJECT.LINEITEM
)
SELECT
    l.L_ORDERKEY,
    l.L_PARTKEY,
    l.L_SUPPKEY,
    l.L_QUANTITY,
    l.L_EXTENDEDPRICE,
    l.L_DISCOUNT,
    l.L_TAX,
    l.L_RETURNFLAG,
    l.L_LINESTATUS,
    l.L_SHIPDATE,
    l.L_COMMITDATE,
    l.L_RECEIPTDATE,
    l.L_SHIPINSTRUCT,
    l.L_SHIPMODE,
    l.L_COMMENT,
    c.C_CUSTKEY,
    c.C_NAME,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_ACCTBAL
FROM
    lineitem_data l
JOIN
    customer_data c
ON
    l.L_ORDERKEY = c.C_CUSTKEY