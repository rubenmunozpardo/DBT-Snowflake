{{ config(materialized="table") }}

with tienda as (select o_orderkey, tienda, pais_tienda, from tpch_sf1.tienda)
select *
from tienda
