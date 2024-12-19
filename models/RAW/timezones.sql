{{ config(materialized="table") }}

with timezones as (
    select  
	timezone,pais
    from TPCH_SF1.timezones
)
select *
from timezones