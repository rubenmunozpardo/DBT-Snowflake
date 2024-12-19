{{ config(materialized="table") }}

with timezones as (
    select  
	UTC
    from TPCH_SF1.timezones
)
select *
from timezones