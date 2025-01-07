{{ config(materialized="table") }}

with timezone as (
    select  
	n_name, timezone,utc_offset
    from TPCH_SF1.timezone
)
select *
from timezone