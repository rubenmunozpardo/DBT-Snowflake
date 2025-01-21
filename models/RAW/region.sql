{{ config(materialized='table') }}

with region as (
    select  
	R_REGIONKEY,
	R_NAME,
	R_COMMENT
    from TPCH_SF1.REGION
)
select *
from region