{{ config(materialized='table') }}

with nation as (
    select  
	N_NATIONKEY,
	N_NAME,
	N_REGIONKEY,
	N_COMMENT
    from TPCH_SF1.nation
)
select *
from nation