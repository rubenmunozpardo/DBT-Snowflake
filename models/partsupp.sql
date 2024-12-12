{{ config(materialized='table') }}

with partsupp as (
    select  
	PS_PARTKEY,
	PS_SUPPKEY,
	PS_AVAILQTY,
	PS_SUPPLYCOST,
	PS_COMMENT
    from TPCH_SF1.PARTSUPP
)
select *
from partsupp