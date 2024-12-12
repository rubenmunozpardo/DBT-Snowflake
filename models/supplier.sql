{{ config(materialized='table') }}

with supplier as (
    select  
	S_SUPPKEY,
	S_NAME,
	S_ADDRESS,
	S_NATIONKEY,
	S_PHONE,
	S_ACCTBAL,
	S_COMMENT
    from TPCH_SF1.SUPPLIER
)
select *
from supplier