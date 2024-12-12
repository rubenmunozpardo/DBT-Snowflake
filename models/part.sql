{{ config(materialized='table') }}

with part as (
    select  
	P_PARTKEY,
	P_NAME,
	P_MFGR,
	P_BRAND,
	P_TYPE,
	P_SIZE,
	P_CONTAINER,
	P_RETAILPRICE,
	P_COMMENT
    from TPCH_SF1.PART
)
select *
from part