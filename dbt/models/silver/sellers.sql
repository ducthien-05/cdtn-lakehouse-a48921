{{ config(tblproperties=olist_silver_properties('sellers')) }}

with latest as (
    {{ olist_latest_cdc(source('olist_bronze', 'sellers'), olist_keys('sellers'), olist_snapshot('sellers')) }}
)
select
    get_json_object(after_json, '$.seller_id') as seller_id,
    get_json_object(after_json, '$.seller_zip_code_prefix') as seller_zip_code_prefix,
    get_json_object(after_json, '$.seller_city') as seller_city,
    get_json_object(after_json, '$.seller_state') as seller_state,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    source_lsn as _source_lsn,
    cast({{ olist_snapshot('sellers') }} as bigint) as _bronze_snapshot_id
from latest
