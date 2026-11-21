{{ config(tblproperties=olist_silver_properties('customers')) }}

with latest as (
    {{ olist_latest_cdc(source('olist_bronze', 'customers'), olist_keys('customers'), olist_snapshot('customers')) }}
)
select
    get_json_object(after_json, '$.customer_id') as customer_id,
    get_json_object(after_json, '$.customer_unique_id') as customer_unique_id,
    get_json_object(after_json, '$.customer_zip_code_prefix') as customer_zip_code_prefix,
    get_json_object(after_json, '$.customer_city') as customer_city,
    get_json_object(after_json, '$.customer_state') as customer_state,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    source_lsn as _source_lsn,
    cast({{ olist_snapshot('customers') }} as bigint) as _bronze_snapshot_id
from latest
