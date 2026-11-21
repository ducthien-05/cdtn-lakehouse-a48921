{{ config(tblproperties=olist_silver_properties('order_payments')) }}

with latest as (
    {{ olist_latest_cdc(source('olist_bronze', 'order_payments'), olist_keys('order_payments'), olist_snapshot('order_payments')) }}
)
select
    get_json_object(after_json, '$.order_id') as order_id,
    try_cast(get_json_object(after_json, '$.payment_sequential') as int) as payment_sequential,
    get_json_object(after_json, '$.payment_type') as payment_type,
    try_cast(get_json_object(after_json, '$.payment_installments') as int) as payment_installments,
    try_cast(get_json_object(after_json, '$.payment_value') as decimal(12,2)) as payment_value,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    source_lsn as _source_lsn,
    cast({{ olist_snapshot('order_payments') }} as bigint) as _bronze_snapshot_id
from latest
