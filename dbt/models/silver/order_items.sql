{{ config(tblproperties=olist_silver_properties('order_items')) }}

with latest as (
    {{ olist_latest_cdc(source('olist_bronze', 'order_items'), olist_keys('order_items'), olist_snapshot('order_items')) }}
)
select
    get_json_object(after_json, '$.order_id') as order_id,
    try_cast(get_json_object(after_json, '$.order_item_id') as int) as order_item_id,
    get_json_object(after_json, '$.product_id') as product_id,
    get_json_object(after_json, '$.seller_id') as seller_id,
    {{ olist_timestamp("get_json_object(after_json, '$.shipping_limit_date')") }} as shipping_limit_date,
    try_cast(get_json_object(after_json, '$.price') as decimal(12,2)) as price,
    try_cast(get_json_object(after_json, '$.freight_value') as decimal(12,2)) as freight_value,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    source_lsn as _source_lsn,
    cast({{ olist_snapshot('order_items') }} as bigint) as _bronze_snapshot_id
from latest
