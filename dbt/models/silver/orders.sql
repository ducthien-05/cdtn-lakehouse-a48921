{{ config(tblproperties=olist_silver_properties('orders')) }}

with latest as (
    {{ olist_latest_cdc(source('olist_bronze', 'orders'), olist_keys('orders'), olist_snapshot('orders')) }}
)
select
    get_json_object(after_json, '$.order_id') as order_id,
    get_json_object(after_json, '$.customer_id') as customer_id,
    get_json_object(after_json, '$.order_status') as order_status,
    {{ olist_timestamp("get_json_object(after_json, '$.order_purchase_timestamp')") }} as order_purchase_timestamp,
    {{ olist_timestamp("get_json_object(after_json, '$.order_approved_at')") }} as order_approved_at,
    {{ olist_timestamp("get_json_object(after_json, '$.order_delivered_carrier_date')") }} as order_delivered_carrier_date,
    {{ olist_timestamp("get_json_object(after_json, '$.order_delivered_customer_date')") }} as order_delivered_customer_date,
    {{ olist_timestamp("get_json_object(after_json, '$.order_estimated_delivery_date')") }} as order_estimated_delivery_date,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    source_lsn as _source_lsn,
    cast({{ olist_snapshot('orders') }} as bigint) as _bronze_snapshot_id
from latest
