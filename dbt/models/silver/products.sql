{{ config(tblproperties=olist_silver_properties('products')) }}

with latest as (
    {{ olist_latest_cdc(source('olist_bronze', 'products'), olist_keys('products'), olist_snapshot('products')) }}
)
select
    get_json_object(after_json, '$.product_id') as product_id,
    get_json_object(after_json, '$.product_category_name') as product_category_name,
    try_cast(get_json_object(after_json, '$.product_name_lenght') as int) as product_name_lenght,
    try_cast(get_json_object(after_json, '$.product_description_lenght') as int) as product_description_lenght,
    try_cast(get_json_object(after_json, '$.product_photos_qty') as int) as product_photos_qty,
    try_cast(get_json_object(after_json, '$.product_weight_g') as int) as product_weight_g,
    try_cast(get_json_object(after_json, '$.product_length_cm') as int) as product_length_cm,
    try_cast(get_json_object(after_json, '$.product_height_cm') as int) as product_height_cm,
    try_cast(get_json_object(after_json, '$.product_width_cm') as int) as product_width_cm,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    source_lsn as _source_lsn,
    cast({{ olist_snapshot('products') }} as bigint) as _bronze_snapshot_id
from latest
