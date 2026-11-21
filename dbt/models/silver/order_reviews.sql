{{ config(tblproperties=olist_silver_properties('order_reviews')) }}

with latest as (
    {{ olist_latest_cdc(source('olist_bronze', 'order_reviews'), olist_keys('order_reviews'), olist_snapshot('order_reviews')) }}
)
select
    get_json_object(after_json, '$.review_id') as review_id,
    get_json_object(after_json, '$.order_id') as order_id,
    try_cast(get_json_object(after_json, '$.review_score') as int) as review_score,
    get_json_object(after_json, '$.review_comment_title') as review_comment_title,
    get_json_object(after_json, '$.review_comment_message') as review_comment_message,
    {{ olist_timestamp("get_json_object(after_json, '$.review_creation_date')") }} as review_creation_date,
    {{ olist_timestamp("get_json_object(after_json, '$.review_answer_timestamp')") }} as review_answer_timestamp,
    kafka_topic as _kafka_topic,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    source_lsn as _source_lsn,
    cast({{ olist_snapshot('order_reviews') }} as bigint) as _bronze_snapshot_id
from latest
