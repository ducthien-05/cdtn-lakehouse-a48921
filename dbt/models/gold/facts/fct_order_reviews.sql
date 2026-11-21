select
    concat(r.review_id, ':', r.order_id) as review_key,
    r.review_id,
    r.order_id,
    r.review_score,
    cast(date_format(cast(r.review_creation_date as date), 'yyyyMMdd') as int) as review_creation_date_key,
    coalesce(cast(date_format(cast(r.review_answer_timestamp as date), 'yyyyMMdd') as int), 0) as review_answer_date_key,
    case when r.review_comment_title is not null or r.review_comment_message is not null then true else false end as has_comment,
    case when r.review_answer_timestamp is not null
         then datediff(cast(r.review_answer_timestamp as date), cast(r.review_creation_date as date))
         else null end as answer_days,
    cast(r._bronze_snapshot_id as bigint) as order_reviews_bronze_snapshot_id
from {{ source('olist_silver', 'order_reviews') }} as r
