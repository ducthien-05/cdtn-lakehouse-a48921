select
    case when o.is_delivered_on_time then 'on_time' else 'late_or_not_delivered' end as delivery_bucket,
    count(distinct r.order_id) as reviewed_order_count,
    count(*) as review_count,
    cast(avg(cast(r.review_score as decimal(4,2))) as decimal(4,2)) as average_review_score,
    cast(avg(r.answer_days) as decimal(12,2)) as average_review_answer_days
from {{ ref('fct_order_reviews') }} as r
join {{ ref('fct_orders') }} as o on o.order_id = r.order_id
group by case when o.is_delivered_on_time then 'on_time' else 'late_or_not_delivered' end
