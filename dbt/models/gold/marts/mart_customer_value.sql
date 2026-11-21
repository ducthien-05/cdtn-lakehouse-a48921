select
    customer_id,
    customer_state,
    count(*) as order_count,
    cast(sum(order_value) as decimal(18,2)) as customer_value,
    cast(avg(order_value) as decimal(18,2)) as average_order_value,
    sum(case when is_delivered then 1 else 0 end) as delivered_order_count,
    min(order_purchase_timestamp) as first_order_timestamp,
    max(order_purchase_timestamp) as last_order_timestamp
from {{ ref('fct_orders') }}
group by customer_id, customer_state
