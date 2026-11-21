select
    order_purchase_month,
    count(*) as order_count,
    cast(sum(order_item_count) as bigint) as order_item_count,
    cast(sum(item_value) as decimal(18,2)) as item_value,
    cast(sum(freight_value) as decimal(18,2)) as freight_value,
    cast(sum(order_value) as decimal(18,2)) as order_value,
    cast(avg(order_value) as decimal(18,2)) as average_order_value,
    sum(case when is_delivered then 1 else 0 end) as delivered_order_count,
    sum(case when is_cancelled then 1 else 0 end) as cancelled_order_count,
    sum(case when is_unavailable then 1 else 0 end) as unavailable_order_count
from {{ ref('fct_orders') }}
group by order_purchase_month
