select
    order_purchase_month,
    count(*) as order_count,
    sum(case when is_delivered then 1 else 0 end) as delivered_order_count,
    sum(case when is_delivered_on_time then 1 else 0 end) as on_time_order_count,
    cast(sum(case when is_delivered_on_time then 1 else 0 end) / cast(nullif(sum(case when is_delivered then 1 else 0 end), 0) as decimal(18,6)) as decimal(9,6)) as on_time_rate,
    cast(avg(purchase_to_carrier_days) as decimal(12,2)) as average_purchase_to_carrier_days,
    cast(avg(purchase_to_customer_days) as decimal(12,2)) as average_purchase_to_customer_days
from {{ ref('fct_orders') }}
group by order_purchase_month
