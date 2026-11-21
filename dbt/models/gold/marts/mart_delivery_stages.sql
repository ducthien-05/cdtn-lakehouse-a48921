select
    order_purchase_month,
    cast(avg(purchase_to_carrier_days) as decimal(12,2)) as average_purchase_to_carrier_days,
    cast(avg(carrier_to_customer_days) as decimal(12,2)) as average_carrier_to_customer_days,
    cast(avg(purchase_to_customer_days) as decimal(12,2)) as average_purchase_to_customer_days,
    cast(avg(estimated_delivery_days) as decimal(12,2)) as average_estimated_delivery_days,
    count(*) as order_count
from {{ ref('fct_orders') }}
where is_delivered
  and purchase_to_customer_days is not null
group by order_purchase_month
