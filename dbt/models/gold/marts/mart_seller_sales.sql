select
    i.seller_id,
    s.seller_city,
    s.seller_state,
    count(*) as item_count,
    count(distinct i.order_id) as order_count,
    cast(sum(i.item_value) as decimal(18,2)) as item_value,
    cast(sum(i.freight_value) as decimal(18,2)) as freight_value,
    cast(sum(i.item_value + i.freight_value) as decimal(18,2)) as sales_value
from {{ ref('fct_order_items') }} as i
left join {{ ref('dim_seller') }} as s on s.seller_id = i.seller_id
group by i.seller_id, s.seller_city, s.seller_state
