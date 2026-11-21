select
    i.product_id,
    p.product_category_name,
    p.product_category_name_english,
    count(*) as item_count,
    count(distinct i.order_id) as order_count,
    cast(sum(i.item_value) as decimal(18,2)) as item_value,
    cast(sum(i.freight_value) as decimal(18,2)) as freight_value,
    cast(sum(i.item_value + i.freight_value) as decimal(18,2)) as sales_value
from {{ ref('fct_order_items') }} as i
left join {{ ref('dim_product') }} as p on p.product_id = i.product_id
group by i.product_id, p.product_category_name, p.product_category_name_english
