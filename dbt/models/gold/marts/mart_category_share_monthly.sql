with grouped as (
    select
        cast(date_trunc('month', o.order_purchase_timestamp) as date) as order_purchase_month,
        coalesce(p.category_key, '__UNKNOWN__') as category_key,
        coalesce(p.product_category_name_english, 'Unknown') as product_category_name_english,
        cast(sum(i.item_value + i.freight_value) as decimal(18,2)) as sales_value
    from {{ ref('fct_order_items') }} as i
    join {{ source('olist_silver', 'orders') }} as o on o.order_id = i.order_id
    left join {{ ref('dim_product') }} as p on p.product_id = i.product_id
    group by cast(date_trunc('month', o.order_purchase_timestamp) as date),
             coalesce(p.category_key, '__UNKNOWN__'), coalesce(p.product_category_name_english, 'Unknown')
)
select
    order_purchase_month,
    category_key,
    product_category_name_english,
    sales_value,
    cast(sales_value / sum(sales_value) over (partition by order_purchase_month) as decimal(9,6)) as category_share
from grouped
