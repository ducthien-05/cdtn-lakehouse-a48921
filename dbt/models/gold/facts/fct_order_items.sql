select
    concat(i.order_id, ':', cast(i.order_item_id as string)) as order_item_key,
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    cast(date_format(cast(o.order_purchase_timestamp as date), 'yyyyMMdd') as int) as order_purchase_date_key,
    cast(o._bronze_snapshot_id as bigint) as orders_bronze_snapshot_id,
    cast(i.price as decimal(18,2)) as item_value,
    cast(i.freight_value as decimal(18,2)) as freight_value,
    cast(i._bronze_snapshot_id as bigint) as order_items_bronze_snapshot_id
from {{ source('olist_silver', 'order_items') }} as i
left join {{ source('olist_silver', 'orders') }} as o
  on o.order_id = i.order_id
