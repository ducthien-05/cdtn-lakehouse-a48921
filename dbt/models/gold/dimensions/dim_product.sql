select
    p.product_id,
    coalesce(p.product_category_name, '__UNKNOWN__') as category_key,
    coalesce(c.product_category_name_english, 'Unknown') as product_category_name_english,
    p.product_category_name,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p._bronze_snapshot_id as products_bronze_snapshot_id
from {{ source('olist_silver', 'products') }} as p
left join {{ ref('dim_category') }} as c
  on c.category_key = coalesce(p.product_category_name, '__UNKNOWN__')
